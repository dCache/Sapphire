import configparser
import logging
import logging.handlers
import os
import re
import signal
import sys
import traceback
import uuid
from datetime import datetime
import time

import shutil as shutils
from zipfile import ZipFile

from pymongo import MongoClient, ASCENDING, errors

running = True
script_id = ""
mongo_url = ""
mongo_db = ""
mongo_client = ""
working_directory = ""
data_root = ""
mount_point = ""


def sigint_handler(signum, frame):
    global running
    logging.info(f"Caught signal {signum}.")
    print(f"Caught signal {signum}.")
    running = False


def uncaught_handler(*exc_info):
    err_text = "".join(traceback.format_exception(*exc_info))
    logging.critical(err_text)
    sys.stderr.write(err_text)


class UserInterruptException(Exception):
    def __init__(self, arcfile):
        self.arcfile = arcfile

    def __str__(self):
        return repr(self.arcfile)


class GroupPackager:
    def __init__(self, path, file_pattern, store_group, store_name, archive_size,
                 min_age, max_age, verify, archive_path):
        self.path = path
        self.path_pattern = re.compile(os.path.join(path, file_pattern))
        self.store_group = re.compile(store_group)
        self.store_name = re.compile(store_name)
        self.archive_size = int(archive_size.replace('G', '000000000').replace('M', '000000').replace('K', '000'))
        self.archive_path = archive_path
        self.min_age = int(min_age)
        self.max_age = int(max_age)
        self.verify = verify
        self.logger = logging.getLogger(name=f"GroupPackager[{self.path_pattern.pattern}]")

    def write_status(self, arcfile, current_size, next_file):
        global script_id
        with open(f"/var/log/dcache/pack-files-{script_id}.status", 'w') as statusFile:
            statusFile.write(f"Container: {arcfile}\n")
            statusFile.write(f"Size: {current_size}/{self.archive_size}\n")
            statusFile.write(f"Next: {next_file.encode('ascii', 'ignore')}\n")

    def run(self):
        global running
        global data_root
        global mount_point

        now = int(datetime.now().strftime("%s"))
        ctime_threshold = now - self.min_age * 60
        self.logger.debug(f"Looking for files matching {{ "
                          f"state: new, "
                          f"path: {self.path_pattern.pattern}, "
                          f"group: {self.store_group.pattern}, "
                          f"store: {self.store_name.pattern}, "
                          f"ctime: {{ $lt: {ctime_threshold} }} }}")
        with mongo_db.files.find({'state': 'new',
                                  'path': self.path_pattern,
                                  'group': self.store_group,
                                  'store': self.store_name,
                                  'ctime': {'$lt': ctime_threshold}},
                                 no_cursor_timeout=False).batch_size(512) as cursor:
            cursor.sort('ctime', ASCENDING)
            sumsize = 0
            old_files_mode = False
            ctime_oldfile_threshold = now - self.max_age * 60
            filecount = mongo_db.files.count_documents({'state': 'new',
                                                        'path': self.path_pattern,
                                                        'group': self.store_group,
                                                        'store': self.store_name,
                                                        'ctime': {'$lt': ctime_threshold}})
            for f in cursor:
                if f['ctime'] < ctime_oldfile_threshold:
                    old_files_mode = True
                sumsize += f['size']

            self.logger.info(f"Found {filecount} files with a combined size of {sumsize} bytes.")
            self.logger.debug(f"Containing {'' if old_files_mode else 'no '}old files: ctime < "
                              f"{ctime_oldfile_threshold}")

            if old_files_mode:
                if sumsize < self.archive_size:
                    self.logger.info("Combined size of old files not big enough for a regular archive, "
                                     "packing in old file mode.")
                else:
                    old_files_mode = False
                    self.logger.info("Combined size of old files big enough for a regular archive, "
                                     "packing in normal mode.")
            elif sumsize < self.archive_size:
                self.logger.info(f"No old files found and {self.archive_size - sumsize} bytes missing to create "
                                 f"regular archive of size {self.archive_size} bytes, leaving packager.")
                return

            cursor.rewind()

            container = None

            try:
                for f in cursor:
                    if filecount <= 0 or sumsize <= 0:
                        self.logger.info("Actual number of files exceeds precalculated number, "
                                         "will collect new files in next run.")
                        break

                    self.logger.debug(f"Next file {f['path']} [{f['pnfsid']}], remaining {filecount} "
                                      f"files [{sumsize} bytes]")
                    if not running:
                        if container:
                            raise UserInterruptException(container.filepath)
                        else:
                            raise UserInterruptException(None)

                    if container is None:
                        if sumsize >= self.archive_size or old_files_mode:
                            container = Container(self.verify, self.archive_path)
                            self.logger.info(f"Creating new Container {container}. {filecount} files "
                                             f"[{sumsize} bytes] remaining.")
                        else:
                            self.logger.info(f"Remaining combined size {sumsize} < {self.archive_size}, "
                                             f"leaving packager")
                            return
                    if old_files_mode:
                        self.logger.debug(f"{sumsize} bytes remaining for this archive")
                        self.write_status(container.filepath, sumsize, f"{f['path']} [{f['pnfsid']}]")
                    else:
                        self.logger.debug(f"{self.archive_size - container.size} bytes remaining for this archive.")
                        self.write_status(container.filepath, self.archive_size - container.size,
                                          f"{f['path']} [{f['pnfsid']}]")
                    try:
                        localfile = f['path'].replace(data_root, mount_point, 1)
                        container.add(f['pnfsid'], f['path'], localfile, f['size'])
                        f['state'] = f"added: {container.filepath}"
                        f['lock'] = script_id
                        cursor.collection.replace_one(
                            {'state': 'new', 'path': self.path_pattern, 'group': self.store_group,
                             'store': self.store_name, 'ctime': {'$lt': ctime_threshold}}, f)
                        self.logger.debug(f"Added file {f['path']} [{f['pnfsid']}]")
                    except(IOError, OSError) as e:
                        self.logger.exception(f"{'IOError' if type(e) == type(IOError) else 'OSError'} "
                                              f"while adding file {f['path']} [{f['pnfsid']}] to "
                                              f"archive {container.filepath}, {e}")
                        self.logger.debug(f"Removing entry for file {f['pnfsid']}")
                        mongo_db.files.delete_one({'pnfsid': f['pnfsid']})
                    except (errors.OperationFailure, errors.ConnectionFailure) as e:
                        self.logger.error(f"Removing container {container.filepath} due to "
                                          f"{'OperationalFailure' if type(e) == type(errors.OperationFailure) else 'ConnectionFailure'}. "
                                          f"See below for details.")
                        container.close()
                        os.remove(container.filepath)
                        raise e
                    sumsize -= f['size']
                    filecount -= 1

                    if container.size >= self.archive_size:
                        self.logger.debug(f"Closing full container {container.filepath}")
                        container.pack()

                        container = None
                if container:
                    if not old_files_mode:
                        self.logger.warning(f"Removing unful container {container.filepath}. Maybe a file was "
                                            f"deleted during packing")
                        container.close()
                        os.remove(container.filepath)
                        return
                    self.logger.debug(f"Closing container {container.filepath} containing remaining old files")
                    container.pack()

                    container = None

            except IOError as e:
                self.logger.error(f"{e.strerror} closing file {container.filepath}. Trying to clean up files in state: "
                                  f"\"added\". This might need additional manual fixing!")
                self.logger.error(traceback.print_exc())
                mongo_db.files.update_many({'state': f"added: {container.filepath}"}, {"$set": {"state": "new"},
                                                                                       "$unset": {"lock": ""}})
            except errors.OperationFailure as e:
                self.logger.error(f"Operation Exception in database communication while creating container "
                                  f"{container.filepath}. Please Check!\n{e}")
                os.remove(container.filepath)
            except errors.ConnectionFailure as e:
                self.logger.error(f"Connection Exception in database communication. Removing incomplete container "
                                  f"{container.filepath}.\n{e}")
                os.remove(container.filepath)


class Container:
    def __init__(self, verify, archive_path):
        self.filename = str(uuid.uuid1())
        self.filepath = os.path.join(working_directory, "container", self.filename)
        self.temp_dir = os.path.join(working_directory, f"tmp-{self.filename}")
        self.content_dict = dict()
        self.size = 0
        self.filecount = 0
        self.verify = verify
        self.zip_file = ZipFile(self.filepath, 'w')
        self.archive_path = archive_path
        self.logger = logging.getLogger(name=f"Container[{self.filename}]")

    def add(self, pnfsid, filepath, localpath, size):
        self.content_dict[pnfsid] = {"filepath": filepath, "localpath": localpath}
        self.filecount += 1
        self.size += size

    def download_files(self):
        os.mkdir(self.temp_dir)
        for pnfsid in self.content_dict.keys():
            filepath = self.content_dict[pnfsid]['filepath']
            localpath = self.content_dict[pnfsid]['localpath']
            self.logger.debug(f"Filepath: {filepath};; localpath: {localpath}")
            try:
                shutils.copy(localpath, os.path.join(self.temp_dir, pnfsid))
            except Exception as e:
                logging.error(f"Exception while copying file: {type(e)}\n\n{traceback.print_exc()}")

    def verify_archive(self):
        if self.verify == 'filelist':
            verified = len(self.zip_file.filelist) == self.filecount
        elif self.verify == 'chksum':
            self.logger.warning("Checksum verification not implemented yet")
            verified = True
        elif self.verify == 'off':
            verified = True
        else:
            self.logger.warning(f"Unknown verification method {self.verify}. Assuming failure!")
            verified = False

        return verified

    def pack(self):
        for pnfsid in self.content_dict:
            self.zip_file.write(self.content_dict[pnfsid]['localpath'], arcname=pnfsid)

        # self.download_files()
        # logging.info("Downloaded files")
        # for file in os.listdir(self.temp_dir):
        #     logging.debug(f"Add file {file} to zipfile")
        #     self.zip_file.write(os.path.join(self.temp_dir, file))
        self.logger.debug("Added all files from temp dir to zip")
        self.zip_file.close()

        if self.verify_archive():
            self.logger.info(f"Container {self.filepath} successfully stored locally")
            mongo_db.files.update_many({'state': f"added: {self.filepath}"},
                                       {"$set": {"state": f"archived: {self.filepath}"}, "$unset": {"lock": ""}})
            mongo_db.archives.insert_one({"path": self.filepath, "dest_path": self.archive_path})
        else:
            self.logger.warning(f"Removing container {self.filepath} due to verification error")
            mongo_db.files.update_many({"state": f"added: {self.filepath}"},
                                       {"$set": {"state": "new"}, "$unset": {"lock": ""}})
            os.remove(self.filepath)
            # for file in os.listdir(self.temp_dir):
            #     os.remove(os.path.join(self.temp_dir, file))
            # os.rmdir(self.temp_dir)
        # for file in os.listdir(self.temp_dir):
        #     os.remove(os.path.join(self.temp_dir, file))
        # os.rmdir(self.temp_dir)

    def close(self):
        if self.zip_file:
            self.zip_file.close()
        if os.path.isdir(self.temp_dir):
            for file in os.listdir(self.temp_dir):
                os.remove(os.path.join(self.temp_dir, file))
            os.rmdir(self.temp_dir)


def main(configfile="/etc/dcache/container.conf"):
    global running
    global mongo_url
    global mongo_db
    global mongo_client
    global working_directory
    global script_id
    global mount_point
    global data_root

    logger = logging.getLogger()
    log_handler = None

    while running:
        try:
            configuration = configparser.RawConfigParser(
                defaults={'script_id': 'pack', 'mongo_url': 'mongodb://localhost:27017/', 'mongo_db': 'smallfiles',
                          'loop_delay': 5, 'log_level': 'ERROR', 'working_dir': '/sapphire'})
            configuration.read(configfile)

            script_id = configuration.get('DEFAULT', 'script_id')

            log_level_str = configuration.get('DEFAULT', 'log_level')
            log_level = getattr(logging, log_level_str.upper(), None)
            logger.setLevel(log_level)

            if log_handler is not None:
                log_handler.close()
                logger.removeHandler(log_handler)

            log_handler = logging.handlers.WatchedFileHandler(f'/var/log/dcache/pack-files-{script_id}.log')
            formatter = logging.Formatter('%(asctime)s %(name)-10s %(levelname)-8s %(message)s')
            log_handler.setFormatter(formatter)
            logger.addHandler(log_handler)

            mongo_url = configuration.get('DEFAULT', 'mongo_url')
            mongo_db_name = configuration.get('DEFAULT', 'mongo_db')
            working_directory = configuration.get('DEFAULT', 'working_dir')
            loop_delay = configuration.get('DEFAULT', 'loop_delay')
            mount_point = configuration.get("DEFAULT", "mount_point")
            data_root = configuration.get("DEFAULT", "data_root")

            logger.info(f"Successfully read configuration from file {configfile}.")

            logger.debug(f"script_id: {script_id}")
            logger.debug(f"mongo_url: {mongo_url}")
            logger.debug(f"mongo_db: {mongo_db_name}")
            logger.debug(f"working_dir: {working_directory}")
            logger.debug(f"log_level: {log_level}")
            logger.debug(f"loop_delay: {loop_delay}")

            try:
                mongo_client = MongoClient(mongo_url)
                mongo_db = mongo_client[mongo_db_name]
                logger.info("Established connection to MongoDB")

                logger.info("Sanitizing database")
                mongo_db.files.update_many({'lock': script_id}, {'$set': {'state': 'new'}, '$unset': {'lock': ""}})

                logger.info("Creating GroupPackager")

                groups = configuration.sections()
                group_packager = []

                for group in groups:
                    logger.debug(f"Group: {group}")
                    file_pattern = configuration.get(group, "file_expression")
                    store_group = configuration.get(group, "s_group")
                    store_name = configuration.get(group, "store_name")
                    archive_size = configuration.get(group, "archive_size")
                    min_age = configuration.get(group, "min_age")
                    max_age = configuration.get(group, "max_age")
                    verify = configuration.get(group, "verify")
                    path_regex = re.compile(configuration.get(group, "path_expression"))
                    archive_path = configuration.get(group, "archive_path")

                    logger.debug(f"file_pattern: {file_pattern}")
                    logger.debug(f"store_group: {store_group}")
                    logger.debug(f"store_name: {store_name}")
                    logger.debug(f"archive_size: {archive_size}")
                    logger.debug(f"min_age: {min_age}")
                    logger.debug(f"max_age: {max_age}")
                    logger.debug(f"verify: {verify}")
                    logger.debug(f"path_expression: {path_regex}")
                    logger.debug(f"archive_path: {archive_path}")

                    paths = mongo_db.files.find({"parent": path_regex}).distinct("parent")
                    pathset = set()
                    for path in paths:
                        pathmatch = re.match(f"(?P<sfpath>{path_regex.pattern})", path).group("sfpath")
                        pathset.add(pathmatch)

                    for path in pathset:
                        packager = GroupPackager(path, file_pattern, store_group, store_name, archive_size,
                                                 min_age, max_age, verify, archive_path)
                        group_packager.append(packager)
                        logger.info(f"Added packager {group} for paths matching {packager.path}")

                    for packager in group_packager:
                        packager.run()

            except Exception as e:
                print(f"Exception occured: {type(e)} -- {e}\n\n{traceback.print_exc()}")

        except Exception as e:
            print(f"Exception occured: {type(e)} -- {e}\n\n{traceback.print_exc()}")
        time.sleep(int(loop_delay))


if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint_handler)
    sys.excepthook = uncaught_handler
    if not os.getuid() == 0:
        print("pack-files must run as root!")
        sys.exit(2)

    if len(sys.argv) == 1:
        main()
    elif len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print("Usage: pack-files.py <configfile>")
        sys.exit(1)
