#!/usr/bin/env python3
# coding=utf-8
import os
import sys
import time
import signal
from zlib import adler32
from hashlib import md5, sha1
from zipfile import ZipFile, BadZipfile
from pymongo import MongoClient, errors
import configparser as parser
import logging
import logging.handlers
import traceback
import requests
from requests.auth import HTTPBasicAuth
import base64

running = True

archiveUser = ""
archiveMode = ""
mountPoint = ""
dataRoot = ""
mongoUri = ""
mongoDb = ""


def sigint_handler(signum, frame):
    global running
    logging.info(f"Caught signal {signum}.")
    print(f"Caught signal {signum}.")
    running = False


def uncaught_handler(*exc_info):
    err_text = "".join(traceback.format_exception(*exc_info))
    logging.critical(err_text)
    sys.stderr.write(err_text)


def _md5(filepath):
    md5_value = md5()
    with open(filepath, "rb") as file:
        for chunk in iter(lambda: file.read(4096), b""):
            md5_value.update(chunk)
    return base64.b64encode(md5_value.digest()).decode()


def _adler32(filepath):  # Does this work for large files?
    with open(filepath, "rb") as file:
        adler32_value = adler32(file.read())
    return hex(adler32_value)[2:]


def _sha1(filepath):
    sha1_value = sha1()
    with open(filepath, "rb") as file:
        for chunk in iter(lambda: file.read(4096), b""):
            sha1_value.update(chunk)
    return sha1_value.hexdigest()


def main(configfile='/etc/dcache/container.conf'):
    global running

    logger = logging.getLogger()
    log_handler = None
    checksum_calculation = {"md5": _md5,
                            "adler32": _adler32,
                            "sha1": _sha1
                            }

    try:
        while running:
            configuration = parser.RawConfigParser(defaults={'scriptId': 'pack', 'mongoUri': 'mongodb://localhost/',
                                                             'mongoDb': 'smallfiles', 'logLevel': 'ERROR'})
            configuration.read(configfile)

            global archiveUser
            global archiveMode
            global mountPoint
            global dataRoot
            global mongoUri
            global mongoDb
            type = "osm"  # Make it configurable
            name = "osm"

            script_id = configuration.get('DEFAULT', 'script_id')

            log_level_str = configuration.get('DEFAULT', 'log_level')
            log_level = getattr(logging, log_level_str.upper(), None)
            logger.setLevel(log_level)

            if log_handler is not None:
                log_handler.close()
                logger.removeHandler(log_handler)

            log_handler = logging.handlers.WatchedFileHandler(f'/var/log/dcache/writebfids-{script_id}.log')
            formatter = logging.Formatter('%(asctime)s %(name)-10s %(levelname)-8s %(message)s')
            log_handler.setFormatter(formatter)
            logger.addHandler(log_handler)

            archiveUser = configuration.get('DEFAULT', 'archiveUser')
            archiveMode = configuration.get('DEFAULT', 'archiveMode')
            mountPoint = configuration.get('DEFAULT', 'mount_point')
            dataRoot = configuration.get('DEFAULT', 'data_root')
            mongoUri = configuration.get('DEFAULT', 'mongo_url')
            mongoDb = configuration.get('DEFAULT', 'mongo_db')

            logging.info(f'Successfully read configuration from file {configfile}.')

            try:
                client = MongoClient(mongoUri)
                db = client[mongoDb]
                logging.info("Established db connection")

                with db.archives.find() as archives:
                    for archive in archives:
                        if not running:
                            logger.info("Exiting")
                            sys.exit(1)
                        try:
                            zip_file = ZipFile(archive['path'], mode="r", allowZip64=True)
                            for f in zip_file.filelist:
                                filerecord = db.files.find_one({'pnfsid': f.filename,
                                                                'state': f"archived: {archive['path']}"})
                                if not filerecord:
                                    logger.error(
                                        f"File {f.filename} in ZipFile is not in MongoDB! Creating failure entry")
                                    db.failures.insert_one({'archivePath': archive['path'], 'pnfsid': f.filename})
                            print(f"Listed all entries from {archive}")
                            filelist = zip_file.filelist
                            zip_file.close()

                            auth = HTTPBasicAuth('admin', 'dickerelch')
                            logger.debug("Filepath: " + os.path.basename(archive['path']))
                            url = f"https://localhost:2881/archives/{os.path.basename(archive['path'])}"
                            logger.debug(f"URL: {url}")
                            headers = {"Content-type": "application/octet-stream"}
                            response = requests.put(url, data=open(archive['path'], 'rb'),
                                                    auth=auth, verify=False, headers=headers)
                            logger.debug(f"Response: {response}")
                            if response.status_code == 201 or response.status_code == 200:
                                logger.info(f"Archive uploaded successfully")
                            else:
                                logger.error(f"Uploading file failed: {response.status_code} -- {response}")
                                continue

                            headers = {"Want-Digest": "MD5,ADLER32,SHA1"}
                            response = requests.head(f"https://localhost:2881/archives/{os.path.basename(archive['path'])}",
                                                     verify=False, auth=auth, headers=headers)
                            pnfsid = response.headers.get("ETag").split('_')[0].replace('"', '')
                            logger.info(f"PNFSID of Container {archive['path']} is {pnfsid}")

                            checksum_mech, remote_checksum = response.headers.get("Digest").split('=', 1)
                            if checksum_mech not in checksum_calculation.keys():
                                logger.error(f"Checksum mechanism {checksum_mech} for file {archive['path']} is not supported")
                                raise NotImplementedError()
                            local_checksum = checksum_calculation[checksum_mech](archive['path'])

                            if remote_checksum == local_checksum:
                                for file in filelist:
                                    file_entry = db.files.find_one({'pnfsid': file.filename})
                                    logger.debug(f"Update file {file.filename} now.")
                                    archive_url = f"{type}://{name}/?store={file_entry['store']}&group={file_entry['group']}&bfid={file.filename}:{pnfsid}"
                                    logger.debug(f"ArchiveUrl: {archive_url}")
                                    file_entry['archiveUrl'] = archive_url
                                    file_entry['state'] = f"verified: {archive['path']}"
                                    db.files.replace_one({"pnfsid": file.filename}, file_entry)
                            else:
                                logger.error("Checksums of local file and uploaded file doesn't match!")
                        except BadZipfile as e:
                            logger.warning(f"Archive {archive['path']} is not ready yet. Will try again later.")

            except errors.ConnectionFailure as e:
                logger.warning(f"Connection to DB failed: {e}")
            except errors.OperationFailure as e:
                logger.warning(f"Could not create cursor: {e}")
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
            finally:
                if client is not None:
                    client.close()

            logging.info("Processed all archive entries. Sleeping 60 seconds.")
            time.sleep(60)

    except parser.NoOptionError as e:
        print(f"Missing option: {e}")
        logger.error(f"Missing option: {e}")
    except parser.Error as e:
        print(f"Error reading configfile {configfile}: {e}")
        logger.error(f"Error reading configfile {configfile}: {e}")
        sys.exit(2)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint_handler)
    sys.excepthook = uncaught_handler
    if not os.getuid() == 0:
        print("writebfsids.py must run as root!")
        sys.exit(2)

    if len(sys.argv) == 1:
        main()
    elif len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print("Usage: writebfids.py <configfile>")
        sys.exit(2)
