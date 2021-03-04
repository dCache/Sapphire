package de.desy.sapphire;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.*;

import static com.mongodb.client.model.Filters.*;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.client.*;
import org.bson.Document;

import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SapphireDriver implements NearlineStorage
{
    private static final Logger _log = LoggerFactory.getLogger(SapphireDriver.class);

    protected final String type;
    protected final String name;
    private MongoClient mongoClient;
    MongoCollection<Document> files;

    public SapphireDriver(String type, String name)
    {
        this.type = type;
        this.name = name;
    }

    /**
     * Flush all files in {@code requests} to nearline storage.
     *
     * @param requests
     */
    @Override
    public void flush(Iterable<FlushRequest> requests)
    {
        FindIterable<Document> results;
        String pnfsid;
        _log.debug("Triggered flush()");
        for (FlushRequest flushRequest : requests) {
            flushRequest.activate();
            pnfsid = flushRequest.getFileAttributes().getPnfsId().toString();
            _log.debug("PNFSID: {}", flushRequest.getFileAttributes().getPnfsId());

            if (flushRequest.getFileAttributes().getSize() == 0) {
                _log.debug("Filesize is 0");
                String store = flushRequest.getFileAttributes().getStorageInfo().getKey("store");
                String group = flushRequest.getFileAttributes().getStorageInfo().getKey("group");
                try {
                    flushRequest.completed(Collections.singleton(new URI("dcache://dcache/store=" + store +
                            "&group=" + group + "&bfid=" + pnfsid + ":*")));
                    continue;
                } catch (URISyntaxException e) {
                    throw new RuntimeException("Could not create URI to complete FlushRequest with filesize 0");
                }
            }

            results = files.find(eq("pnfsid", flushRequest.getFileAttributes().getPnfsId().toString()));
            if(results.iterator().hasNext()) {
                Document result = results.iterator().next();
                _log.debug("Result: {}", result.toJson());
                if (result.containsKey("archiveUrl")) {
                    try {
                        String archiveUrl = (String) result.get("archiveUrl");
                        URI fileUri = new URI(archiveUrl.replace("dcache://dcache", type + "://" + name));
                        _log.debug("archiveUrl exists, fileUri: {}", fileUri);
                        files.deleteOne(new Document("pnfsid", pnfsid));
                        flushRequest.completed(Collections.singleton(fileUri));
                    } catch (URISyntaxException e) {
                        _log.error("Error completing flushRequest: " + e);
                        flushRequest.failed(e);
                    }
                } else {
                    flushRequest.failed(72, "Not yet ready");
                }
            } else {
                try{
                    Path path = Path.of(flushRequest.getFileAttributes().getStorageInfo().getKey("path"));
                    Document entry = new Document("pnfsid", pnfsid)
                            .append("store", flushRequest.getFileAttributes().getStorageInfo().getKey("store"))
                            .append("group", flushRequest.getFileAttributes().getStorageInfo().getKey("group"))
                            .append("path", path.toString())
                            .append("parent", path.getParent().toString())
                            .append("size", Integer.parseInt(Long.toString(flushRequest.getFileAttributes().getSize())))
                            .append("ctime", Double.parseDouble(Long.toString(flushRequest.getReplicaCreationTime())) / 1000)
                            .append("state", "new");
                    _log.debug("Inserting to database: {}", entry.toJson());
                    files.insertOne(entry);
                } catch (IllegalStateException e) {
                  _log.error("Some fields could not be retrieved: " + e);
                  flushRequest.failed(e);
                  continue;
                }
                flushRequest.failed(72, "Not yet ready (empty)");
            }
        }
    }

    /**
     * Stage all files in {@code requests} from nearline storage.
     *
     * @param requests
     */
    @Override
    public void stage(Iterable<StageRequest> requests)
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Delete all files in {@code requests} from nearline storage.
     *
     * @param requests
     */
    @Override
    public void remove(Iterable<RemoveRequest> requests)
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Cancel any flush, stage or remove request with the given id.
     * <p>
     * The failed method of any cancelled request should be called with a
     * CancellationException. If the request completes before it can be
     * cancelled, then the cancellation should be ignored and the completed
     * or failed method should be called as appropriate.
     * <p>
     * A call to cancel must be non-blocking.
     *
     * @param uuid id of the request to cancel
     */
    @Override
    public void cancel(UUID uuid)
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Applies a new configuration.
     *
     * @param properties
     * @throws IllegalArgumentException if the configuration is invalid
     */
    @Override
    public void configure(Map<String, String> properties) throws IllegalArgumentException
    {
        _log.debug("Triggered configure()");
        String mongoUri = properties.getOrDefault("mongo_url", "");
        String database = properties.getOrDefault("database", "");

        if (mongoUri.equals("") || database.equals("")) {
            String propertiesPath = properties.getOrDefault("conf_file", "");
            if (propertiesPath.equals("")) {
                throw new IllegalArgumentException("No or not enough details to MongoDB or configuration file given.");
            } else {
                try(InputStream inputStream = new FileInputStream(propertiesPath)){
                    Properties prop = new Properties();
                    try{
                        prop.load(inputStream);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    mongoUri = prop.getProperty("mongo_url");
                    database = prop.getProperty("database");

                } catch (FileNotFoundException e) {
                    throw new RuntimeException("Configuration file not found");
                } catch (IOException e) {
                    throw new RuntimeException("Could not open and read configuration file");
                }
            }
        }
        _log.debug("mongoUri: {}; database: {}", mongoUri, database);

        try{
            mongoClient = MongoClients.create(mongoUri);
        } catch (MongoSocketOpenException e) {
            _log.error("Could not open connection to MongoDB");
            throw e;
        }
        MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
        files = mongoDatabase.getCollection("files");
    }

    /**
     * Cancels all requests and initiates a shutdown of the nearline storage
     * interface.
     * <p>
     * This method does not wait for actively executing requests to
     * terminate.
     */
    @Override
    public void shutdown()
    {
        mongoClient.close();
    }

}
