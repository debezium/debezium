/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.MongoException;

import io.debezium.function.BlockingConsumer;

/**
 * Utilities for working with MongoDB.
 *
 * @author Randall Hauch
 */
public class MongoUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoUtils.class);

    /**
     * Perform the given operation on each of the database names.
     *
     * @param client the MongoDB client; may not be null
     * @param operation the operation to perform; may not be null
     */
    public static void forEachDatabaseName(MongoClient client, Consumer<String> operation) {
        forEach(client.listDatabaseNames(), operation);
    }

    /**
     * Perform the given operation on each of the collection names in the named database.
     *
     * @param client the MongoDB client; may not be null
     * @param databaseName the name of the database; may not be null
     * @param operation the operation to perform; may not be null
     */
    public static void forEachCollectionNameInDatabase(MongoClient client, String databaseName, Consumer<String> operation) {
        MongoDatabase db = client.getDatabase(databaseName);
        forEach(db.listCollectionNames(), operation);
    }

    /**
     * Perform the given operation on each of the values in the iterable container.
     *
     * @param iterable  the iterable collection obtained from a MongoDB client; may not be null
     * @param operation the operation to perform; may not be null
     */
    public static <T> void forEach(MongoIterable<T> iterable, Consumer<T> operation) {
        try (MongoCursor<T> cursor = iterable.iterator()) {
            while (cursor.hasNext()) {
                operation.accept(cursor.next());
            }
        }
    }

    /**
     * Perform the given operation on the database with the given name, only if that database exists.
     *
     * @param client the MongoDB client; may not be null
     * @param dbName the name of the database; may not be null
     * @param dbOperation the operation to perform; may not be null
     */
    public static void onDatabase(MongoClient client, String dbName, Consumer<MongoDatabase> dbOperation) {
        if (contains(client.listDatabaseNames(), dbName)) {
            dbOperation.accept(client.getDatabase(dbName));
        }
    }

    /**
     * Perform the given operation on the named collection in the named database, if the database and collection both exist.
     *
     * @param client the MongoDB client; may not be null
     * @param dbName the name of the database; may not be null
     * @param collectionName the name of the collection; may not be null
     * @param collectionOperation the operation to perform; may not be null
     */
    public static void onCollection(MongoClient client, String dbName, String collectionName,
                                    Consumer<MongoCollection<Document>> collectionOperation) {
        onDatabase(client, dbName, db -> {
            if (contains(db.listCollectionNames(), collectionName)) {
                collectionOperation.accept(db.getCollection(collectionName));
            }
        });
    }

    /**
     * Perform the given operation on all of the documents inside the named collection in the named database, if the database and
     * collection both exist. The operation is called once for each document, so if the collection exists but is empty then the
     * function will not be called.
     *
     * @param client the MongoDB client; may not be null
     * @param dbName the name of the database; may not be null
     * @param collectionName the name of the collection; may not be null
     * @param documentOperation the operation to perform; may not be null
     */
    public static void onCollectionDocuments(MongoClient client, String dbName, String collectionName,
                                             BlockingConsumer<Document> documentOperation) {
        onCollection(client, dbName, collectionName, collection -> {
            try (MongoCursor<Document> cursor = collection.find().iterator()) {
                while (cursor.hasNext()) {
                    try {
                        documentOperation.accept(cursor.next());
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        });
    }

    /**
     * Determine if the supplied {@link MongoIterable} contains an element that is equal to the supplied value.
     *
     * @param iterable the iterable; may not be null
     * @param match the value to find in the iterable; may be null
     * @return {@code true} if a matching value was found, or {@code false} otherwise
     */
    public static <T> boolean contains(MongoIterable<String> iterable, String match) {
        return contains(iterable, v -> Objects.equals(v, match));
    }

    /**
     * Determine if the supplied {@link MongoIterable} contains at least one element that satisfies the given predicate.
     *
     * @param iterable the iterable; may not be null
     * @param matcher the predicate function called on each value in the iterable until a match is found; may not be null
     * @return {@code true} if a matching value was found, or {@code false} otherwise
     */
    public static <T> boolean contains(MongoIterable<T> iterable, Predicate<T> matcher) {
        try (MongoCursor<T> cursor = iterable.iterator()) {
            while (cursor.hasNext()) {
                if (matcher.test(cursor.next())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Helper function to extract the session transaction-id from an Change Stream event.
     *
     * @param event the Change Stream event
     * @return the session transaction id from the event
     */
    public static SourceInfo.SessionTransactionId getChangeStreamSessionTransactionId(ChangeStreamDocument<BsonDocument> event) {
        if (event.getLsid() == null || event.getTxnNumber() == null) {
            return null;
        }

        return new SourceInfo.SessionTransactionId(event.getLsid() == null ? null : event.getLsid().toJson(JsonSerialization.COMPACT_JSON_SETTINGS),
                event.getTxnNumber() == null ? null : event.getTxnNumber().longValue());
    }

    /**
     * Retrieves cluster description, forcing a connection if not yet available
     *
     * @param client cluster connection client
     * @return cluster description
     */
    public static ClusterDescription clusterDescription(MongoClient client) {
        ClusterDescription description = client.getClusterDescription();

        if (description.getType() == ClusterType.UNKNOWN) {
            // force the connection and try again
            client.listDatabaseNames().first(); // force the connection
            description = client.getClusterDescription();
        }

        return description;
    }

    public static Optional<String> replicaSetName(ClusterDescription clusterDescription) {
        var servers = clusterDescription.getServerDescriptions();

        return servers.stream()
                .map(ServerDescription::getSetName)
                .filter(Objects::nonNull)
                .findFirst();
    }

    /**
     * Opens change stream based on {@link MongoDbConnectorConfig#getCaptureScope()}
     *
     * @param client mongodb client
     * @param taskContext task context
     * @return change stream iterable
     */
    public static ChangeStreamIterable<BsonDocument> openChangeStream(MongoClient client, MongoDbTaskContext taskContext) {
        var config = taskContext.getConnectorConfig();
        final ChangeStreamPipeline pipeline = new ChangeStreamPipelineFactory(config, taskContext.getFilters().getConfig()).create();

        // capture scope is database
        if (config.getCaptureScope() == MongoDbConnectorConfig.CaptureScope.DATABASE) {
            var database = config.getCaptureTarget().orElseThrow();
            LOGGER.info("Change stream is restricted to '{}' database", database);
            return client.getDatabase(database).watch(pipeline.getStages(), BsonDocument.class);
        }

        // capture scope is collection
        if (config.getCaptureScope() == MongoDbConnectorConfig.CaptureScope.COLLECTION) {
            var captureTarget = config.getCaptureTarget().orElseThrow();
            var database = captureTarget.split("\\.")[0];
            var collection = captureTarget.split("\\.")[1];
            LOGGER.info("Change stream is restricted to '{}' collection", collection);
            return client.getDatabase(database).getCollection(collection).watch(pipeline.getStages(), BsonDocument.class);
        }

        // capture scope is deployment
        return client.watch(pipeline.getStages(), BsonDocument.class);
    }

    public static BsonTimestamp hello(MongoClient client, String dbName) {
        var database = client.getDatabase(dbName);
        BsonDocument result;
        try {
            result = database.runCommand(new Document("hello", 1), BsonDocument.class);
        }
        catch (MongoException e) {
            LOGGER.error(e.getMessage(), e);
            result = database.runCommand(new Document("isMaster", 1), BsonDocument.class);
        }
        return result.getTimestamp("operationTime");
    }

    private MongoUtils() {
    }
}
