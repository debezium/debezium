/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;

import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;

import io.debezium.DebeziumException;
import io.debezium.connector.mongodb.util.ResumeTokenDecoder;
import io.debezium.function.BlockingConsumer;
import io.debezium.util.Strings;

/**
 * Utilities for working with MongoDB.
 *
 * @author Randall Hauch
 */
public class MongoUtil {

    /**
     * The delimiter used between addresses.
     */
    private static final String ADDRESS_DELIMITER = ",";

    public static final Pattern ADDRESS_DELIMITER_PATTERN = Pattern.compile(ADDRESS_DELIMITER);

    /**
     * Regular expression that gets the host and (optional) port. The raw expression is {@code ([^:]+)(:(\d+))?}.
     */
    private static final Pattern ADDRESS_PATTERN = Pattern.compile("([^:]+)(:(\\d+))?");

    /**
     * Regular expression that gets the IPv6 host and (optional) port, where the IPv6 address must be surrounded
     * by square brackets. The raw expression is {@code (\[[^]]+\])(:(\d+))?}.
     */
    private static final Pattern IPV6_ADDRESS_PATTERN = Pattern.compile("(\\[[^]]+\\])(:(\\d+))?");

    /**
     * Find the name of the replica set precedes the host addresses.
     *
     * @param addresses the string containing the host addresses, of the form {@code replicaSetName/...}; may not be null
     * @return the replica set name, or {@code null} if no replica set name is in the string
     */
    public static String replicaSetUsedIn(String addresses) {
        if (addresses.startsWith("[")) {
            // Just an IPv6 address, so no replica set name ...
            return null;
        }
        // Either a replica set name + an address, or just an IPv4 address ...
        int index = addresses.indexOf('/');
        if (index < 0) {
            return null;
        }
        return addresses.substring(0, index);
    }

    /**
     * Perform the given operation on each of the database names.
     *
     * @param client    the MongoDB client; may not be null
     * @param operation the operation to perform; may not be null
     */
    public static void forEachDatabaseName(MongoClient client, Consumer<String> operation) {
        forEach(client.listDatabaseNames(), operation);
    }

    /**
     * Perform the given operation on each of the collection names in the named database.
     *
     * @param client       the MongoDB client; may not be null
     * @param databaseName the name of the database; may not be null
     * @param operation    the operation to perform; may not be null
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
     * @param client      the MongoDB client; may not be null
     * @param dbName      the name of the database; may not be null
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
     * @param client              the MongoDB client; may not be null
     * @param dbName              the name of the database; may not be null
     * @param collectionName      the name of the collection; may not be null
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
     * @param client            the MongoDB client; may not be null
     * @param dbName            the name of the database; may not be null
     * @param collectionName    the name of the collection; may not be null
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
     * @param match    the value to find in the iterable; may be null
     * @return {@code true} if a matching value was found, or {@code false} otherwise
     */
    public static <T> boolean contains(MongoIterable<String> iterable, String match) {
        return contains(iterable, v -> Objects.equals(v, match));
    }

    /**
     * Determine if the supplied {@link MongoIterable} contains at least one element that satisfies the given predicate.
     *
     * @param iterable the iterable; may not be null
     * @param matcher  the predicate function called on each value in the iterable until a match is found; may not be null
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
     * Parse the server address string, of the form {@code host:port} or {@code host}.
     * <p>
     * The IP address can be either an IPv4 address, or an IPv6 address surrounded by square brackets.
     *
     * @param addressStr the string containing the host and port; may be null
     * @return the server address, or {@code null} if the string did not contain a host or host:port pair
     */
    public static ServerAddress parseAddress(String addressStr) {
        if (addressStr != null) {
            addressStr = addressStr.trim();
            Matcher matcher = ADDRESS_PATTERN.matcher(addressStr);
            if (!matcher.matches()) {
                matcher = IPV6_ADDRESS_PATTERN.matcher(addressStr);
            }
            if (matcher.matches()) {
                // Both regex have the same groups
                String host = matcher.group(1);
                String port = matcher.group(3);
                if (port == null) {
                    return new ServerAddress(host.trim());
                }
                return new ServerAddress(host.trim(), Integer.parseInt(port));
            }
        }
        return null;
    }

    /**
     * Helper function to extract the session transaction-id from an oplog event.
     *
     * @param oplogEvent the oplog event
     * @return the session transaction id from the oplog event
     */
    public static String getOplogSessionTransactionId(BsonDocument oplogEvent) {
        if (!oplogEvent.containsKey("txnNumber")) {
            return null;
        }
        final BsonDocument lsidDoc = oplogEvent.getDocument("lsid");
        final BsonValue id = lsidDoc.get("id");
        // MongoDB 4.2 returns Binary instead of UUID
        final String lsid = id.isBinary() ? id.asBinary().asUuid().toString() : id.asString().getValue();
        final Long txnNumber = oplogEvent.get("txnNumber").isInt32() ? oplogEvent.getInt32("txnNumber").getValue() : oplogEvent.getInt64("txnNumber").getValue();
        return lsid + ":" + txnNumber;
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
     * Helper function that returns the txn op index from the changeStreamDocument, otherwise returns 0 if it does not exist
     * @return a unique index of the event within the transaction. This is different from the transaction id of the overall transaction
     * @throws DebeziumException if any unexpected exceptions or errors
     * */
    public static long getChangeStreamTxnIdx(ChangeStreamDocument<BsonDocument> event) throws DebeziumException {
        // if this isn't a transaction event, return early
        if (event.getLsid() == null || event.getTxnNumber() == null) {
            return 0;
        }
        try {
            // add 1 because txnOpIndex starts with 0 for first transaction event
            return 1 + ResumeTokenDecoder.txnOpIndexFromTokenHex(event.getResumeToken().get("_data").asString().getValue());
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }
    }

    /**
     * similar to ChangeStreamDocument.toString() except it filters out some details like the fullDocument and the fullDocumentBeforeChange
     * See <a href="https://github.com/mongodb/mongo-java-driver/blob/4.7.x/driver-core/src/main/com/mongodb/client/model/changestream/ChangeStreamDocument.java#L529-L544">ChangeStreamDocument.java</a>
     * @param event
     * @return the ChangeStreamDocument event as a string with appropriate details
     */
    public static String changeStreamEventToStringCompact(ChangeStreamDocument<BsonDocument> event) {
        // TODO: dynamically construct string
        return "ChangeStreamDocument{ operationType=" + event.getOperationTypeString() + ", resumeToken=" + event.getResumeToken() + ", namespace=" + event.getNamespace() + ", destinationNamespace=" + event.getDestinationNamespace() +  ", documentKey=" + event.getDocumentKey() + ", clusterTime=" + event.getClusterTime() + ", updateDescription=" + event.getUpdateDescription() + ", txnNumber=" + event.getTxnNumber() + ", lsid=" + event.getLsid() + ", wallTime=" + event.getWallTime() + "}";
    }

    /**
     * Parse the comma-separated list of server addresses. The format of the supplied string is one of the following:
     *
     * <pre>
     * replicaSetName/host:port
     * replicaSetName/host:port,host2:port2
     * replicaSetName/host:port,host2:port2,host3:port3
     * host:port
     * host:port,host2:port2
     * host:port,host2:port2,host3:port3
     * </pre>
     * <p>
     * where {@code replicaSetName} is the name of the replica set, {@code host} contains the resolvable hostname or IP address of
     * the server, and {@code port} is the integral port number. If the port is not provided, the
     * {@link ServerAddress#defaultPort() default port} is used. If neither the host or port are provided (or
     * {@code addressString} is {@code null}), then an address will use the {@link ServerAddress#defaultHost() default host} and
     * {@link ServerAddress#defaultPort() default port}.
     * <p>
     * The IP address can be either an IPv4 address, or an IPv6 address surrounded by square brackets.
     * <p>
     * This method does not use the replica set name.
     *
     * @param addressStr the string containing a comma-separated list of host and port pairs, optionally preceded by a
     *                   replica set name
     * @return the list of server addresses; never null, but possibly empty
     */
    protected static List<ServerAddress> parseAddresses(String addressStr) {
        List<ServerAddress> addresses = new ArrayList<>();
        if (addressStr != null) {
            addressStr = addressStr.trim();
            for (String address : ADDRESS_DELIMITER_PATTERN.split(addressStr)) {
                String hostAndPort = null;
                if (address.startsWith("[")) {
                    // Definitely an IPv6 address without a replica set name ...
                    hostAndPort = address;
                }
                else {
                    // May start with replica set name ...
                    int index = address.indexOf("/[");
                    if (index >= 0) {
                        if ((index + 2) < address.length()) {
                            // replica set name with IPv6, so use just the IPv6 address ...
                            hostAndPort = address.substring(index + 1);
                        }
                        else {
                            // replica set name with just opening bracket; this is invalid, so we'll ignore ...
                            continue;
                        }
                    }
                    else {
                        // possible replica set name with IPv4 only
                        index = address.indexOf("/");
                        if (index >= 0) {
                            if ((index + 1) < address.length()) {
                                // replica set name with IPv4, so use just the IPv4 address ...
                                hostAndPort = address.substring(index + 1);
                            }
                            else {
                                // replica set name with no address ...
                                hostAndPort = ServerAddress.defaultHost();
                            }
                        }
                        else {
                            // No replica set name with IPv4, so use the whole address ...
                            hostAndPort = address;
                        }
                    }
                }
                ServerAddress newAddress = parseAddress(hostAndPort);
                if (newAddress != null) {
                    addresses.add(newAddress);
                }
            }
        }
        return addresses;
    }

    protected static String toString(ServerAddress address) {
        String host = address.getHost();
        if (host.contains(":")) {
            // IPv6 address, so wrap with square brackets ...
            return "[" + host + "]:" + address.getPort();
        }
        return host + ":" + address.getPort();
    }

    protected static String toString(List<ServerAddress> addresses) {
        return Strings.join(ADDRESS_DELIMITER, addresses);
    }

    protected static ServerAddress getPrimaryAddress(MongoClient client) {

        ClusterDescription clusterDescription = client.getClusterDescription();

        if (clusterDescription == null || !clusterDescription.hasReadableServer(ReadPreference.primaryPreferred())) {
            client.listDatabaseNames().first(); // force connection attempt and make async client wait/block
            clusterDescription = client.getClusterDescription();
        }

        if (clusterDescription == null) {
            throw new DebeziumException("Unable to read cluster description from MongoDB connection.");
        }
        else if (!clusterDescription.hasReadableServer(ReadPreference.primaryPreferred())) {
            throw new DebeziumException("Unable to use cluster description from MongoDB connection: " + clusterDescription);
        }

        List<ServerDescription> serverDescriptions = clusterDescription.getServerDescriptions();

        if (serverDescriptions == null || serverDescriptions.size() == 0) {
            throw new DebeziumException("Unable to read server descriptions from MongoDB connection (Null or empty list).");
        }

        Optional<ServerDescription> primaryDescription = serverDescriptions.stream().filter(ServerDescription::isPrimary).findFirst();

        if (!primaryDescription.isPresent()) {
            for (ServerDescription description : serverDescriptions) {
                if (description != null && description.getPrimary() != null && !description.getPrimary().isEmpty()) {
                    String[] address = description.getPrimary().split(":");
                    if (address.length != 2) {
                        throw new DebeziumException("Unable to parse primary address, got '" + serverDescriptions + "'");
                    }
                    return new ServerAddress(address[0], Integer.parseInt(address[1]));
                }
            }
        }

        if (!primaryDescription.isPresent()) {
            throw new DebeziumException("Unable to find primary from MongoDB connection, got '" + serverDescriptions + "'");
        }

        ServerAddress primaryAddress = primaryDescription.get().getAddress();

        return new ServerAddress(primaryAddress.getHost(), primaryAddress.getPort());
    }

    private MongoUtil() {
    }
}
