/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import com.mongodb.MongoException;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.config.Field.Recommender;
import io.debezium.config.Field.ValidationOutput;
import io.debezium.connector.mongodb.ConnectionContext.MongoPrimary;

/**
 * The configuration properties.
 */
public class MongoDbConnectorConfig {

    private static final String DATABASE_LIST_NAME = "database.list";
    private static final String COLLECTION_LIST_NAME = "collection.list";
    private static final CollectionRecommender COLLECTION_LIST_RECOMMENDER = new CollectionRecommender();
    private static final DatabaseRecommender DATABASE_LIST_RECOMMENDER = new DatabaseRecommender();

    /**
     * The comma-separated list of hostname and port pairs (in the form 'host' or 'host:port') of the MongoDB servers in the
     * replica set.
     */
    public static final Field HOSTS = Field.create("mongodb.hosts")
                                           .withDisplayName("Hosts")
                                           .withType(Type.LIST)
                                           .withWidth(Width.LONG)
                                           .withImportance(Importance.HIGH)
                                           .withDependents(DATABASE_LIST_NAME)
                                           .withValidation(MongoDbConnectorConfig::validateHosts)
                                           .withDescription("The hostname and port pairs (in the form 'host' or 'host:port') "
                                                   + "of the MongoDB server(s) in the replica set.");

    public static final Field LOGICAL_NAME = Field.create("mongodb.name")
                                                  .withDisplayName("Namespace")
                                                  .withType(Type.STRING)
                                                  .withWidth(Width.MEDIUM)
                                                  .withImportance(Importance.HIGH)
                                                  .withValidation(Field::isRequired)
                                                  .withDescription("Unique name that identifies the MongoDB replica set or cluster and all recorded offsets, and"
                                                          + "that is used as a prefix for all schemas and topics. "
                                                          + "Each distinct MongoDB installation should have a separate namespace and monitored by "
                                                          + "at most one Debezium connector.");

    public static final Field USER = Field.create("mongodb.user")
                                          .withDisplayName("User")
                                          .withType(Type.STRING)
                                          .withWidth(Width.SHORT)
                                          .withImportance(Importance.HIGH)
                                          .withDependents(DATABASE_LIST_NAME)
                                          .withDescription("Database user for connecting to MongoDB, if necessary.");

    public static final Field PASSWORD = Field.create("mongodb.password")
                                              .withDisplayName("Password")
                                              .withType(Type.PASSWORD)
                                              .withWidth(Width.SHORT)
                                              .withImportance(Importance.HIGH)
                                              .withDependents(DATABASE_LIST_NAME)
                                              .withDescription("Password to be used when connecting to MongoDB, if necessary.");

    public static final Field POLL_INTERVAL_SEC = Field.create("mongodb.poll.interval.sec")
                                                       .withDisplayName("Replica membership poll interval (sec)")
                                                       .withType(Type.INT)
                                                       .withWidth(Width.SHORT)
                                                       .withImportance(Importance.MEDIUM)
                                                       .withDefault(30)
                                                       .withValidation(Field::isPositiveInteger)
                                                       .withDescription("Frequency in seconds to look for new, removed, or changed replica sets. Defaults to 30 seconds.");

    public static final Field MAX_COPY_THREADS = Field.create("initial.sync.max.threads")
                                                      .withDisplayName("Maximum number of threads for initial sync")
                                                      .withType(Type.INT)
                                                      .withWidth(Width.SHORT)
                                                      .withImportance(Importance.MEDIUM)
                                                      .withDefault(1)
                                                      .withValidation(Field::isPositiveInteger)
                                                      .withDescription("Maximum number of threads used to perform an intial sync of the collections in a replica set. "
                                                              + "Defaults to 1.");

    public static final Field MAX_QUEUE_SIZE = Field.create("max.queue.size")
                                                    .withDisplayName("Change event buffer size")
                                                    .withType(Type.INT)
                                                    .withWidth(Width.SHORT)
                                                    .withImportance(Importance.MEDIUM)
                                                    .withDefault(2048)
                                                    .withValidation(MongoDbConnectorConfig::validateMaxQueueSize)
                                                    .withDescription("Maximum size of the queue for change events read from the database log but not yet recorded or forwarded. Defaults to 2048, and should always be larger than the maximum batch size.");

    public static final Field MAX_BATCH_SIZE = Field.create("max.batch.size")
                                                    .withDisplayName("Change event batch size")
                                                    .withType(Type.INT)
                                                    .withWidth(Width.SHORT)
                                                    .withImportance(Importance.MEDIUM)
                                                    .withDefault(1024)
                                                    .withValidation(Field::isPositiveInteger)
                                                    .withDescription("Maximum size of each batch of source records. Defaults to 1024.");

    public static final Field POLL_INTERVAL_MS = Field.create("poll.interval.ms")
                                                      .withDisplayName("Poll interval (ms)")
                                                      .withType(Type.LONG)
                                                      .withWidth(Width.SHORT)
                                                      .withImportance(Importance.MEDIUM)
                                                      .withDefault(TimeUnit.SECONDS.toMillis(1))
                                                      .withValidation(Field::isPositiveInteger)
                                                      .withDescription("Frequency in milliseconds to wait after processing no events for new change events to appear. Defaults to 1 second (1000 ms).");

    public static final Field CONNECT_BACKOFF_INITIAL_DELAY_MS = Field.create("connect.backoff.initial.delay.ms")
                                                                      .withDisplayName("Initial delay before reconnection (ms)")
                                                                      .withType(Type.LONG)
                                                                      .withWidth(Width.SHORT)
                                                                      .withImportance(Importance.MEDIUM)
                                                                      .withDefault(TimeUnit.SECONDS.toMillis(1))
                                                                      .withValidation(Field::isPositiveInteger)
                                                                      .withDescription("The initial delay when trying to reconnect to a primary after a connection cannot be made or when no primary is available. Defaults to 1 second (1000 ms).");

    public static final Field CONNECT_BACKOFF_MAX_DELAY_MS = Field.create("connect.backoff.max.delay.ms")
                                                                  .withDisplayName("Maximum delay before reconnection (ms)")
                                                                  .withType(Type.LONG)
                                                                  .withWidth(Width.SHORT)
                                                                  .withImportance(Importance.MEDIUM)
                                                                  .withDefault(TimeUnit.SECONDS.toMillis(120))
                                                                  .withValidation(Field::isPositiveInteger)
                                                                  .withDescription("The maximum delay when trying to reconnect to a primary after a connection cannot be made or when no primary is available. Defaults to 120 second (120,000 ms).");

    public static final Field MAX_FAILED_CONNECTIONS = Field.create("connect.max.attempts")
                                                            .withDisplayName("Connection attempt limit")
                                                            .withType(Type.INT)
                                                            .withWidth(Width.SHORT)
                                                            .withImportance(Importance.HIGH)
                                                            .withDefault(16)
                                                            .withValidation(Field::isPositiveInteger)
                                                            .withDescription("Maximum number of failed connection attempts to a replica set primary before an exception occurs and task is aborted. "
                                                                    + "Defaults to 16, which with the defaults for '"
                                                                    + CONNECT_BACKOFF_INITIAL_DELAY_MS + "' and '"
                                                                    + CONNECT_BACKOFF_MAX_DELAY_MS + "' results in "
                                                                    + "just over 20 minutes of attempts before failing.");

    public static final Field AUTO_DISCOVER_MEMBERS = Field.create("mongodb.members.auto.discover")
                                                           .withDisplayName("Auto-discovery")
                                                           .withType(Type.BOOLEAN)
                                                           .withWidth(Width.SHORT)
                                                           .withImportance(Importance.LOW)
                                                           .withDefault(true)
                                                           .withValidation(Field::isBoolean)
                                                           .withDescription("Specifies whether the addresses in 'hosts' are seeds that should be "
                                                                   + "used to discover all members of the cluster or replica set ('true'), "
                                                                   + "or whether the address(es) in 'hosts' should be used as is ('false'). "
                                                                   + "The default is 'true'.");

    public static final Field DATABASE_LIST = Field.create(DATABASE_LIST_NAME)
                                                   .withDisplayName("Databases")
                                                   .withType(Type.LIST)
                                                   .withWidth(Width.LONG)
                                                   .withImportance(Importance.HIGH)
                                                   .withRecommender(DATABASE_LIST_RECOMMENDER)
                                                   .withDependents(COLLECTION_LIST_NAME)
                                                   .withDescription("The databases for which changes are to be captured");

    /**
     * A comma-separated list of regular expressions that match the fully-qualified namespaces of collections to be monitored.
     * Fully-qualified namespaces for collections are of the form {@code '<databaseName>.<collectionName>'}.
     * May not be used with {@link #COLLECTION_BLACKLIST}.
     */
    public static final Field COLLECTION_WHITELIST = Field.create("collection.whitelist")
                                                          .withDisplayName("Collections")
                                                          .withType(Type.LIST)
                                                          .withWidth(Width.LONG)
                                                          .withImportance(Importance.HIGH)
                                                          .withRecommender(COLLECTION_LIST_RECOMMENDER)
                                                          .withValidation(Field::isListOfRegex,
                                                                          MongoDbConnectorConfig::validateCollectionBlacklist)
                                                          .withDescription("The collections for which changes are to be captured");

    /**
     * A comma-separated list of regular expressions that match the fully-qualified namespaces of collections to be excluded from
     * monitoring. Fully-qualified namespaces for collections are of the form {@code <databaseName>.<collectionName>}.
     * May not be used with {@link #COLLECTION_WHITELIST}.
     */
    public static final Field COLLECTION_BLACKLIST = Field.create("collection.blacklist")
                                                          .withValidation(Field::isListOfRegex)
                                                          .withInvisibleRecommender();

    protected static final Field TASK_ID = Field.create("mongodb.task.id")
                                                .withDescription("Internal use only")
                                                .withValidation(Field::isInteger)
                                                .withInvisibleRecommender();

    public static Field.Set ALL_FIELDS = Field.setOf(USER, PASSWORD, HOSTS, LOGICAL_NAME,
                                                     MAX_COPY_THREADS, MAX_QUEUE_SIZE, MAX_BATCH_SIZE,
                                                     POLL_INTERVAL_MS,
                                                     MAX_FAILED_CONNECTIONS,
                                                     CONNECT_BACKOFF_INITIAL_DELAY_MS,
                                                     CONNECT_BACKOFF_MAX_DELAY_MS,
                                                     DATABASE_LIST,
                                                     COLLECTION_WHITELIST,
                                                     COLLECTION_BLACKLIST,
                                                     AUTO_DISCOVER_MEMBERS);

    protected static Field.Set EXPOSED_FIELDS = ALL_FIELDS;

    protected static ConfigDef configDef() {
        ConfigDef config = new ConfigDef();
        Field.group(config, "MongoDB", HOSTS, USER, PASSWORD, LOGICAL_NAME, CONNECT_BACKOFF_INITIAL_DELAY_MS,
                    CONNECT_BACKOFF_MAX_DELAY_MS, MAX_FAILED_CONNECTIONS, AUTO_DISCOVER_MEMBERS);
        Field.group(config, "Events", DATABASE_LIST, COLLECTION_WHITELIST, COLLECTION_BLACKLIST);
        Field.group(config, "Connector", MAX_COPY_THREADS, MAX_QUEUE_SIZE, MAX_BATCH_SIZE, POLL_INTERVAL_MS);
        return config;
    }

    protected static class DatabaseRecommender implements Recommender {

        @Override
        public List<Object> validValues(Field field, Configuration config) {
            // Make sure we connect just once
            config = config.edit().with(MAX_FAILED_CONNECTIONS, 1).build();
            Set<String> databaseNames = new HashSet<>();
            try (ConnectionContext connContext = new ConnectionContext(config)) {
                // Connect to each replica set ...
                connContext.replicaSets().onEachReplicaSet(replicaSet -> {
                    // And get the databases that are in this replica set ...
                    MongoPrimary primary = connContext.primaryFor(replicaSet, (msg, e) -> {});
                    primary.databaseNames().forEach(databaseNames::add);
                });
            } catch (MongoException e) {
                // don't do anything ...
            }
            databaseNames.removeAll(Filters.BUILT_IN_DB_NAMES);
            return new ArrayList<>(databaseNames);
        }

        @Override
        public boolean visible(Field field, Configuration config) {
            return true;
        }
    }

    protected static class CollectionRecommender implements Recommender {

        @Override
        public List<Object> validValues(Field field, Configuration config) {
            List<String> dbNames = config.getStrings(DATABASE_LIST, ",");
            if ( dbNames == null ) return new ArrayList<>();
            
            Set<String> namespaces = new HashSet<>();
            try (ConnectionContext connContext = new ConnectionContext(config)) {
                // Connect to each replica set ...
                connContext.replicaSets().onEachReplicaSet(replicaSet -> {
                    // And get the collections that are in this replica set and in the selected databases...
                    MongoPrimary primary = connContext.primaryFor(replicaSet, (msg, e) -> {});
                    primary.collections().stream()
                           .filter(id -> dbNames.contains(id.dbName()))
                           .map(CollectionId::namespace)
                           .forEach(namespaces::add);
                });
            } catch (MongoException e) {
                // don't do anything ...
            }
            return new ArrayList<>(namespaces);
        }

        @Override
        public boolean visible(Field field, Configuration config) {
            return true;
        }
    }

    private static int validateHosts(Configuration config, Field field, ValidationOutput problems) {
        String hosts = config.getString(field);
        if (hosts == null) {
            problems.accept(field, hosts, "Host specification is required");
            return 1;
        }
        int count = 0;
        if (ReplicaSets.parse(hosts) == null) {
            problems.accept(field, hosts, "Invalid host specification");
            ++count;
        }
        return count;
    }

    private static int validateMaxQueueSize(Configuration config, Field field, ValidationOutput problems) {
        int maxQueueSize = config.getInteger(field);
        int maxBatchSize = config.getInteger(MAX_BATCH_SIZE);
        int count = 0;
        if (maxQueueSize <= 0) {
            maxBatchSize = maxQueueSize / 2;
            problems.accept(field, maxQueueSize, "A positive queue size is required");
            ++count;
        }
        if (maxQueueSize <= maxBatchSize) {
            maxBatchSize = maxQueueSize / 2;
            problems.accept(field, maxQueueSize, "Must be larger than the maximum batch size");
            ++count;
        }
        return count;
    }

    private static int validateCollectionBlacklist(Configuration config, Field field, ValidationOutput problems) {
        String whitelist = config.getString(COLLECTION_WHITELIST);
        String blacklist = config.getString(COLLECTION_BLACKLIST);
        if (whitelist != null && blacklist != null) {
            problems.accept(COLLECTION_BLACKLIST, blacklist, "Whitelist is already specified");
            return 1;
        }
        return 0;
    }
}
