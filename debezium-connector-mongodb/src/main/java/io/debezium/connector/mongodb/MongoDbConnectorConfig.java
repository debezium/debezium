/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.util.Collect;

/**
 * The configuration properties.
 */
public class MongoDbConnectorConfig {

    public static final Field HOSTS = Field.create("mongodb.hosts")
                                           .withDescription("The comma-separated list of hostname and port pairs (in the form 'host' or 'host:port') of the MongoDB servers in the replica set. The list can contain a single hostname and port pair.")
                                           .withValidation(MongoDbConnectorConfig::validateHosts);

    public static final Field LOGICAL_NAME = Field.create("mongodb.name")
                                                  .withDescription("A unique name that identifies the connector and/or MongoDB replica set or cluster that this connector monitors. Each server should be monitored by at most one Debezium connector, since this server name prefixes all persisted Kakfa topics eminating from this server.")
                                                  .withValidation(Field::isRequired);

    public static final Field USER = Field.create("mongodb.user")
                                          .withDescription("Name of the database user to be used when connecting to MongoDB. "
                                                  + "This is required only when MongoDB is configured to use authentication.")
                                          .withValidation(Field::isOptional);

    public static final Field PASSWORD = Field.create("mongodb.password")
                                              .withDescription("Password to be used when connecting to MongoDB. "
                                                      + "This is required only when MongoDB is configured to use authentication.")
                                              .withValidation(Field::isOptional);

    public static final Field POLL_INTERVAL_SEC = Field.create("mongodb.poll.interval.sec")
                                                       .withDescription("Frequency in seconds to look for new, removed, or changed replica sets. Defaults to 30 seconds.")
                                                       .withDefault(30)
                                                       .withValidation(Field::isPositiveInteger);

    public static final Field MAX_COPY_THREADS = Field.create("initial.sync.max.threads")
                                                      .withDescription("Maximum number of threads used to perform an intial sync of the collections in a replica set. "
                                                              + "Defaults to 1.")
                                                      .withDefault(1)
                                                      .withValidation(Field::isPositiveInteger);

    public static final Field MAX_QUEUE_SIZE = Field.create("max.queue.size")
                                                    .withDescription("Maximum size of the queue for change events read from the database log but not yet recorded or forwarded. Defaults to 2048, and should always be larger than the maximum batch size.")
                                                    .withDefault(2048)
                                                    .withValidation(MongoDbConnectorConfig::validateMaxQueueSize);

    public static final Field MAX_BATCH_SIZE = Field.create("max.batch.size")
                                                    .withDescription("Maximum size of each batch of source records. Defaults to 1024.")
                                                    .withDefault(1024)
                                                    .withValidation(Field::isPositiveInteger);

    public static final Field POLL_INTERVAL_MS = Field.create("poll.interval.ms")
                                                      .withDescription("Frequency in milliseconds to wait after processing no events for new change events to appear. Defaults to 1 second (1000 ms).")
                                                      .withDefault(TimeUnit.SECONDS.toMillis(1))
                                                      .withValidation(Field::isPositiveInteger);

    public static final Field CONNECT_BACKOFF_INITIAL_DELAY_MS = Field.create("connect.backoff.initial.delay.ms")
                                                                      .withDescription("The initial delay when trying to reconnect to a primary after a connection cannot be made or when no primary is available. Defaults to 1 second (1000 ms).")
                                                                      .withDefault(TimeUnit.SECONDS.toMillis(1))
                                                                      .withValidation(Field::isPositiveInteger);

    public static final Field CONNECT_BACKOFF_MAX_DELAY_MS = Field.create("connect.backoff.max.delay.ms")
                                                                  .withDescription("The maximum delay when trying to reconnect to a primary after a connection cannot be made or when no primary is available. Defaults to 120 second (120,000 ms).")
                                                                  .withDefault(TimeUnit.SECONDS.toMillis(120))
                                                                  .withValidation(Field::isPositiveInteger);

    public static final Field MAX_FAILED_CONNECTIONS = Field.create("connect.max.attempts")
                                                            .withDescription("Maximum number of failed connection attempts to a replica set primary before an exception occurs and task is aborted. "
                                                                    + "Defaults to 16, which with the defaults for '" + CONNECT_BACKOFF_INITIAL_DELAY_MS + "' and '" + CONNECT_BACKOFF_MAX_DELAY_MS + "' results in "
                                                                            + "just over 20 minutes of attempts before failing.")
                                                            .withDefault(16)
                                                            .withValidation(Field::isPositiveInteger);

    public static final Field COLLECTION_BLACKLIST = Field.create("collection.blacklist")
                                                          .withValidation(MongoDbConnectorConfig::validateCollectionBlacklist)
                                                          .withDescription("A comma-separated list of regular expressions that match the fully-qualified namespaces of collections to be excluded from monitoring. "
                                                                  + "Fully-qualified namespaces for collections are of the form '<databaseName>.<collectionName>'. "
                                                                  + "May not be used with 'collection.whitelist'.");

    public static final Field COLLECTION_WHITELIST = Field.create("collection.whitelist")
                                                          .withDescription("A comma-separated list of regular expressions that match the fully-qualified namespaces of collections to be monitored. "
                                                                  + "Fully-qualified namespaces for collections are of the form '<databaseName>.<collectionName>'. "
                                                                  + "May not be used with '" + COLLECTION_BLACKLIST + "'.");

    public static final Field AUTO_DISCOVER_MEMBERS = Field.create("mongodb.members.auto.discover",
                                                                   "Specifies whether the addresses in 'hosts' are seeds that should be used to discover all members of the cluster or replica set ('true'), "
                                                                           + "or whether the address(es) in 'hosts' should be used as is ('false'). The default is 'true'.")
                                                           .withDefault(true).withValidation(Field::isBoolean);

    public static final Field TASK_ID = Field.create("mongodb.task.id")
                                             .withDescription("Internal use only")
                                             .withValidation(Field::isInteger);

    public static Collection<Field> ALL_FIELDS = Collections.unmodifiableList(Collect.arrayListOf(USER, PASSWORD, HOSTS, LOGICAL_NAME,
                                                                                                  MAX_QUEUE_SIZE, MAX_BATCH_SIZE,
                                                                                                  POLL_INTERVAL_MS,
                                                                                                  MAX_FAILED_CONNECTIONS,
                                                                                                  CONNECT_BACKOFF_INITIAL_DELAY_MS,
                                                                                                  CONNECT_BACKOFF_MAX_DELAY_MS,
                                                                                                  COLLECTION_WHITELIST,
                                                                                                  COLLECTION_BLACKLIST,
                                                                                                  AUTO_DISCOVER_MEMBERS));

    private static int validateHosts(Configuration config, Field field, Consumer<String> problems) {
        String hosts = config.getString(field);
        if (hosts == null) {
            problems.accept("'" + field + "' is required.");
            return 1;
        }
        int count = 0;
        if (ReplicaSets.parse(hosts) == null) {
            problems.accept("'" + hosts + "' is not valid host specification");
            ++count;
        }
        return count;
    }

    private static int validateMaxQueueSize(Configuration config, Field field, Consumer<String> problems) {
        int maxQueueSize = config.getInteger(field);
        int maxBatchSize = config.getInteger(MAX_BATCH_SIZE);
        int count = 0;
        if (maxQueueSize <= 0) {
            maxBatchSize = maxQueueSize / 2;
            problems.accept("The " + MAX_QUEUE_SIZE + " value '" + maxQueueSize + "' must be positive");
            ++count;
        }
        if (maxQueueSize <= maxBatchSize) {
            maxBatchSize = maxQueueSize / 2;
            problems.accept("The " + MAX_QUEUE_SIZE + " value '" + maxQueueSize + "' must be larger than " +
                    MAX_BATCH_SIZE + " of '" + maxBatchSize + ".");
            ++count;
        }
        return count;
    }

    private static int validateCollectionBlacklist(Configuration config, Field field, Consumer<String> problems) {
        String whitelist = config.getString(COLLECTION_WHITELIST);
        String blacklist = config.getString(COLLECTION_BLACKLIST);
        if (whitelist != null && blacklist != null) {
            problems.accept("May use either '" + COLLECTION_WHITELIST + "' or '" + COLLECTION_BLACKLIST + "', but not both.");
            return 1;
        }
        return 0;
    }
}
