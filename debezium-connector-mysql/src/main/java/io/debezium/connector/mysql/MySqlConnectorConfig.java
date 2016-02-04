/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.relational.history.KafkaDatabaseHistory;
import io.debezium.util.Collect;

/**
 * The configuration properties.
 */
@SuppressWarnings("unchecked")
public class MySqlConnectorConfig {

    public static final Field USER = Field.create("database.user")
                                          .withDescription("Name of the database user to be used when connecting to the database.")
                                          .withValidation(Field::isRequired);

    public static final Field PASSWORD = Field.create("database.password")
                                              .withDescription("Password to be used when connecting to the database.")
                                              .withValidation(Field::isRequired);

    public static final Field HOSTNAME = Field.create("database.hostname")
                                              .withDescription("IP address of the MySQL database server.")
                                              .withValidation(Field::isRequired);

    public static final Field PORT = Field.create("database.port")
                                          .withDescription("Port of the MySQL database server.")
                                          .withDefault(3306)
                                          .withValidation(Field::isRequired, Field::isInteger);

    public static final Field SERVER_ID = Field.create("database.server.id")
                                               .withDescription("A numeric ID of this database client, which must be unique across all currently-running database processes in the cluster. This is required because this connector essentially joins the MySQL database cluster as another server (with this unique ID) so it can read the binlog.")
                                               .withValidation(Field::isRequired, Field::isInteger);

    public static final Field SERVER_NAME = Field.create("database.server.name")
                                                 .withDescription("A unique name that identifies the database server that this connector monitors. Each database server should be monitored by at most one Debezium connector, since this server name delineates all persisted data eminating from this server. Defaults to 'host:port'")
                                                 .withValidation(Field::isRequired);

    public static final Field CONNECTION_TIMEOUT_MS = Field.create("connect.timeout.ms")
                                                           .withDescription("Maximum time in milliseconds to wait after trying to connect to the database before timing out.")
                                                           .withDefault(30 * 1000)
                                                           .withValidation(Field::isPositiveInteger);

    public static final Field KEEP_ALIVE = Field.create("connect.keep.alive")
                                                .withDescription("Whether a separate thread should be used to ensure the connection is kept alive.")
                                                .withDefault(true)
                                                .withValidation(Field::isBoolean);

    public static final Field MAX_QUEUE_SIZE = Field.create("max.queue.size")
                                                    .withDescription("Maximum size of the queue for change events read from the database log but not yet recorded or forwarded. Defaults to 2048, and should always be larger than the maximum batch size.")
                                                    .withDefault(2048)
                                                    .withValidation(MySqlConnectorConfig::validateMaxQueueSize);

    public static final Field MAX_BATCH_SIZE = Field.create("max.batch.size")
                                                    .withDescription("Maximum size of each batch of source records. Defaults to 1024.")
                                                    .withDefault(1024)
                                                    .withValidation(Field::isPositiveInteger);

    public static final Field POLL_INTERVAL_MS = Field.create("poll.interval.ms")
                                                      .withDescription("Frequency in milliseconds to poll for new change events. Defaults to 1 second (1000 ms)")
                                                      .withDefault(TimeUnit.SECONDS.toMillis(1))
                                                      .withValidation(Field::isPositiveInteger);

    public static final Field DATABASE_HISTORY = Field.create("database.history")
                                                      .withDescription("The name of the DatabaseHistory class that should be used to store and recover database schema changes. "
                                                              + "The configuration properties for the history can be specified with the '"
                                                              + DatabaseHistory.CONFIG_PREFIX + "' prefix.")
                                                      .withDefault(KafkaDatabaseHistory.class.getName());

    public static final Field INCLUDE_SCHEMA_CHANGES = Field.create("include.schema.changes")
                                                            .withDescription("Whether schema changes should be included in the "
                                                                    + "change events (in addition to storing in the database "
                                                                    + "history). The default is 'false'.")
                                                            .withDefault(false)
                                                            .withValidation(Field::isBoolean);

    public static final Field TABLE_WHITELIST = Field.create("table.whitelist")
                                                     .withDescription("A comma-separated list of table identifiers to be monitored, where each identifer consists "
                                                             + "of the '<databaseName>.<tableName>'. A whitelist takes precedence over any blacklist.");

    public static final Field TABLE_BLACKLIST = Field.create("table.blacklist")
                                                     .withDescription("A comma-separated list of table identifiers to not be monitored, where each identifer consists "
                                                             + "of the '<databaseName>.<tableName>'. Any whitelist takes precedence over this blacklist.");

    public static final Field DATABASE_WHITELIST = Field.create("database.whitelist")
                                                        .withDescription("A comma-separated list of database names to be monitored. A database whitelist takes precedence over any database blacklist and supersedes a table whitelist or table blacklist.");

    public static final Field DATABASE_BLACKLIST = Field.create("database.blacklist")
                                                        .withDescription("A comma-separated list of database names to not be monitored. Any database whitelist takes precedence over this blacklist, and supersedes a table whitelist or table blacklist.");

    public static Collection<Field> ALL_FIELDS = Collect.arrayListOf(USER, PASSWORD, HOSTNAME, PORT, SERVER_ID,
                                                                     SERVER_NAME, CONNECTION_TIMEOUT_MS, KEEP_ALIVE,
                                                                     MAX_QUEUE_SIZE, MAX_BATCH_SIZE, POLL_INTERVAL_MS,
                                                                     DATABASE_HISTORY, INCLUDE_SCHEMA_CHANGES,
                                                                     TABLE_WHITELIST, TABLE_BLACKLIST,
                                                                     DATABASE_WHITELIST, DATABASE_BLACKLIST);

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
}
