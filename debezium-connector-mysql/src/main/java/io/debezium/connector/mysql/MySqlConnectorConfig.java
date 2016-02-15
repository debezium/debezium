/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.Collection;
import java.util.Random;
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

    public static final Field SERVER_NAME = Field.create("database.server.name")
                                                 .withDescription("A unique name that identifies the database server that this connector monitors. Each database server should be monitored by at most one Debezium connector, since this server name prefixes all persisted Kakfa topics eminating from this server. Defaults to 'host:port'")
                                                 .withValidation(Field::isRequired);

    public static final Field SERVER_ID = Field.create("database.server.id")
                                               .withDescription("A numeric ID of this database client, which must be unique across all currently-running database processes in the cluster. This connector joins the MySQL database cluster as another server (with this unique ID) so it can read the binlog. By default, a random number is generated between 5400 and 6400.")
                                               .withValidation(Field::isPositiveInteger)
                                               .withDefault(MySqlConnectorConfig::randomServerId);

    public static final Field INITIAL_BINLOG_FILENAME = Field.create("database.binlog")
                                                             .withDescription("The name of the first binlog filename that should be processed by this connector. "
                                                                     + "This should include the basename and extension.")
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
                                                      .withDescription("Frequency in milliseconds to wait for new change events to appear after receiving no events. Defaults to 1 second (1000 ms).")
                                                      .withDefault(TimeUnit.SECONDS.toMillis(1))
                                                      .withValidation(Field::isPositiveInteger);

    public static final Field DATABASE_HISTORY = Field.create("database.history")
                                                      .withDescription("The name of the DatabaseHistory class that should be used to store and recover database schema changes. "
                                                              + "The configuration properties for the history are prefixed with the '"
                                                              + DatabaseHistory.CONFIGURATION_FIELD_PREFIX_STRING + "' string.")
                                                      .withDefault(KafkaDatabaseHistory.class.getName());

    public static final Field INCLUDE_SCHEMA_CHANGES = Field.create("include.schema.changes")
                                                            .withDescription("Whether the connector should publish changes in the database schema to a Kafka topic with "
                                                                    + "the same name as the database server ID. Each schema change will be recorded using a key that "
                                                                    + "contains the database name and whose value includes the DDL statement(s)."
                                                                    + "The default is 'false'. This is independent of how the connector internally records database history.")
                                                            .withDefault(false)
                                                            .withValidation(Field::isBoolean);

    public static final Field TABLE_BLACKLIST = Field.create("table.blacklist")
                                                     .withValidation(MySqlConnectorConfig::validateTableBlacklist)
                                                     .withDescription("A comma-separated list of table identifiers to be excluded from monitoring, where each identifer is of the form "
                                                             + "'<databaseName>.<tableName>'.");

    public static final Field TABLE_WHITELIST = Field.create("table.whitelist")
                                                     .withDescription("A comma-separated list of table identifiers to be monitored, where each identifer is of the form "
                                                             + "'<databaseName>.<tableName>'. May not be used with '" + TABLE_BLACKLIST
                                                             + "'. "
                                                             + "The named table will be monitored only if its database is allowed by the `database.whitelist` or "
                                                             + "not disallowed by '" + TABLE_BLACKLIST + "'.");

    public static final Field DATABASE_WHITELIST = Field.create("database.whitelist")
                                                        .withDescription("A comma-separated list of database names to be monitored. "
                                                                + "May not be used with 'database.blacklist'.");

    public static final Field DATABASE_BLACKLIST = Field.create("database.blacklist")
                                                        .withValidation(MySqlConnectorConfig::validateDatabaseBlacklist)
                                                        .withDescription("A comma-separated list of database names to be excluded from monitoring. "
                                                                + "May not be used with '" + DATABASE_WHITELIST + "'.");

    public static final Field TABLES_IGNORE_BUILTIN = Field.create("table.ignore.builtin")
                                                           .withValidation(Field::isBoolean)
                                                           .withDescription("Flag specifying whether built-in tables should be ignored. This applies regardless of the table whitelist or blacklists.")
                                                           .withDefault(true);

    public static Collection<Field> ALL_FIELDS = Collect.arrayListOf(USER, PASSWORD, HOSTNAME, PORT, SERVER_ID,
                                                                     SERVER_NAME, INITIAL_BINLOG_FILENAME,
                                                                     CONNECTION_TIMEOUT_MS, KEEP_ALIVE,
                                                                     MAX_QUEUE_SIZE, MAX_BATCH_SIZE, POLL_INTERVAL_MS,
                                                                     DATABASE_HISTORY, INCLUDE_SCHEMA_CHANGES,
                                                                     TABLE_WHITELIST, TABLE_BLACKLIST, TABLES_IGNORE_BUILTIN,
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

    private static int validateDatabaseBlacklist(Configuration config, Field field, Consumer<String> problems) {
        String whitelist = config.getString(DATABASE_WHITELIST);
        String blacklist = config.getString(DATABASE_BLACKLIST);
        if (whitelist != null && blacklist != null) {
            problems.accept("May use either '" + DATABASE_WHITELIST + "' or '" + DATABASE_BLACKLIST + "', but not both.");
            return 1;
        }
        return 0;
    }

    private static int validateTableBlacklist(Configuration config, Field field, Consumer<String> problems) {
        String whitelist = config.getString(TABLE_WHITELIST);
        String blacklist = config.getString(TABLE_BLACKLIST);
        if (whitelist != null && blacklist != null) {
            problems.accept("May use either '" + TABLE_WHITELIST + "' or '" + TABLE_BLACKLIST + "', but not both.");
            return 1;
        }
        return 0;
    }

    private static int randomServerId() {
        int lowestServerId = 5400;
        int highestServerId = 6400;
        return lowestServerId + new Random().nextInt(highestServerId - lowestServerId);
    }
}
