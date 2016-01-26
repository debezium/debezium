/*
 * Copyright 2015 Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.mysql;

import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Field;

/**
 * The configuration properties.
 */
public class MySqlConfiguration {

    public static final Field USER = Configuration.field("database.user",
                                                         "Name of the database user to be used when connecting to the database");
    public static final Field PASSWORD = Configuration.field("database.password",
                                                             "Password to be used when connecting to the database");
    public static final Field HOSTNAME = Configuration.field("database.hostname", "IP address of the database");
    public static final Field PORT = Configuration.field("database.port", "Port of the database", 5432);
    public static final Field SERVER_ID = Configuration.field("connect.id",
                                                              "ID of this database client, which must be unique across all database processes in the cluster.");
    public static final Field CONNECTION_TIMEOUT_MS = Configuration.field("connect.timeout.ms",
                                                                          "Maximum time in milliseconds to wait after trying to connect to the database before timing out.",
                                                                          30 * 1000);
    public static final Field KEEP_ALIVE = Configuration.field("connect.keep.alive",
                                                               "Whether a separate thread should be used to ensure the connection is kept alive.",
                                                               true);
    public static final Field MAX_QUEUE_SIZE = Configuration.field("max.queue.size",
                                                                   "Maximum size of the queue for change events read from the database log but not yet recorded or forwarded. Should be larger than the maximum batch size.",
                                                                   2048);
    public static final Field MAX_BATCH_SIZE = Configuration.field("max.batch.size", "Maximum size of each batch of source records.",
                                                                   1024);
    public static final Field POLL_INTERVAL_MS = Configuration.field("poll.interval.ms",
                                                                     "Frequency in milliseconds to poll for new change events", 1 * 1000);
    public static final Field LOGICAL_ID = Configuration.field("database.logical.id",
                                                               "Logical unique identifier for this database. Defaults to host:port");
}
