/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.mysql.source;

import io.debezium.config.Field;

/**
 * The configuration properties.
 */
public class ConnectorConfig {

    public static final Field USER = Field.create("database.user",
                                                  "Name of the database user to be used when connecting to the database.");
    public static final Field PASSWORD = Field.create("database.password",
                                                      "Password to be used when connecting to the database.");
    public static final Field HOSTNAME = Field.create("database.hostname", "IP address of the MySQL database server.");
    public static final Field PORT = Field.create("database.port", "Port of the MySQL database server.", 3306);
    public static final Field SERVER_ID = Field.create("database.server.id",
                                                       "A numeric ID of this database client, which must be unique across all currently-running database processes in the cluster. This is required because this connector essentially joins the MySQL database cluster as another server (with this unique ID) so it can read the binlog.");
    public static final Field SERVER_NAME = Field.create("database.server.name",
                                                         "A unique name that identifies the database server that this connector monitors. Each database server should be monitored by at most one Debezium connector, since this server name delineates all persisted data eminating from this server. Defaults to 'host:port'");
    public static final Field CONNECTION_TIMEOUT_MS = Field.create("connect.timeout.ms",
                                                                   "Maximum time in milliseconds to wait after trying to connect to the database before timing out.",
                                                                   30 * 1000);
    public static final Field KEEP_ALIVE = Field.create("connect.keep.alive",
                                                        "Whether a separate thread should be used to ensure the connection is kept alive.",
                                                        true);
    public static final Field MAX_QUEUE_SIZE = Field.create("max.queue.size",
                                                            "Maximum size of the queue for change events read from the database log but not yet recorded or forwarded. Defaults to 2048, and should always be larger than the maximum batch size.",
                                                            2048);
    public static final Field MAX_BATCH_SIZE = Field.create("max.batch.size", "Maximum size of each batch of source records. Defaults to 1024.",
                                                            1024);
    public static final Field POLL_INTERVAL_MS = Field.create("poll.interval.ms",
                                                              "Frequency in milliseconds to poll for new change events.", 1 * 1000);
}
