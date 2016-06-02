/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotMode;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcConnection.ConnectionFactory;
import io.debezium.util.Clock;

/**
 * A Kafka Connect source task reads the MySQL binary log and generate the corresponding data change events.
 * 
 * @see MySqlConnector
 * @author Randall Hauch
 */
public final class MySqlTaskContext {

    protected static ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory("jdbc:mysql://${hostname}:${port}");

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Configuration config;
    private final SourceInfo source;
    private final MySqlSchema dbSchema;
    private final TopicSelector topicSelector;
    private final RecordMakers recordProcessor;
    private final Clock clock = Clock.system();
    private final JdbcConnection jdbc;

    public MySqlTaskContext(Configuration config) {
        this.config = config; // must be set before most methods are used

        // Set up the topic selector ...
        this.topicSelector = TopicSelector.defaultSelector(serverName());

        // Set up the source information ...
        this.source = new SourceInfo();
        this.source.setServerName(serverName());

        // Set up the MySQL schema ...
        this.dbSchema = new MySqlSchema(config);
        this.dbSchema.start();

        // Set up the record processor ...
        this.recordProcessor = new RecordMakers(dbSchema, source, topicSelector);

        // Set up the JDBC connection without actually connecting ...
        Configuration jdbcConfig = config.subset("database.", true);
        this.jdbc = new JdbcConnection(jdbcConfig, FACTORY);
    }

    public Configuration config() {
        return config;
    }

    public JdbcConnection jdbc() {
        return jdbc;
    }

    public TopicSelector topicSelector() {
        return topicSelector;
    }

    public Logger logger() {
        return logger;
    }

    public SourceInfo source() {
        return source;
    }

    public MySqlSchema dbSchema() {
        return dbSchema;
    }

    public RecordMakers makeRecord() {
        return recordProcessor;
    }

    /**
     * Load the database schema information using the previously-recorded history, and stop reading the history when the
     * the history reaches the supplied starting point.
     * 
     * @param startingPoint the source information with the current {@link SourceInfo#partition()} and {@link SourceInfo#offset()
     *            offset} at which the database schemas are to reflect; may not be null
     */
    public void loadHistory(SourceInfo startingPoint) {
        dbSchema.loadHistory(startingPoint);
        recordProcessor.regenerate();
    }
    
    public Clock clock() {
        return clock;
    }

    public long serverId() {
        return config.getLong(MySqlConnectorConfig.SERVER_ID);
    }

    public String serverName() {
        return config.getString(MySqlConnectorConfig.SERVER_NAME);
    }

    public String username() {
        return config.getString(MySqlConnectorConfig.USER);
    }

    public String password() {
        return config.getString(MySqlConnectorConfig.PASSWORD);
    }

    public String hostname() {
        return config.getString(MySqlConnectorConfig.HOSTNAME);
    }

    public int port() {
        return config.getInteger(MySqlConnectorConfig.PORT);
    }

    public int maxQueueSize() {
        return config.getInteger(MySqlConnectorConfig.MAX_QUEUE_SIZE);
    }

    public int maxBatchSize() {
        return config.getInteger(MySqlConnectorConfig.MAX_BATCH_SIZE);
    }

    public long timeoutInMilliseconds() {
        return config.getLong(MySqlConnectorConfig.CONNECTION_TIMEOUT_MS);
    }

    public long pollIntervalInMillseconds() {
        return config.getLong(MySqlConnectorConfig.POLL_INTERVAL_MS);
    }

    public boolean includeSchemaChangeRecords() {
        return config.getBoolean(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES);
    }

    public boolean isSnapshotAllowedWhenNeeded() {
        return snapshotMode() == SnapshotMode.WHEN_NEEDED;
    }

    public boolean isSnapshotNeverAllowed() {
        return snapshotMode() == SnapshotMode.NEVER;
    }

    protected SnapshotMode snapshotMode() {
        String value = config.getString(MySqlConnectorConfig.SNAPSHOT_MODE);
        return SnapshotMode.parse(value, MySqlConnectorConfig.SNAPSHOT_MODE.defaultValue());
    }
    
    public boolean useMinimalSnapshotLocking() {
        return config.getBoolean(MySqlConnectorConfig.SNAPSHOT_MINIMAL_LOCKING);
    }

    public void start() {
        // Start the MySQL database history, which simply starts up resources but does not recover the history to a specific
        // point.
        dbSchema().start();
    }

    public void shutdown() {
        try {
            // Flush and stop the database history ...
            logger.debug("Stopping database history for MySQL server '{}'", serverName());
            dbSchema.shutdown();
        } catch (Throwable e) {
            logger.error("Unexpected error shutting down the database history", e);
        } finally {
            try {
                jdbc.close();
            } catch (SQLException e) {
                logger.error("Unexpected error shutting down the database connection", e);
            }
        }
    }

}
