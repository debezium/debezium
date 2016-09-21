/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.Map;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotMode;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.LoggingContext.PreviousContext;
import io.debezium.util.Strings;

/**
 * A Kafka Connect source task reads the MySQL binary log and generate the corresponding data change events.
 * 
 * @see MySqlConnector
 * @author Randall Hauch
 */
public final class MySqlTaskContext extends MySqlJdbcContext {

    private final SourceInfo source;
    private final MySqlSchema dbSchema;
    private final TopicSelector topicSelector;
    private final RecordMakers recordProcessor;
    private final Clock clock = Clock.system();

    public MySqlTaskContext(Configuration config) {
        super(config);

        // Set up the topic selector ...
        this.topicSelector = TopicSelector.defaultSelector(serverName());

        // Set up the source information ...
        this.source = new SourceInfo();
        this.source.setServerName(serverName());

        // Set up the MySQL schema ...
        this.dbSchema = new MySqlSchema(config, serverName());

        // Set up the record processor ...
        this.recordProcessor = new RecordMakers(dbSchema, source, topicSelector);
    }

    public TopicSelector topicSelector() {
        return topicSelector;
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
     * Initialize the database history with any server-specific information. This should be done only upon connector startup
     * when the connector has no prior history.
     */
    public void initializeHistory() {
        // Read the system variables from the MySQL instance and get the current database name ...
        Map<String, String> variables = readMySqlCharsetSystemVariables(null);
        String ddlStatement = setStatementFor(variables);

        // And write them into the database history ...
        dbSchema.applyDdl(source, "", ddlStatement, null);
    }

    /**
     * Load the database schema information using the previously-recorded history, and stop reading the history when the
     * the history reaches the supplied starting point.
     * 
     * @param startingPoint the source information with the current {@link SourceInfo#partition()} and {@link SourceInfo#offset()
     *            offset} at which the database schemas are to reflect; may not be null
     */
    public void loadHistory(SourceInfo startingPoint) {
        // Read the system variables from the MySQL instance and load them into the DDL parser as defaults ...
        Map<String, String> variables = readMySqlCharsetSystemVariables(null);
        dbSchema.setSystemVariables(variables);

        // And then load the history ...
        dbSchema.loadHistory(startingPoint);

        // The server's default character set may have changed since we last recorded it in the history,
        // so we need to see if the history's state does not match ...
        String systemCharsetName = variables.get(MySqlSystemVariables.CHARSET_NAME_SERVER);
        String systemCharsetNameFromHistory = dbSchema.systemVariables().getVariable(MySqlSystemVariables.CHARSET_NAME_SERVER);
        if (!Strings.equalsIgnoreCase(systemCharsetName, systemCharsetNameFromHistory)) {
            // The history's server character set is NOT the same as the server's current default,
            // so record the change in the history ...
            String ddlStatement = setStatementFor(variables);
            dbSchema.applyDdl(source, "", ddlStatement, null);
        }
        recordProcessor.regenerate();
    }

    public Clock clock() {
        return clock;
    }

    public long serverId() {
        return config.getLong(MySqlConnectorConfig.SERVER_ID);
    }

    public String serverName() {
        String serverName = config.getString(MySqlConnectorConfig.SERVER_NAME);
        if (serverName == null) {
            serverName = hostname() + ":" + port();
        }
        return serverName;
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

    public long rowCountForLargeTable() {
        return config.getLong(MySqlConnectorConfig.ROW_COUNT_FOR_STREAMING_RESULT_SETS);
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
        return SnapshotMode.parse(value, MySqlConnectorConfig.SNAPSHOT_MODE.defaultValueAsString());
    }

    public boolean useMinimalSnapshotLocking() {
        return config.getBoolean(MySqlConnectorConfig.SNAPSHOT_MINIMAL_LOCKING);
    }

    @Override
    public void start() {
        super.start();
        // Start the MySQL database history, which simply starts up resources but does not recover the history to a specific point
        dbSchema().start();
    }

    @Override
    public void shutdown() {
        try {
            // Flush and stop the database history ...
            logger.debug("Stopping database history");
            dbSchema.shutdown();
        } catch (Throwable e) {
            logger.error("Unexpected error shutting down the database history", e);
        } finally {
            super.shutdown();
        }
    }

    /**
     * Configure the logger's Mapped Diagnostic Context (MDC) properties for the thread making this call.
     * 
     * @param contextName the name of the context; may not be null
     * @return the previous MDC context; never null
     * @throws IllegalArgumentException if {@code contextName} is null
     */
    public PreviousContext configureLoggingContext(String contextName) {
        return LoggingContext.forConnector("MySQL", serverName(), contextName);
    }

    /**
     * Run the supplied function in the temporary connector MDC context, and when complete always return the MDC context to its
     * state before this method was called.
     * 
     * @param contextName the name of the context; may not be null
     * @param operation the function to run in the new MDC context; may not be null
     * @throws IllegalArgumentException if any of the parameters are null
     */
    public void temporaryLoggingContext(String contextName, Runnable operation) {
        LoggingContext.temporarilyForConnector("MySQL", serverName(), contextName, operation);
    }

}
