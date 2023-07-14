/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.legacy;

import java.util.Collections;
import java.util.Map;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.mysql.GtidSet;
import io.debezium.connector.mysql.Module;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlConnectorConfig.GtidNewChannelPosition;
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotMode;
import io.debezium.connector.mysql.MySqlSystemVariables;
import io.debezium.connector.mysql.MySqlTopicSelector;
import io.debezium.function.Predicates;
import io.debezium.relational.TableId;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.schema.TopicSelector;
import io.debezium.util.LoggingContext;
import io.debezium.util.Strings;

/**
 * A Kafka Connect source task reads the MySQL binary log and generate the corresponding data change events.
 *
 * @see MySqlConnector
 * @author Randall Hauch
 */
public final class MySqlTaskContext extends CdcSourceTaskContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlTaskContext.class);

    private final MySqlJdbcContext connectionContext;
    private final Configuration config;
    private final MySqlConnectorConfig connectorConfig;
    private final SourceInfo source;
    private final MySqlSchema dbSchema;
    private final TopicSelector<TableId> topicSelector;
    private final RecordMakers recordProcessor;
    private final Predicate<String> gtidSourceFilter;
    private final Predicate<String> ddlFilter;

    /**
     * Whether table ids are compared ignoring letter casing.
     */
    private final boolean tableIdCaseInsensitive;

    public MySqlTaskContext(Configuration config, Filters filters) {
        this(config, filters, null, null);
    }

    public MySqlTaskContext(Configuration config, Filters filters, Map<String, ?> restartOffset) {
        this(config, filters, null, restartOffset);
    }

    public MySqlTaskContext(Configuration config, Filters filters, Boolean tableIdCaseInsensitive, Map<String, ?> restartOffset) {
        super(Module.contextName(), config.getString(MySqlConnectorConfig.SERVER_NAME),
                new MySqlConnectorConfig(config).getCustomMetricTags(), Collections::emptyList);

        this.config = config;
        this.connectorConfig = new MySqlConnectorConfig(config);
        this.connectionContext = new MySqlJdbcContext(connectorConfig);

        // Set up the topic selector ...
        this.topicSelector = MySqlTopicSelector.defaultSelector(connectorConfig.getLogicalName(), connectorConfig.getHeartbeatTopicsPrefix());

        // Set up the source information ...
        this.source = new SourceInfo(connectorConfig);

        // Set up the GTID filter ...
        String gtidSetIncludes = config.getString(MySqlConnectorConfig.GTID_SOURCE_INCLUDES);
        String gtidSetExcludes = config.getString(MySqlConnectorConfig.GTID_SOURCE_EXCLUDES);
        this.gtidSourceFilter = gtidSetIncludes != null ? Predicates.includesUuids(gtidSetIncludes)
                : (gtidSetExcludes != null ? Predicates.excludesUuids(gtidSetExcludes) : null);

        if (tableIdCaseInsensitive == null) {
            this.tableIdCaseInsensitive = !"0".equals(connectionContext.readMySqlSystemVariables().get(MySqlSystemVariables.LOWER_CASE_TABLE_NAMES));
        }
        else {
            this.tableIdCaseInsensitive = tableIdCaseInsensitive;
        }

        // Set up the MySQL schema ...
        this.dbSchema = new MySqlSchema(connectorConfig, this.gtidSourceFilter, this.tableIdCaseInsensitive, topicSelector, filters);

        // Set up the record processor ...
        this.recordProcessor = new RecordMakers(dbSchema, source, topicSelector, connectorConfig.isEmitTombstoneOnDelete(), restartOffset);

        // Set up the DDL filter
        final String ddlFilter = config.getString(DatabaseHistory.DDL_FILTER);
        this.ddlFilter = (ddlFilter != null) ? Predicates.includes(ddlFilter) : (x -> false);
    }

    public Configuration config() {
        return config;
    }

    public MySqlConnectorConfig getConnectorConfig() {
        return this.connectorConfig;
    }

    public MySqlJdbcContext getConnectionContext() {
        return connectionContext;
    }

    public String connectorName() {
        return config.getString("name");
    }

    public TopicSelector<TableId> topicSelector() {
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
     * Get the predicate function that will return {@code true} if a GTID source is to be included, or {@code false} if
     * a GTID source is to be excluded.
     *
     * @return the GTID source predicate function; never null
     */
    public Predicate<String> gtidSourceFilter() {
        return gtidSourceFilter;
    }

    /**
     * Initialize the database history with any server-specific information. This should be done only upon connector startup
     * when the connector has no prior history.
     */
    public void initializeHistory() {
        // Read the system variables from the MySQL instance and get the current database name ...
        Map<String, String> variables = connectionContext.readMySqlCharsetSystemVariables();
        String ddlStatement = connectionContext.setStatementFor(variables);

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
        Map<String, String> variables = connectionContext.readMySqlCharsetSystemVariables();
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
            String ddlStatement = connectionContext.setStatementFor(variables);
            dbSchema.applyDdl(source, "", ddlStatement, null);
        }
        recordProcessor.regenerate();
    }

    /**
     * Return true if the database history entity exists
     */
    public boolean historyExists() {
        // Read the system variables from the MySQL instance and load them into the DDL parser as defaults ...
        Map<String, String> variables = connectionContext.readMySqlCharsetSystemVariables();
        dbSchema.setSystemVariables(variables);

        // And then load the history ...
        return dbSchema.historyExists();
    }

    /**
     * Initialize permanent storage for database history
     */
    public void initializeHistoryStorage() {
        dbSchema.intializeHistoryStorage();
    }

    public long serverId() {
        return config.getLong(MySqlConnectorConfig.SERVER_ID);
    }

    public long rowCountForLargeTable() {
        return config.getLong(MySqlConnectorConfig.ROW_COUNT_FOR_STREAMING_RESULT_SETS);
    }

    public int bufferSizeForBinlogReader() {
        return config.getInteger(MySqlConnectorConfig.BUFFER_SIZE_FOR_BINLOG_READER);
    }

    public boolean includeSchemaChangeRecords() {
        return config.getBoolean(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES);
    }

    public boolean includeSqlQuery() {
        return config.getBoolean(MySqlConnectorConfig.INCLUDE_SQL_QUERY);
    }

    public boolean isSnapshotAllowedWhenNeeded() {
        return snapshotMode() == SnapshotMode.WHEN_NEEDED;
    }

    public boolean isSnapshotNeverAllowed() {
        return snapshotMode() == SnapshotMode.NEVER;
    }

    public boolean isInitialSnapshotOnly() {
        return snapshotMode() == SnapshotMode.INITIAL_ONLY;
    }

    public boolean isSchemaOnlySnapshot() {
        return snapshotMode() == SnapshotMode.SCHEMA_ONLY;
    }

    public boolean isSchemaOnlyRecoverySnapshot() {
        return snapshotMode() == SnapshotMode.SCHEMA_ONLY_RECOVERY;
    }

    protected SnapshotMode snapshotMode() {
        String value = config.getString(MySqlConnectorConfig.SNAPSHOT_MODE);
        return SnapshotMode.parse(value, MySqlConnectorConfig.SNAPSHOT_MODE.defaultValueAsString());
    }

    public void start() {
        // Start the MySQL database history, which simply starts up resources but does not recover the history to a specific point
        dbSchema().start();
    }

    public void shutdown() {
        try {
            // Flush and stop the database history ...
            LOGGER.debug("Stopping database history");
            dbSchema.shutdown();
        }
        catch (Throwable e) {
            LOGGER.error("Unexpected error shutting down the database history", e);
        }
        finally {
            connectionContext.shutdown();
        }
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
        LoggingContext.temporarilyForConnector("MySQL", connectorConfig.getLogicalName(), contextName, operation);
    }

    /**
     * Apply the include/exclude GTID source filters to the current {@link #source() GTID set} and merge them onto the
     * currently available GTID set from a MySQL server.
     *
     * The merging behavior of this method might seem a bit strange at first. It's required in order for Debezium to consume a
     * MySQL binlog that has multi-source replication enabled, if a failover has to occur. In such a case, the server that
     * Debezium is failed over to might have a different set of sources, but still include the sources required for Debezium
     * to continue to function. MySQL does not allow downstream replicas to connect if the GTID set does not contain GTIDs for
     * all channels that the server is replicating from, even if the server does have the data needed by the client. To get
     * around this, we can have Debezium merge its GTID set with whatever is on the server, so that MySQL will allow it to
     * connect. See <a href="https://issues.jboss.org/browse/DBZ-143">DBZ-143</a> for details.
     *
     * This method does not mutate any state in the context.
     *
     * @param availableServerGtidSet the GTID set currently available in the MySQL server
     * @param purgedServerGtid the GTID set already purged by the MySQL server
     * @return A GTID set meant for consuming from a MySQL binlog; may return null if the SourceInfo has no GTIDs and therefore
     *         none were filtered
     */
    public GtidSet filterGtidSet(GtidSet availableServerGtidSet, GtidSet purgedServerGtid) {
        String gtidStr = source.gtidSet();
        if (gtidStr == null) {
            return null;
        }
        LOGGER.info("Attempting to generate a filtered GTID set");
        LOGGER.info("GTID set from previous recorded offset: {}", gtidStr);
        GtidSet filteredGtidSet = new GtidSet(gtidStr);
        Predicate<String> gtidSourceFilter = gtidSourceFilter();
        if (gtidSourceFilter != null) {
            filteredGtidSet = filteredGtidSet.retainAll(gtidSourceFilter);
            LOGGER.info("GTID set after applying GTID source includes/excludes to previous recorded offset: {}", filteredGtidSet);
        }
        LOGGER.info("GTID set available on server: {}", availableServerGtidSet);

        GtidSet mergedGtidSet;

        if (connectorConfig.gtidNewChannelPosition() == GtidNewChannelPosition.EARLIEST) {
            final GtidSet knownGtidSet = filteredGtidSet;
            LOGGER.info("Using first available positions for new GTID channels");
            final GtidSet relevantAvailableServerGtidSet = (gtidSourceFilter != null) ? availableServerGtidSet.retainAll(gtidSourceFilter) : availableServerGtidSet;
            LOGGER.info("Relevant GTID set available on server: {}", relevantAvailableServerGtidSet);

            mergedGtidSet = relevantAvailableServerGtidSet
                    .retainAll(uuid -> knownGtidSet.forServerWithId(uuid) != null)
                    .with(purgedServerGtid)
                    .with(filteredGtidSet);
        }
        else {
            mergedGtidSet = availableServerGtidSet.with(filteredGtidSet);
        }

        LOGGER.info("Final merged GTID set to use when connecting to MySQL: {}", mergedGtidSet);
        return mergedGtidSet;
    }

    /**
     * Get the predicate function that will return {@code true} if a DDL has to be skipped over and left out of the schema history
     * or {@code false} when it should be processed.
     *
     * @return the DDL predicate function; never null
     */
    public Predicate<String> ddlFilter() {
        return ddlFilter;
    }

    public boolean isTableIdCaseInsensitive() {
        return tableIdCaseInsensitive;
    }
}
