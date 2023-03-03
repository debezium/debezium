/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.mysql.MySqlConnection.MySqlConnectionConfiguration;
import io.debezium.connector.mysql.MySqlConnectorConfig.BigIntUnsignedHandlingMode;
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotMode;
import io.debezium.jdbc.DefaultMainConnectionFactory;
import io.debezium.jdbc.JdbcValueConverters.BigIntUnsignedMode;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

/**
 * The main task executing streaming from MySQL.
 * Responsible for lifecycle management of the streaming code.
 *
 * @author Jiri Pechanec
 *
 */
public class MySqlConnectorTask extends BaseSourceTask<MySqlPartition, MySqlOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlConnectorTask.class);
    private static final String CONTEXT_NAME = "mysql-connector-task";

    private volatile MySqlTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile MySqlConnection connection;
    private volatile ErrorHandler errorHandler;
    private volatile MySqlDatabaseSchema schema;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public ChangeEventSourceCoordinator<MySqlPartition, MySqlOffsetContext> start(Configuration configuration) {
        final Clock clock = Clock.system();
        final MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(configuration);
        final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(MySqlConnectorConfig.TOPIC_NAMING_STRATEGY);
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();
        final MySqlValueConverters valueConverters = getValueConverters(connectorConfig);

        // DBZ-3238: automatically set "useCursorFetch" to true when a snapshot fetch size other than the default of -1 is given
        // By default do not load whole result sets into memory
        final Configuration config = configuration.edit()
                .withDefault("database.responseBuffering", "adaptive")
                .withDefault("database.fetchSize", 10_000)
                .withDefault("database.useCursorFetch", connectorConfig.useCursorFetch())
                .build();

        MainConnectionProvidingConnectionFactory<MySqlConnection> connectionFactory = new DefaultMainConnectionFactory<>(() -> new MySqlConnection(new MySqlConnectionConfiguration(config),
                connectorConfig.useCursorFetch() ? new MySqlBinaryProtocolFieldReader(connectorConfig) : new MySqlTextProtocolFieldReader(connectorConfig)));

        connection = connectionFactory.getMainConnection();

        validateBinlogConfiguration(connectorConfig);

        Offsets<MySqlPartition, MySqlOffsetContext> previousOffsets = getPreviousOffsets(
                new MySqlPartition.Provider(connectorConfig, config),
                new MySqlOffsetContext.Loader(connectorConfig));

        final boolean tableIdCaseInsensitive = connection.isTableIdCaseSensitive();

        this.schema = new MySqlDatabaseSchema(connectorConfig, valueConverters, topicNamingStrategy, schemaNameAdjuster, tableIdCaseInsensitive);

        LOGGER.info("Closing connection before starting schema recovery");

        try {
            connection.close();
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }

        MySqlPartition partition = previousOffsets.getTheOnlyPartition();
        MySqlOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        validateAndLoadSchemaHistory(connectorConfig, partition, previousOffset, schema);

        LOGGER.info("Reconnecting after finishing schema recovery");

        try {
            connection.setAutoCommit(false);
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }

        // If the binlog position is not available it is necessary to reexecute snapshot
        if (validateSnapshotFeasibility(connectorConfig, previousOffset)) {
            previousOffsets.resetOffset(partition);
        }

        taskContext = new MySqlTaskContext(connectorConfig, schema);

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .buffering()
                .build();

        errorHandler = new MySqlErrorHandler(connectorConfig, queue);

        final MySqlEventMetadataProvider metadataProvider = new MySqlEventMetadataProvider();

        final Configuration heartbeatConfig = config;
        final EventDispatcher<MySqlPartition, TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicNamingStrategy,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                null,
                metadataProvider,
                connectorConfig.createHeartbeat(
                        topicNamingStrategy,
                        schemaNameAdjuster,
                        () -> new MySqlConnection(new MySqlConnectionConfiguration(heartbeatConfig), connectorConfig.useCursorFetch()
                                ? new MySqlBinaryProtocolFieldReader(connectorConfig)
                                : new MySqlTextProtocolFieldReader(connectorConfig)),
                        exception -> {
                            String sqlErrorId = exception.getSQLState();
                            switch (sqlErrorId) {
                                case "42000":
                                    // error_er_dbaccess_denied_error, see https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_dbaccess_denied_error
                                    throw new DebeziumException("Could not execute heartbeat action query (Error: " + sqlErrorId + ")", exception);
                                case "3D000":
                                    // error_er_no_db_error, see https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_no_db_error
                                    throw new DebeziumException("Could not execute heartbeat action query (Error: " + sqlErrorId + ")", exception);
                                default:
                                    break;
                            }
                        }),
                schemaNameAdjuster);

        final MySqlStreamingChangeEventSourceMetrics streamingMetrics = new MySqlStreamingChangeEventSourceMetrics(taskContext, queue, metadataProvider);

        ChangeEventSourceCoordinator<MySqlPartition, MySqlOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                previousOffsets,
                errorHandler,
                MySqlConnector.class,
                connectorConfig,
                new MySqlChangeEventSourceFactory(connectorConfig, connectionFactory, errorHandler, dispatcher, clock, schema, taskContext, streamingMetrics, queue),
                new MySqlChangeEventSourceMetricsFactory(streamingMetrics),
                dispatcher,
                schema);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    private MySqlValueConverters getValueConverters(MySqlConnectorConfig configuration) {
        // Use MySQL-specific converters and schemas for values ...

        TemporalPrecisionMode timePrecisionMode = configuration.getTemporalPrecisionMode();

        DecimalMode decimalMode = configuration.getDecimalMode();

        String bigIntUnsignedHandlingModeStr = configuration.getConfig().getString(MySqlConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE);
        BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode = BigIntUnsignedHandlingMode.parse(bigIntUnsignedHandlingModeStr);
        BigIntUnsignedMode bigIntUnsignedMode = bigIntUnsignedHandlingMode.asBigIntUnsignedMode();

        final boolean timeAdjusterEnabled = configuration.getConfig().getBoolean(MySqlConnectorConfig.ENABLE_TIME_ADJUSTER);
        return new MySqlValueConverters(decimalMode, timePrecisionMode, bigIntUnsignedMode,
                configuration.binaryHandlingMode(), timeAdjusterEnabled ? MySqlValueConverters::adjustTemporal : x -> x,
                MySqlValueConverters::defaultParsingErrorHandler);
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        final List<DataChangeEvent> records = queue.poll();

        final List<SourceRecord> sourceRecords = records.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());

        return sourceRecords;
    }

    @Override
    protected void doStop() {
        try {
            if (connection != null) {
                connection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC connection", e);
        }

        if (schema != null) {
            schema.close();
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return MySqlConnectorConfig.ALL_FIELDS;
    }

    private void validateBinlogConfiguration(MySqlConnectorConfig config) {
        if (config.getSnapshotMode().shouldStream()) {
            // Check whether the row-level binlog is enabled ...
            if (!connection.isBinlogFormatRow()) {
                throw new DebeziumException("The MySQL server is not configured to use a ROW binlog_format, which is "
                        + "required for this connector to work properly. Change the MySQL configuration to use a "
                        + "binlog_format=ROW and restart the connector.");
            }

            if (!connection.isBinlogRowImageFull()) {
                throw new DebeziumException("The MySQL server is not configured to use a FULL binlog_row_image, which is "
                        + "required for this connector to work properly. Change the MySQL configuration to use a "
                        + "binlog_row_image=FULL and restart the connector.");
            }
        }
    }

    /**
     * Determine whether the binlog position as set on the {@link MySqlOffsetContext} is available in the server.
     *
     * @return {@code true} if the server has the binlog coordinates, or {@code false} otherwise
     */
    protected boolean isBinlogAvailable(MySqlConnectorConfig config, MySqlOffsetContext offset) {
        String gtidStr = offset.gtidSet();
        if (gtidStr != null) {
            if (gtidStr.trim().isEmpty()) {
                return true; // start at beginning ...
            }
            String availableGtidStr = connection.knownGtidSet();
            if (availableGtidStr == null || availableGtidStr.trim().isEmpty()) {
                // Last offsets had GTIDs but the server does not use them ...
                LOGGER.info("Connector used GTIDs previously, but MySQL does not know of any GTIDs or they are not enabled");
                return false;
            }
            // GTIDs are enabled, and we used them previously, but retain only those GTID ranges for the allowed source UUIDs ...
            GtidSet gtidSet = new GtidSet(gtidStr).retainAll(config.gtidSourceFilter());
            // Get the GTID set that is available in the server ...
            GtidSet availableGtidSet = new GtidSet(availableGtidStr);
            if (gtidSet.isContainedWithin(availableGtidSet)) {
                LOGGER.info("MySQL current GTID set {} does contain the GTID set required by the connector {}", availableGtidSet, gtidSet);
                final GtidSet knownServerSet = availableGtidSet.retainAll(config.gtidSourceFilter());
                final GtidSet gtidSetToReplicate = connection.subtractGtidSet(knownServerSet, gtidSet);
                final GtidSet purgedGtidSet = connection.purgedGtidSet();
                LOGGER.info("Server has already purged {} GTIDs", purgedGtidSet);
                final GtidSet nonPurgedGtidSetToReplicate = connection.subtractGtidSet(gtidSetToReplicate, purgedGtidSet);
                LOGGER.info("GTIDs known by the server but not processed yet {}, for replication are available only {}", gtidSetToReplicate, nonPurgedGtidSetToReplicate);
                if (!gtidSetToReplicate.equals(nonPurgedGtidSetToReplicate)) {
                    LOGGER.info("Some of the GTIDs needed to replicate have been already purged");
                    return false;
                }
                return true;
            }
            LOGGER.info("Connector last known GTIDs are {}, but MySQL has {}", gtidSet, availableGtidSet);
            return false;
        }

        String binlogFilename = offset.getSource().binlogFilename();
        if (binlogFilename == null) {
            return true; // start at current position
        }
        if (binlogFilename.equals("")) {
            return true; // start at beginning
        }

        // Accumulate the available binlog filenames ...
        List<String> logNames = connection.availableBinlogFiles();

        // And compare with the one we're supposed to use ...
        boolean found = logNames.stream().anyMatch(binlogFilename::equals);
        if (!found) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Connector requires binlog file '{}', but MySQL only has {}", binlogFilename, String.join(", ", logNames));
            }
        }
        else {
            LOGGER.info("MySQL has the binlog file '{}' required by the connector", binlogFilename);
        }

        return found;
    }

    private boolean validateAndLoadSchemaHistory(MySqlConnectorConfig config, MySqlPartition partition, MySqlOffsetContext offset, MySqlDatabaseSchema schema) {
        if (offset == null) {
            if (config.getSnapshotMode().shouldSnapshotOnSchemaError()) {
                // We are in schema only recovery mode, use the existing binlog position
                // would like to also verify binlog position exists, but it defaults to 0 which is technically valid
                throw new DebeziumException("Could not find existing binlog information while attempting schema only recovery snapshot");
            }
            LOGGER.info("Connector started for the first time, database schema history recovery will not be executed");
            schema.initializeStorage();
            return false;
        }
        if (!schema.historyExists()) {
            LOGGER.warn("Database schema history was not found but was expected");
            if (config.getSnapshotMode().shouldSnapshotOnSchemaError()) {
                // But check to see if the server still has those binlog coordinates ...
                if (!isBinlogAvailable(config, offset)) {
                    throw new DebeziumException("The connector is trying to read binlog starting at " + offset.getSource() + ", but this is no longer "
                            + "available on the server. Reconfigure the connector to use a snapshot when needed.");
                }
                LOGGER.info("The db-history topic is missing but we are in {} snapshot mode. " +
                        "Attempting to snapshot the current schema and then begin reading the binlog from the last recorded offset.",
                        SnapshotMode.SCHEMA_ONLY_RECOVERY);
            }
            else {
                throw new DebeziumException("The db history topic is missing. You may attempt to recover it by reconfiguring the connector to "
                        + SnapshotMode.SCHEMA_ONLY_RECOVERY);
            }
            schema.initializeStorage();
            return true;
        }
        schema.recover(partition, offset);
        return false;
    }

    private boolean validateSnapshotFeasibility(MySqlConnectorConfig config, MySqlOffsetContext offset) {
        if (offset != null) {
            if (offset.isSnapshotRunning()) {
                // The last offset was an incomplete snapshot and now the snapshot was disabled
                if (!config.getSnapshotMode().shouldSnapshot()) {
                    // No snapshots are allowed
                    throw new DebeziumException("The connector previously stopped while taking a snapshot, but now the connector is configured "
                            + "to never allow snapshots. Reconfigure the connector to use snapshots initially or when needed.");
                }
            }
            else {
                // But check to see if the server still has those binlog coordinates ...
                if (!isBinlogAvailable(config, offset)) {
                    if (!config.getSnapshotMode().shouldSnapshotOnDataError()) {
                        throw new DebeziumException("The connector is trying to read binlog starting at " + offset.getSource() + ", but this is no longer "
                                + "available on the server. Reconfigure the connector to use a snapshot when needed.");
                    }
                    else {
                        LOGGER.warn(
                                "The connector is trying to read binlog starting at '{}', but this is no longer available on the server. Forcing the snapshot execution as it is allowed by the configuration.",
                                offset.getSource());
                        return true;
                    }
                }
            }
        }
        else {
            if (!config.getSnapshotMode().shouldSnapshot()) {
                // Look to see what the first available binlog file is called, and whether it looks like binlog files have
                // been purged. If so, then output a warning ...
                String earliestBinlogFilename = connection.earliestBinlogFilename();
                if (earliestBinlogFilename == null) {
                    LOGGER.warn("No binlog appears to be available. Ensure that the MySQL row-level binlog is enabled.");
                }
                else if (!earliestBinlogFilename.endsWith("00001")) {
                    LOGGER.warn("It is possible the server has purged some binlogs. If this is the case, then using snapshot mode may be required.");
                }
            }
        }
        return false;
    }
}
