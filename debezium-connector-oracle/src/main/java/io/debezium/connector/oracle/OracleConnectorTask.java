/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.oracle.StreamingAdapter.TableNameCaseSensitivity;
import io.debezium.document.DocumentReader;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

public class OracleConnectorTask extends BaseSourceTask<OraclePartition, OracleOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleConnectorTask.class);
    private static final String CONTEXT_NAME = "oracle-connector-task";

    private volatile OracleTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile OracleConnection jdbcConnection;
    private volatile ErrorHandler errorHandler;
    private volatile OracleDatabaseSchema schema;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public ChangeEventSourceCoordinator<OraclePartition, OracleOffsetContext> start(Configuration config) {
        OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY);
        SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();

        JdbcConfiguration jdbcConfig = connectorConfig.getJdbcConfig();
        MainConnectionProvidingConnectionFactory<OracleConnection> connectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(
                () -> new OracleConnection(jdbcConfig));
        jdbcConnection = connectionFactory.mainConnection();

        validateRedoLogConfiguration(connectorConfig);

        OracleValueConverters valueConverters = connectorConfig.getAdapter().getValueConverter(connectorConfig, jdbcConnection);
        OracleDefaultValueConverter defaultValueConverter = new OracleDefaultValueConverter(valueConverters, jdbcConnection);
        TableNameCaseSensitivity tableNameCaseSensitivity = connectorConfig.getAdapter().getTableNameCaseSensitivity(jdbcConnection);
        this.schema = new OracleDatabaseSchema(connectorConfig, valueConverters, defaultValueConverter, schemaNameAdjuster,
                topicNamingStrategy, tableNameCaseSensitivity);

        Offsets<OraclePartition, OracleOffsetContext> previousOffsets = getPreviousOffsets(new OraclePartition.Provider(connectorConfig),
                connectorConfig.getAdapter().getOffsetContextLoader());

        OraclePartition partition = previousOffsets.getTheOnlyPartition();
        OracleOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        validateAndLoadSchemaHistory(connectorConfig, partition, previousOffset, schema);

        taskContext = new OracleTaskContext(connectorConfig, schema);

        Clock clock = Clock.system();

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        errorHandler = new OracleErrorHandler(connectorConfig, queue, errorHandler);

        final OracleEventMetadataProvider metadataProvider = new OracleEventMetadataProvider();

        SignalProcessor<OraclePartition, OracleOffsetContext> signalProcessor = new SignalProcessor<>(
                OracleConnector.class, connectorConfig, Map.of(),
                getAvailableSignalChannels(),
                DocumentReader.defaultReader(),
                previousOffsets);

        EventDispatcher<OraclePartition, TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicNamingStrategy,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                metadataProvider,
                connectorConfig.createHeartbeat(
                        topicNamingStrategy,
                        schemaNameAdjuster,
                        () -> getHeartbeatConnection(connectorConfig, jdbcConfig),
                        exception -> {
                            final String sqlErrorId = exception.getMessage();
                            throw new DebeziumException("Could not execute heartbeat action query (Error: " + sqlErrorId + ")", exception);
                        }),
                schemaNameAdjuster,
                signalProcessor);

        final OracleStreamingChangeEventSourceMetrics streamingMetrics = new OracleStreamingChangeEventSourceMetrics(taskContext, queue, metadataProvider,
                connectorConfig);

        NotificationService<OraclePartition, OracleOffsetContext> notificationService = new NotificationService<>(getNotificationChannels(),
                connectorConfig, SchemaFactory.get(), dispatcher::enqueueNotification);

        ChangeEventSourceCoordinator<OraclePartition, OracleOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                previousOffsets,
                errorHandler,
                OracleConnector.class,
                connectorConfig,
                new OracleChangeEventSourceFactory(connectorConfig, connectionFactory, errorHandler, dispatcher, clock, schema, jdbcConfig, taskContext,
                        streamingMetrics),
                new OracleChangeEventSourceMetricsFactory(streamingMetrics),
                dispatcher,
                schema, signalProcessor,
                notificationService);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    private OracleConnection getHeartbeatConnection(OracleConnectorConfig connectorConfig, JdbcConfiguration jdbcConfig) {
        final OracleConnection connection = new OracleConnection(jdbcConfig);
        if (!Strings.isNullOrBlank(connectorConfig.getPdbName())) {
            connection.setSessionToPdb(connectorConfig.getPdbName());
        }
        return connection;
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        List<DataChangeEvent> records = queue.poll();

        List<SourceRecord> sourceRecords = records.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());

        return sourceRecords;
    }

    @Override
    public void doStop() {
        try {
            if (jdbcConnection != null) {
                jdbcConnection.close();
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
        return OracleConnectorConfig.ALL_FIELDS;
    }

    private void validateRedoLogConfiguration(OracleConnectorConfig config) {
        // Check whether the archive log is enabled.
        final boolean archivelogMode = jdbcConnection.isArchiveLogMode();
        if (!archivelogMode) {
            if (redoLogRequired(config)) {
                throw new DebeziumException("The Oracle server is not configured to use a archive log LOG_MODE, which is "
                        + "required for this connector to work properly. Change the Oracle configuration to use a "
                        + "LOG_MODE=ARCHIVELOG and restart the connector.");
            }
            else {
                LOGGER.warn("Failed the archive log check but continuing as redo log isn't strictly required");
            }
        }
    }

    private static boolean redoLogRequired(OracleConnectorConfig config) {
        // Check whether our connector configuration relies on the redo log and should fail fast if it isn't configured
        return config.getSnapshotMode().shouldStream() ||
                config.getLogMiningTransactionSnapshotBoundaryMode() == OracleConnectorConfig.TransactionSnapshotBoundaryMode.ALL;
    }

    private void validateAndLoadSchemaHistory(OracleConnectorConfig config, OraclePartition partition, OracleOffsetContext offset, OracleDatabaseSchema schema) {
        if (offset == null) {
            if (config.getSnapshotMode().shouldSnapshotOnSchemaError() && config.getSnapshotMode() != OracleConnectorConfig.SnapshotMode.ALWAYS) {
                // We are in schema only recovery mode, use the existing redo log position
                // would like to also verify redo log position exists, but it defaults to 0 which is technically valid
                throw new DebeziumException("Could not find existing redo log information while attempting schema only recovery snapshot");
            }
            LOGGER.info("Connector started for the first time, database schema history recovery will not be executed");
            schema.initializeStorage();
            return;
        }
        if (!schema.historyExists()) {
            LOGGER.warn("Database schema history was not found but was expected");
            if (config.getSnapshotMode().shouldSnapshotOnSchemaError()) {
                LOGGER.info("The db-history topic is missing but we are in {} snapshot mode. " +
                        "Attempting to snapshot the current schema and then begin reading the redo log from the last recorded offset.",
                        OracleConnectorConfig.SnapshotMode.SCHEMA_ONLY_RECOVERY);
            }
            else {
                throw new DebeziumException("The db history topic is missing. You may attempt to recover it by reconfiguring the connector to "
                        + OracleConnectorConfig.SnapshotMode.SCHEMA_ONLY_RECOVERY);
            }
            schema.initializeStorage();
            return;
        }
        schema.recover(Offsets.of(partition, offset));
    }
}
