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
import io.debezium.bean.StandardBeanNames;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.oracle.StreamingAdapter.TableNameCaseSensitivity;
import io.debezium.document.DocumentReader;
import io.debezium.jdbc.JdbcConfiguration;
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
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

public class OracleConnectorTask extends BaseSourceTask<OraclePartition, OracleOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleConnectorTask.class);
    private static final String CONTEXT_NAME = "oracle-connector-task";

    private volatile OracleTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile OracleConnection jdbcConnection;
    private volatile OracleConnection beanRegistryJdbcConnection;
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
        Configuration readonlyConfig = connectorConfig.isLogMiningReadOnly() ? buildReadonlyConfig(jdbcConfig.asMap(), connectorConfig) : null;
        OracleConnection mainConnection = new OracleConnection(jdbcConfig);
        DualOracleConnectionFactory<OracleConnection> dualConnectionFactory = new DualOracleConnectionFactory<>(
                () -> mainConnection,
                () -> readonlyConfig != null ? new ReadOnlyOracleConnection(JdbcConfiguration.adapt(readonlyConfig)) : mainConnection,
                connectorConfig);
        jdbcConnection = dualConnectionFactory.mainConnection();

        final boolean extendedStringsSupported = jdbcConnection.hasExtendedStringSupport();

        OracleValueConverters valueConverters = connectorConfig.getAdapter().getValueConverter(connectorConfig, jdbcConnection);
        OracleDefaultValueConverter defaultValueConverter = new OracleDefaultValueConverter(valueConverters, jdbcConnection);
        TableNameCaseSensitivity tableNameCaseSensitivity = connectorConfig.getAdapter().getTableNameCaseSensitivity(jdbcConnection);
        this.schema = new OracleDatabaseSchema(connectorConfig, valueConverters, defaultValueConverter, schemaNameAdjuster,
                topicNamingStrategy, tableNameCaseSensitivity, extendedStringsSupported);

        Offsets<OraclePartition, OracleOffsetContext> previousOffsets = getPreviousOffsets(new OraclePartition.Provider(connectorConfig),
                connectorConfig.getAdapter().getOffsetContextLoader());

        // The bean registry JDBC connection should always be pinned to the PDB
        // when the connector is configured to use a pluggable database
        beanRegistryJdbcConnection = dualConnectionFactory.newReadonlyConnection();
        if (!Strings.isNullOrEmpty(connectorConfig.getPdbName())) {
            beanRegistryJdbcConnection.setSessionToPdb(connectorConfig.getPdbName());
        }

        // Manual Bean Registration
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, config);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.DATABASE_SCHEMA, schema);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.JDBC_CONNECTION, beanRegistryJdbcConnection);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.VALUE_CONVERTER, valueConverters);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.OFFSETS, previousOffsets);

        // Service providers
        registerServiceProviders(connectorConfig.getServiceRegistry());

        final SnapshotterService snapshotterService = connectorConfig.getServiceRegistry().tryGetService(SnapshotterService.class);

        validateRedoLogConfiguration(connectorConfig, snapshotterService);

        checkArchiveLogDestination(jdbcConnection, connectorConfig.getArchiveLogDestinationName());

        OracleOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        validateAndLoadSchemaHistory(connectorConfig, jdbcConnection::validateLogPosition, previousOffsets, schema, snapshotterService.getSnapshotter());

        taskContext = new OracleTaskContext(connectorConfig, schema);

        // If the redo log position is not available it is necessary to re-execute snapshot
        if (previousOffset == null) {
            LOGGER.info("No previous offset found");
        } else {
            LOGGER.info("Found previous offset {}", previousOffset);
        }

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

        final AbstractOracleStreamingChangeEventSourceMetrics streamingMetrics = connectorConfig.getAdapter()
                .getStreamingMetrics(taskContext, queue, metadataProvider, connectorConfig);

        NotificationService<OraclePartition, OracleOffsetContext> notificationService = new NotificationService<>(getNotificationChannels(),
                connectorConfig, SchemaFactory.get(), dispatcher::enqueueNotification);

        ChangeEventSourceCoordinator<OraclePartition, OracleOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                previousOffsets,
                errorHandler,
                OracleConnector.class,
                connectorConfig,
                new OracleChangeEventSourceFactory(connectorConfig, dualConnectionFactory, errorHandler, dispatcher, clock, schema, jdbcConfig, taskContext,
                        streamingMetrics, snapshotterService),
                new OracleChangeEventSourceMetricsFactory(streamingMetrics),
                dispatcher,
                schema, signalProcessor,
                notificationService, snapshotterService);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    private Configuration buildReadonlyConfig(Map<String, String> config, OracleConnectorConfig connectorConfig) {
        config.put("hostname", connectorConfig.getReadonlyHostname());
        Configuration.Builder builder = Configuration.create();
        config.forEach(
                (k, v) -> builder.with(k.toString(), v));
        return builder.build();
    }

    private void checkArchiveLogDestination(OracleConnection connection, String destinationName) {
        try {

            if (!Strings.isNullOrBlank(destinationName)) {
                if (!connection.isArchiveLogDestinationValid(destinationName)) {
                    LOGGER.warn("Archive log destination '{}' may not be valid, please check the database.", destinationName);
                }
            } else {
                if (!connection.isOnlyOneArchiveLogDestinationValid()) {
                    LOGGER.warn("There are multiple valid archive log destinations. " +
                                    "Please add '{}' to the connector configuration to avoid log availability problems.",
                            OracleConnectorConfig.ARCHIVE_DESTINATION_NAME.name());
                }
            }
        } catch (SQLException e) {
            throw new DebeziumException("Error while checking validity of archive log configuration", e);
        }
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

        return records.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());
    }

    @Override
    public void doStop() {
        try {
            if (jdbcConnection != null) {
                jdbcConnection.close();
            }
        } catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC connection", e);
        }

        try {
            if (beanRegistryJdbcConnection != null) {
                beanRegistryJdbcConnection.close();
            }
        } catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC bean registry connection", e);
        }

        if (schema != null) {
            schema.close();
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return OracleConnectorConfig.ALL_FIELDS;
    }

    private void validateRedoLogConfiguration(OracleConnectorConfig config, SnapshotterService snapshotterService) {
        // Check whether the archive log is enabled.
        final boolean archivelogMode = jdbcConnection.isArchiveLogMode();
        if (!archivelogMode) {
            if (redoLogRequired(config, snapshotterService)) {
                throw new DebeziumException("The Oracle server is not configured to use a archive log LOG_MODE, which is "
                        + "required for this connector to work properly. Change the Oracle configuration to use a "
                        + "LOG_MODE=ARCHIVELOG and restart the connector.");
            } else {
                LOGGER.warn("Failed the archive log check but continuing as redo log isn't strictly required");
            }
        }
    }

    private static boolean redoLogRequired(OracleConnectorConfig config, SnapshotterService snapshotterService) {
        // Check whether our connector configuration relies on the redo log and should fail fast if it isn't configured
        return snapshotterService.getSnapshotter().shouldStream() ||
                config.getLogMiningTransactionSnapshotBoundaryMode() == OracleConnectorConfig.TransactionSnapshotBoundaryMode.ALL;
    }

}
