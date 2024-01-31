/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.bean.StandardBeanNames;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.mysql.MySqlConnectorConfig.BigIntUnsignedHandlingMode;
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotMode;
import io.debezium.connector.mysql.snapshot.MySqlSnapshotLockProvider;
import io.debezium.connector.mysql.snapshot.MySqlSnapshotQueryProvider;
import io.debezium.connector.mysql.snapshot.MySqlSnapshotterServiceProvider;
import io.debezium.connector.mysql.strategy.AbstractConnectorConnection;
import io.debezium.connector.mysql.strategy.ConnectorAdapter;
import io.debezium.connector.mysql.strategy.mysql.MySqlConnection;
import io.debezium.connector.mysql.strategy.mysql.MySqlConnectionConfiguration;
import io.debezium.document.DocumentReader;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.JdbcValueConverters.BigIntUnsignedMode;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.signal.channels.KafkaSignalChannel;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.snapshot.Snapshotter;
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
    private volatile AbstractConnectorConnection connection;
    private volatile AbstractConnectorConnection beanRegistryJdbcConnection;
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

        final ConnectorAdapter adapter = connectorConfig.getConnectorAdapter();

        MainConnectionProvidingConnectionFactory<AbstractConnectorConnection> connectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(
                () -> adapter.createConnection(config));

        connection = connectionFactory.mainConnection();

        Offsets<MySqlPartition, MySqlOffsetContext> previousOffsets = getPreviousOffsets(
                new MySqlPartition.Provider(connectorConfig, config),
                new MySqlOffsetContext.Loader(connectorConfig));

        final boolean tableIdCaseInsensitive = connection.isTableIdCaseSensitive();

        this.schema = new MySqlDatabaseSchema(connectorConfig, valueConverters, topicNamingStrategy, schemaNameAdjuster, tableIdCaseInsensitive);

        // Manual Bean Registration
        beanRegistryJdbcConnection = connectionFactory.newConnection();
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, config);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.DATABASE_SCHEMA, schema);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.JDBC_CONNECTION, beanRegistryJdbcConnection);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.VALUE_CONVERTER, valueConverters);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.OFFSETS, previousOffsets);

        // Service providers
        registerServiceProviders(connectorConfig.getServiceRegistry());

        final SnapshotterService snapshotterService = connectorConfig.getServiceRegistry().tryGetService(SnapshotterService.class);
        final Snapshotter snapshotter = snapshotterService.getSnapshotter();

        validateBinlogConfiguration(snapshotter);

        LOGGER.info("Closing connection before starting schema recovery");

        try {
            connection.close();
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }

        MySqlOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        validateAndLoadSchemaHistory(connectorConfig, previousOffsets, schema, snapshotter);

        LOGGER.info("Reconnecting after finishing schema recovery");

        try {
            connection.setAutoCommit(false);
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }

        // If the binlog position is not available it is necessary to re-execute snapshot
        if (previousOffset == null) {
            LOGGER.info("No previous offset found");
            // if we have no initial offset, indicate that to Snapshotter by passing null
            snapshotter.validate(false, false);
        }
        else {
            LOGGER.info("Found previous offset {}", previousOffset);
            snapshotter.validate(true, previousOffset.isSnapshotRunning());
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

        errorHandler = new MySqlErrorHandler(connectorConfig, queue, errorHandler);

        final MySqlEventMetadataProvider metadataProvider = new MySqlEventMetadataProvider();

        SignalProcessor<MySqlPartition, MySqlOffsetContext> signalProcessor = new SignalProcessor<>(
                MySqlConnector.class, connectorConfig, Map.of(),
                getAvailableSignalChannels(),
                DocumentReader.defaultReader(),
                previousOffsets);
        resetOffset(connectorConfig, previousOffset, signalProcessor);

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
                        () -> new MySqlConnection(new MySqlConnectionConfiguration(heartbeatConfig),
                                getFieldReader(connectorConfig)),
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
                schemaNameAdjuster,
                signalProcessor);

        final MySqlStreamingChangeEventSourceMetrics streamingMetrics = new MySqlStreamingChangeEventSourceMetrics(taskContext, queue, metadataProvider);

        NotificationService<MySqlPartition, MySqlOffsetContext> notificationService = new NotificationService<>(getNotificationChannels(),
                connectorConfig, SchemaFactory.get(), dispatcher::enqueueNotification);

        ChangeEventSourceCoordinator<MySqlPartition, MySqlOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                previousOffsets,
                errorHandler,
                MySqlConnector.class,
                connectorConfig,
                new MySqlChangeEventSourceFactory(
                        connectorConfig,
                        connectionFactory,
                        errorHandler,
                        dispatcher,
                        clock,
                        schema,
                        taskContext,
                        streamingMetrics,
                        queue,
                        snapshotterService),
                new MySqlChangeEventSourceMetricsFactory(streamingMetrics),
                dispatcher,
                schema,
                signalProcessor,
                notificationService,
                snapshotterService);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    @Override
    protected void registerServiceProviders(ServiceRegistry serviceRegistry) {

        super.registerServiceProviders(serviceRegistry);
        serviceRegistry.registerServiceProvider(new MySqlSnapshotLockProvider());
        serviceRegistry.registerServiceProvider(new MySqlSnapshotQueryProvider());
        serviceRegistry.registerServiceProvider(new MySqlSnapshotterServiceProvider());
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
                configuration.getConnectorAdapter(), configuration.getEventConvertingFailureHandlingMode());
    }

    private MySqlFieldReader getFieldReader(MySqlConnectorConfig configuration) {
        if (configuration.usesMariaDbProtocol()) {
            return new MariaDbProtocolFieldReader(configuration);
        }
        else if (configuration.useCursorFetch()) {
            return new MySqlBinaryProtocolFieldReader(configuration);
        }
        else {
            return new MySqlTextProtocolFieldReader(configuration);
        }
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

        try {
            if (beanRegistryJdbcConnection != null) {
                beanRegistryJdbcConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC bean registry connection", e);
        }

        if (schema != null) {
            schema.close();
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return MySqlConnectorConfig.ALL_FIELDS;
    }

    private void validateBinlogConfiguration(Snapshotter snapshotter) {

        if (snapshotter.shouldStream()) {
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

    private void validateAndLoadSchemaHistory(MySqlConnectorConfig config, Offsets<MySqlPartition, MySqlOffsetContext> previousoffsets, MySqlDatabaseSchema schema,
                                              Snapshotter snapshotter) {

        final MySqlOffsetContext offset = previousoffsets.getTheOnlyOffset();
        final MySqlPartition partition = previousoffsets.getTheOnlyPartition();
        if (offset == null) {
            if (snapshotter.shouldSnapshotOnSchemaError()) {
                // We are in schema only recovery mode, use the existing binlog position
                // would like to also verify binlog position exists, but it defaults to 0 which is technically valid
                throw new DebeziumException("Could not find existing binlog information while attempting schema only recovery snapshot");
            }
            LOGGER.info("Connector started for the first time, database schema history recovery will not be executed");
            schema.initializeStorage();
            return;
        }
        if (!schema.historyExists()) {
            LOGGER.warn("Database schema history was not found but was expected");
            if (snapshotter.shouldSnapshotOnSchemaError()) {
                // But check to see if the server still has those binlog coordinates ...
                if (!connection.isBinlogPositionAvailable(config, offset.gtidSet(), offset.getSource().binlogFilename())) {
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
            return;
        }
        schema.recover(partition, offset);
    }

    private void resetOffset(MySqlConnectorConfig connectorConfig, MySqlOffsetContext previousOffset,
                             SignalProcessor<MySqlPartition, MySqlOffsetContext> signalProcessor) {
        boolean isKafkaChannelEnabled = connectorConfig.getEnabledChannels().contains(KafkaSignalChannel.CHANNEL_NAME);
        if (previousOffset != null && isKafkaChannelEnabled && connectorConfig.isReadOnlyConnection()) {
            KafkaSignalChannel kafkaSignal = signalProcessor.getSignalChannel(KafkaSignalChannel.class);
            Long signalOffset = connectorConfig.getConnectorAdapter().getReadOnlyIncrementalSnapshotSignalOffset(previousOffset);
            if (signalOffset != null) {
                LOGGER.info("Resetting Kafka Signal offset to {}", signalOffset);
                kafkaSignal.reset(signalOffset);
            }
        }
    }
}
