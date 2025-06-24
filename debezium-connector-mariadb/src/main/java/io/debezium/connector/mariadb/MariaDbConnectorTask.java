/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static io.debezium.connector.binlog.BinlogConnectorConfig.TOPIC_NAMING_STRATEGY;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.bean.StandardBeanNames;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.binlog.BinlogEventMetadataProvider;
import io.debezium.connector.binlog.BinlogSourceTask;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.binlog.jdbc.BinlogFieldReader;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.connector.mariadb.jdbc.MariaDbConnection;
import io.debezium.connector.mariadb.jdbc.MariaDbConnectionConfiguration;
import io.debezium.connector.mariadb.jdbc.MariaDbFieldReader;
import io.debezium.connector.mariadb.jdbc.MariaDbValueConverters;
import io.debezium.connector.mariadb.metrics.MariaDbChangeEventSourceMetricsFactory;
import io.debezium.connector.mariadb.metrics.MariaDbStreamingChangeEventSourceMetrics;
import io.debezium.document.DocumentReader;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.snapshot.Snapshotter;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

/**
 * The MariaDB connector task that performs snapshot and streaming of changes from the database.
 *
 * @author Chris Cranford
 */
public class MariaDbConnectorTask extends BinlogSourceTask<MariaDbPartition, MariaDbOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MariaDbConnectorTask.class);
    private static final String CONTEXT_NAME = "mariadb-connector-task";

    private volatile MariaDbTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile BinlogConnectorConnection connection;
    private volatile BinlogConnectorConnection beanRegistryJdbcConnection;
    private volatile ErrorHandler errorHandler;
    private volatile MariaDbDatabaseSchema schema;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return MariaDbConnectorConfig.ALL_FIELDS;
    }

    @Override
    protected ChangeEventSourceCoordinator<MariaDbPartition, MariaDbOffsetContext> start(Configuration configuration) {
        final Clock clock = Clock.system();
        final MariaDbConnectorConfig connectorConfig = new MariaDbConnectorConfig(configuration);
        final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(TOPIC_NAMING_STRATEGY);
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();
        final MariaDbValueConverters valueConverters = getValueConverters(connectorConfig);

        // DBZ-3238
        // Automatically set useCursorFetch to true when a snapshot fetch size other than the default is given.
        // By default, do not load whole result sets into memory.
        final Configuration config = configuration.edit()
                .withDefault("database.responseBuffering", "adaptive")
                .withDefault("database.fetchSize", 10_000)
                .withDefault("database.useCursorFetch", connectorConfig.useCursorFetch())
                .build();

        final MainConnectionProvidingConnectionFactory<BinlogConnectorConnection> connectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(() -> {
            final MariaDbConnectionConfiguration connectionConfig = new MariaDbConnectionConfiguration(config);
            return new MariaDbConnection(connectionConfig, new MariaDbFieldReader(connectorConfig));
        });

        this.connection = connectionFactory.mainConnection();

        final Offsets<MariaDbPartition, MariaDbOffsetContext> previousOffsets = getPreviousOffsets(
                new MariaDbPartition.Provider(connectorConfig, config),
                new MariaDbOffsetContext.Loader(connectorConfig));

        // Service providers
        registerServiceProviders(connectorConfig.getServiceRegistry());

        final boolean tableIdCaseInsensitive = connection.isTableIdCaseSensitive();
        CustomConverterRegistry converterRegistry = connectorConfig.getServiceRegistry().tryGetService(CustomConverterRegistry.class);

        this.schema = new MariaDbDatabaseSchema(connectorConfig, valueConverters, topicNamingStrategy, schemaNameAdjuster, tableIdCaseInsensitive, converterRegistry);
        taskContext = new MariaDbTaskContext(connectorConfig, schema);

        // Manual Bean Registration
        beanRegistryJdbcConnection = connectionFactory.newConnection();
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, config);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.DATABASE_SCHEMA, schema);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.JDBC_CONNECTION, beanRegistryJdbcConnection);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.VALUE_CONVERTER, valueConverters);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.OFFSETS, previousOffsets);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, taskContext);

        final SnapshotterService snapshotterService = connectorConfig.getServiceRegistry().tryGetService(SnapshotterService.class);
        final Snapshotter snapshotter = snapshotterService.getSnapshotter();

        validateBinlogConfiguration(snapshotter, connection);

        // If the binlog position is not available, it is necessary to re-execute the snapshot
        if (validateSnapshotFeasibility(snapshotter, previousOffsets.getTheOnlyOffset(), connection)) {
            previousOffsets.resetOffset(previousOffsets.getTheOnlyPartition());
        }

        LOGGER.info("Closing JDBC connection before starting schema recovery.");
        try {
            connection.close();
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }

        MariaDbOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        validateSchemaHistory(connectorConfig, connection::validateLogPosition, previousOffsets, schema, snapshotter);

        LOGGER.info("Reconnecting after validating schema recovery");

        try {
            try {
                connection.execute("SELECT 1");
            }
            catch (SQLException e) {
                LOGGER.warn("Connection was dropped during schema recovery. Reconnecting...");
                try {
                    connection.close();
                }
                catch (Exception e1) {
                    // Ignore any error
                }
                connection = connectionFactory.mainConnection();
            }

            connection.setAutoCommit(false);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to reconnect after schema recovery", e);
        }

        // If the binlog position is not available it is necessary to re-execute snapshot
        if (previousOffset == null) {
            LOGGER.info("No previous offset found");
        }
        else {
            LOGGER.info("Found previous offset {}", previousOffset);
        }

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .buffering()
                .build();

        errorHandler = new MariaDbErrorHandler(connectorConfig, queue, errorHandler);

        final BinlogEventMetadataProvider metadataProvider = new BinlogEventMetadataProvider();

        SignalProcessor<MariaDbPartition, MariaDbOffsetContext> signalProcessor = new SignalProcessor<>(
                MariaDbConnector.class,
                connectorConfig,
                Map.of(),
                getAvailableSignalChannels(),
                DocumentReader.defaultReader(),
                previousOffsets);

        final EventDispatcher<MariaDbPartition, TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicNamingStrategy,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                null,
                metadataProvider,
                new HeartbeatFactory<>().create(connectorConfig,
                        schemaNameAdjuster,
                        () -> new MariaDbConnection(
                                new MariaDbConnectionConfiguration(config),
                                getFieldReader(connectorConfig)),
                        new BinlogHeartbeatErrorHandler(),
                        topicNamingStrategy.heartbeatTopic()),
                schemaNameAdjuster,
                signalProcessor,
                connectorConfig.getServiceRegistry().tryGetService(DebeziumHeaderProducer.class));

        final MariaDbStreamingChangeEventSourceMetrics streamingMetrics = new MariaDbStreamingChangeEventSourceMetrics(
                taskContext,
                queue,
                metadataProvider);

        NotificationService<MariaDbPartition, MariaDbOffsetContext> notificationService = new NotificationService<>(
                getNotificationChannels(),
                connectorConfig,
                SchemaFactory.get(),
                dispatcher::enqueueNotification);

        ChangeEventSourceCoordinator<MariaDbPartition, MariaDbOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                previousOffsets,
                errorHandler,
                MariaDbConnector.class,
                connectorConfig,
                new MariaDbChangeEventSourceFactory(
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
                new MariaDbChangeEventSourceMetricsFactory(streamingMetrics),
                dispatcher,
                schema,
                signalProcessor,
                notificationService,
                snapshotterService);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    @Override
    protected String connectorName() {
        return Module.name();
    }

    @Override
    protected void doStop() {
        try {
            if (connection != null) {
                connection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing the JDBC connection.", e);
        }

        try {
            if (beanRegistryJdbcConnection != null) {
                beanRegistryJdbcConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing the Bean Registry JDBC connection.", e);
        }

        if (schema != null) {
            schema.close();
        }
    }

    @Override
    protected List<SourceRecord> doPoll() throws InterruptedException {
        final List<DataChangeEvent> records = queue.poll();
        return records.stream().map(DataChangeEvent::getRecord).collect(Collectors.toList());
    }

    @Override
    protected Optional<ErrorHandler> getErrorHandler() {
        return Optional.of(errorHandler);
    }

    private MariaDbValueConverters getValueConverters(MariaDbConnectorConfig connectorConfig) {
        return new MariaDbValueConverters(
                connectorConfig.getDecimalMode(),
                connectorConfig.getTemporalPrecisionMode(),
                connectorConfig.getBigIntUnsignedHandlingMode().asBigIntUnsignedMode(),
                connectorConfig.binaryHandlingMode(),
                connectorConfig.isTimeAdjustedEnabled() ? MariaDbValueConverters::adjustTemporal : x -> x,
                connectorConfig.getEventConvertingFailureHandlingMode(),
                connectorConfig.getServiceRegistry());
    }

    private BinlogFieldReader getFieldReader(MariaDbConnectorConfig connectorConfig) {
        return new MariaDbFieldReader(connectorConfig);
    }

}
