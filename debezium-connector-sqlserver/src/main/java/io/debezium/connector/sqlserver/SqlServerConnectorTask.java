/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.sqlserver.metrics.SqlServerMetricsFactory;
import io.debezium.document.DocumentReader;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

/**
 * The main task executing streaming from SQL Server.
 * Responsible for lifecycle management the streaming code.
 *
 * @author Jiri Pechanec
 *
 */
public class SqlServerConnectorTask extends BaseSourceTask<SqlServerPartition, SqlServerOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerConnectorTask.class);
    private static final String CONTEXT_NAME = "sql-server-connector-task";

    private volatile SqlServerTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile SqlServerConnection dataConnection;
    private volatile SqlServerConnection metadataConnection;
    private volatile SqlServerErrorHandler errorHandler;
    private volatile SqlServerDatabaseSchema schema;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public ChangeEventSourceCoordinator<SqlServerPartition, SqlServerOffsetContext> start(Configuration config) {
        final Clock clock = Clock.system();

        // By default do not load whole result sets into memory
        config = config.edit()
                .withDefault(CommonConnectorConfig.DRIVER_CONFIG_PREFIX + "responseBuffering", "adaptive")
                .withDefault(CommonConnectorConfig.DRIVER_CONFIG_PREFIX + "fetchSize", 10_000)
                .build();

        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(config);
        final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY, true);
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();
        final SqlServerValueConverters valueConverters = new SqlServerValueConverters(connectorConfig.getDecimalMode(),
                connectorConfig.getTemporalPrecisionMode(), connectorConfig.binaryHandlingMode());

        MainConnectionProvidingConnectionFactory<SqlServerConnection> connectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(
                () -> new SqlServerConnection(connectorConfig,
                        valueConverters, connectorConfig.getSkippedOperations(), connectorConfig.useSingleDatabase(), connectorConfig.getOptionRecompile()));
        dataConnection = connectionFactory.mainConnection();
        metadataConnection = new SqlServerConnection(connectorConfig, valueConverters,
                connectorConfig.getSkippedOperations(), connectorConfig.useSingleDatabase());

        this.schema = new SqlServerDatabaseSchema(connectorConfig, metadataConnection.getDefaultValueConverter(), valueConverters, topicNamingStrategy,
                schemaNameAdjuster);
        this.schema.initializeStorage();

        Offsets<SqlServerPartition, SqlServerOffsetContext> offsets = getPreviousOffsets(
                new SqlServerPartition.Provider(connectorConfig),
                new SqlServerOffsetContext.Loader(connectorConfig));

        schema.recover(offsets);

        connectorConfig.postProcessorRegistry().injectDependencies(valueConverters, metadataConnection, schema, connectorConfig);

        taskContext = new SqlServerTaskContext(connectorConfig, schema);

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        errorHandler = new SqlServerErrorHandler(connectorConfig, queue, errorHandler);

        final SqlServerEventMetadataProvider metadataProvider = new SqlServerEventMetadataProvider();

        SignalProcessor<SqlServerPartition, SqlServerOffsetContext> signalProcessor = new SignalProcessor<>(
                SqlServerConnector.class, connectorConfig, Map.of(),
                getAvailableSignalChannels(),
                DocumentReader.defaultReader(),
                offsets);

        final EventDispatcher<SqlServerPartition, TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicNamingStrategy,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                metadataProvider,
                schemaNameAdjuster,
                signalProcessor);

        NotificationService<SqlServerPartition, SqlServerOffsetContext> notificationService = new NotificationService<>(getNotificationChannels(),
                connectorConfig, SchemaFactory.get(), dispatcher::enqueueNotification);

        ChangeEventSourceCoordinator<SqlServerPartition, SqlServerOffsetContext> coordinator = new SqlServerChangeEventSourceCoordinator(
                offsets,
                errorHandler,
                SqlServerConnector.class,
                connectorConfig,
                new SqlServerChangeEventSourceFactory(connectorConfig, connectionFactory, metadataConnection, errorHandler, dispatcher, clock, schema,
                        notificationService),
                new SqlServerMetricsFactory(offsets.getPartitions()),
                dispatcher,
                schema,
                clock,
                signalProcessor,
                notificationService);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        final List<DataChangeEvent> records = queue.poll();

        final List<SourceRecord> sourceRecords = records.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());

        // Reset the retries if all partitions have streamed without exceptions at least once after a restart
        if (coordinator.getErrorHandler().getRetries() > 0 && ((SqlServerChangeEventSourceCoordinator) coordinator).firstStreamingIterationCompletedSuccessfully()) {
            coordinator.getErrorHandler().resetRetries();
        }

        return sourceRecords;
    }

    @Override
    protected void doStop() {
        try {
            if (dataConnection != null) {
                dataConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC connection", e);
        }

        try {
            if (metadataConnection != null) {
                metadataConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC metadata connection", e);
        }

        if (schema != null) {
            schema.close();
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return SqlServerConnectorConfig.ALL_FIELDS;
    }
}
