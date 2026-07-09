/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.bean.StandardBeanNames;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.document.DocumentReader;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

/**
 * The main task executing the TiDB connector: it consumes the Debezium-format output of a TiCDC
 * changefeed from Kafka and re-emits it through the Debezium connector framework.
 *
 * @author Aviral Srivastava
 */
public class TiDbConnectorTask extends BaseSourceTask<TiDbPartition, TiDbOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TiDbConnectorTask.class);
    private static final String CONTEXT_NAME = "tidb-connector-task";

    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile ErrorHandler errorHandler;
    private volatile TiDbSchema schema;
    private volatile TiDbTaskContext taskContext;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public CdcSourceTaskContext<TiDbConnectorConfig> preStart(Configuration config) {
        final TiDbConnectorConfig connectorConfig = new TiDbConnectorConfig(config);
        this.taskContext = new TiDbTaskContext(config, connectorConfig);
        return taskContext;
    }

    @Override
    protected ChangeEventSourceCoordinator<TiDbPartition, TiDbOffsetContext> start(Configuration config) {
        final TiDbConnectorConfig connectorConfig = new TiDbConnectorConfig(config);
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();
        final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(TiDbConnectorConfig.TOPIC_NAMING_STRATEGY);
        final Clock clock = Clock.system();

        this.schema = new TiDbSchema(topicNamingStrategy, connectorConfig.getSourceInfoStructMaker().schema(), schemaNameAdjuster);

        final Offsets<TiDbPartition, TiDbOffsetContext> previousOffsets = getPreviousOffsets(
                new TiDbPartition.Provider(connectorConfig), new TiDbOffsetContext.Loader(connectorConfig));

        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        this.errorHandler = new TiDbErrorHandler(connectorConfig, queue, errorHandler);

        final TiDbEventMetadataProvider metadataProvider = new TiDbEventMetadataProvider();

        final SignalProcessor<TiDbPartition, TiDbOffsetContext> signalProcessor = new SignalProcessor<>(
                TiDbConnector.class, connectorConfig, Map.of(),
                getAvailableSignalChannels(),
                DocumentReader.defaultReader(),
                previousOffsets);

        // Manually register beans
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, config);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.DATABASE_SCHEMA, schema);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.OFFSETS, previousOffsets);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, taskContext);

        // Service providers
        registerServiceProviders(connectorConfig.getServiceRegistry());

        final SnapshotterService snapshotterService = connectorConfig.getServiceRegistry().tryGetService(SnapshotterService.class);

        final EventDispatcher<TiDbPartition, TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicNamingStrategy,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                metadataProvider,
                schemaNameAdjuster,
                signalProcessor,
                connectorConfig.getServiceRegistry().tryGetService(DebeziumHeaderProducer.class));

        final NotificationService<TiDbPartition, TiDbOffsetContext> notificationService = new NotificationService<>(
                getNotificationChannels(), connectorConfig, SchemaFactory.get(), dispatcher::enqueueNotification);

        final ChangeEventSourceCoordinator<TiDbPartition, TiDbOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                previousOffsets,
                errorHandler,
                TiDbConnector.class,
                connectorConfig,
                new TiDbChangeEventSourceFactory(connectorConfig, errorHandler, dispatcher, clock, schema),
                new DefaultChangeEventSourceMetricsFactory<>(),
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
    protected List<SourceRecord> doPoll() throws InterruptedException {
        return queue.poll().stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());
    }

    @Override
    protected Optional<ErrorHandler> getErrorHandler() {
        return Optional.ofNullable(errorHandler);
    }

    @Override
    protected void doStop() {
        if (schema != null) {
            schema.close();
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return TiDbConnectorConfig.ALL_FIELDS;
    }
}
