/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * The Kafka Connect source task for ${connectorName}.
 *
 * <p>Wires together all Debezium pipeline components and delegates snapshot/streaming
 * work to {@link ${connectorName}SnapshotChangeEventSource} and
 * {@link ${connectorName}StreamingChangeEventSource} via the coordinator.
 */
public class ${connectorName}ConnectorTask
        extends BaseSourceTask<${connectorName}Partition, ${connectorName}OffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(${connectorName}ConnectorTask.class);
    private static final String CONTEXT_NAME = "${connectorName.toLowerCase()}-connector-task";

    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile ${connectorName}ConnectorConfig connectorConfig;
    private volatile CdcSourceTaskContext<${connectorName}ConnectorConfig> taskContext;
    private volatile ErrorHandler errorHandler;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    protected String connectorName() {
        return Module.name();
    }

    @Override
    public CdcSourceTaskContext<${connectorName}ConnectorConfig> preStart(Configuration config) {
        connectorConfig = new ${connectorName}ConnectorConfig(config);
        taskContext = new CdcSourceTaskContext<>(config, connectorConfig, Map.of());
        return taskContext;
    }

    @Override
    public ChangeEventSourceCoordinator<${connectorName}Partition, ${connectorName}OffsetContext> start(
            Configuration config) {

        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();

        // Derive the data collection identifier from the connector configuration.
        // Adapt this to your connector's notion of a "table" or "collection".
        final String collectionName = connectorConfig.getLogicalName();
        final ${connectorName}DataCollectionId dataCollectionId =
                new ${connectorName}DataCollectionId(collectionName);

        // Restore or initialise offset context.
        final Offsets<${connectorName}Partition, ${connectorName}OffsetContext> previousOffsets =
                getPreviousOffsets(
                        new ${connectorName}Partition.Provider(connectorConfig),
                        new ${connectorName}OffsetLoader(connectorConfig));

        // Build the change event queue used to buffer records between threads.
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        this.errorHandler = new ${connectorName}ErrorHandler(connectorConfig, queue, null);

        final ${connectorName}DatabaseSchema schema =
                new ${connectorName}DatabaseSchema(connectorConfig, schemaNameAdjuster);

        final TopicNamingStrategy<${connectorName}DataCollectionId> topicNamingStrategy =
                connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY);

        final ${connectorName}EventMetadataProvider metadataProvider = new ${connectorName}EventMetadataProvider();

        final EventDispatcher<${connectorName}Partition, ${connectorName}DataCollectionId> dispatcher =
                new EventDispatcher<>(
                        connectorConfig,
                        topicNamingStrategy,
                        schema,
                        queue,
                        id -> true,
                        DataChangeEvent::new,
                        metadataProvider,
                        schemaNameAdjuster,
                        null);

        final NotificationService<${connectorName}Partition, ${connectorName}OffsetContext> notificationService =
                new NotificationService<>(
                        getNotificationChannels(),
                        connectorConfig,
                        SchemaFactory.get(),
                        dispatcher::enqueueNotification);

        final ChangeEventSourceCoordinator<${connectorName}Partition, ${connectorName}OffsetContext> coordinator =
                new ChangeEventSourceCoordinator<>(
                        previousOffsets,
                        errorHandler,
                        ${connectorName}SourceConnector.class,
                        connectorConfig,
                        new ${connectorName}ChangeEventSourceFactory(
                                connectorConfig, dataCollectionId, dispatcher, errorHandler),
                        new DefaultChangeEventSourceMetricsFactory<>(),
                        dispatcher,
                        schema,
                        null,
                        notificationService,
                        null);

        coordinator.start(taskContext, this.queue, metadataProvider);
        return coordinator;
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        return queue.poll()
                .stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());
    }

    @Override
    protected Optional<ErrorHandler> getErrorHandler() {
        return Optional.ofNullable(errorHandler);
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return ${connectorName}ConnectorConfig.ALL_FIELDS;
    }
}
