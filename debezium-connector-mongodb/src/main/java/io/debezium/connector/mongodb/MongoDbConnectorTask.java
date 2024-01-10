/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.bean.StandardBeanNames;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.mongodb.metrics.MongoDbChangeEventSourceMetricsFactory;
import io.debezium.document.DocumentReader;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext.PreviousContext;

/**
 * A Kafka Connect source task that replicates the changes from one or more MongoDB replica sets.
 * <p>
 * Generally, the {@link MongoDbConnector} assigns each replica set to a separate task, although multiple
 * replica sets will be assigned to each task when the maximum number of tasks is limited. Regardless, every task will use a
 * separate thread to replicate the contents of each replica set, and each replication thread may use multiple threads
 * to perform an initial sync of the replica set.
 *
 * @see MongoDbConnector
 * @see MongoDbConnectorConfig
 * @author Randall Hauch
 */
@ThreadSafe
public final class MongoDbConnectorTask extends BaseSourceTask<MongoDbPartition, MongoDbOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbConnectorTask.class);

    private static final String CONTEXT_NAME = "mongodb-connector-task";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    // These are all effectively constants between start(...) and stop(...)
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile String taskName;
    private volatile MongoDbTaskContext taskContext;
    private volatile ErrorHandler errorHandler;
    private volatile MongoDbSchema schema;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public ChangeEventSourceCoordinator<MongoDbPartition, MongoDbOffsetContext> start(Configuration config) {
        final MongoDbConnectorConfig connectorConfig = new MongoDbConnectorConfig(config);
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();

        this.taskName = "task" + config.getInteger(MongoDbConnectorConfig.TASK_ID);
        this.taskContext = new MongoDbTaskContext(config);

        final Schema structSchema = connectorConfig.getSourceInfoStructMaker().schema();
        this.schema = new MongoDbSchema(taskContext.filters(), taskContext.topicNamingStrategy(), structSchema, schemaNameAdjuster);

        final Offsets<MongoDbPartition, MongoDbOffsetContext> previousOffset = getPreviousOffsets(
                new MongoDbPartition.Provider(taskContext),
                new MongoDbOffsetContext.Loader(taskContext));
        final Clock clock = Clock.system();

        PreviousContext previousLogContext = taskContext.configureLoggingContext(taskName);

        try {

            this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                    .pollInterval(connectorConfig.getPollInterval())
                    .maxBatchSize(connectorConfig.getMaxBatchSize())
                    .maxQueueSize(connectorConfig.getMaxQueueSize())
                    .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                    .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                    .build();

            errorHandler = new MongoDbErrorHandler(connectorConfig, queue, errorHandler);

            final MongoDbEventMetadataProvider metadataProvider = new MongoDbEventMetadataProvider();

            SignalProcessor<MongoDbPartition, MongoDbOffsetContext> signalProcessor = new SignalProcessor<>(
                    MongoDbConnector.class, connectorConfig, Map.of(),
                    getAvailableSignalChannels(),
                    DocumentReader.defaultReader(),
                    previousOffset);

            // Manually Register Beans
            connectorConfig.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);
            connectorConfig.getBeanRegistry().add(StandardBeanNames.DATABASE_SCHEMA, schema);

            // Service providers
            registerServiceProviders(connectorConfig.getServiceRegistry());

            final EventDispatcher<MongoDbPartition, CollectionId> dispatcher = new EventDispatcher<>(
                    connectorConfig,
                    taskContext.topicNamingStrategy(),
                    schema,
                    queue,
                    taskContext.filters().collectionFilter()::test,
                    DataChangeEvent::new,
                    metadataProvider,
                    schemaNameAdjuster,
                    signalProcessor);

            NotificationService<MongoDbPartition, MongoDbOffsetContext> notificationService = new NotificationService<>(getNotificationChannels(),
                    connectorConfig, SchemaFactory.get(), dispatcher::enqueueNotification);

            MongoDbChangeEventSourceMetricsFactory metricsFactory = new MongoDbChangeEventSourceMetricsFactory();

            ChangeEventSourceCoordinator<MongoDbPartition, MongoDbOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                    previousOffset,
                    errorHandler,
                    MongoDbConnector.class,
                    connectorConfig,
                    new MongoDbChangeEventSourceFactory(
                            connectorConfig,
                            errorHandler,
                            dispatcher,
                            clock,
                            taskContext,
                            schema,
                            metricsFactory.getStreamingMetrics(taskContext, queue, metadataProvider)),
                    metricsFactory,
                    dispatcher,
                    schema,
                    signalProcessor,
                    notificationService);

            coordinator.start(taskContext, this.queue, metadataProvider);

            return coordinator;
        }
        finally {
            previousLogContext.restore();
        }
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        List<DataChangeEvent> records = queue.poll();
        return records.stream().map(DataChangeEvent::getRecord).collect(Collectors.toList());
    }

    @Override
    public void doStop() {
        PreviousContext previousLogContext = this.taskContext.configureLoggingContext(taskName);
        try {
            if (schema != null) {
                schema.close();
            }
        }
        finally {
            previousLogContext.restore();
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return MongoDbConnectorConfig.ALL_FIELDS;
    }

    @Override
    protected Configuration withMaskedSensitiveOptions(Configuration config) {
        return super.withMaskedSensitiveOptions(config).withMasked(MongoDbConnectorConfig.CONNECTION_STRING.name());
    }
}
