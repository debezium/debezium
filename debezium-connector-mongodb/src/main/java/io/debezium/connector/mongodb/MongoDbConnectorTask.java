/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.ThreadSafe;
import io.debezium.bean.StandardBeanNames;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.mongodb.connection.ReplicaSet;
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

        final ReplicaSets replicaSets = getReplicaSets(connectorConfig);
        final MongoDbOffsetContext previousOffset = getPreviousOffset(connectorConfig, replicaSets);
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
                    Offsets.of(Collections.singletonMap(new MongoDbPartition(), previousOffset)));

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
                    // TODO pass offsets from all the partitions
                    Offsets.of(Collections.singletonMap(new MongoDbPartition(), previousOffset)),
                    errorHandler,
                    MongoDbConnector.class,
                    connectorConfig,
                    new MongoDbChangeEventSourceFactory(
                            connectorConfig,
                            errorHandler,
                            dispatcher,
                            clock,
                            replicaSets,
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

    private MongoDbOffsetContext getPreviousOffset(MongoDbConnectorConfig connectorConfig, ReplicaSets replicaSets) {
        MongoDbOffsetContext.Loader loader = new MongoDbOffsetContext.Loader(connectorConfig, replicaSets);
        Collection<Map<String, String>> partitions = loader.getPartitions();

        Map<Map<String, String>, Map<String, Object>> offsets = context.offsetStorageReader().offsets(partitions);
        if (offsets != null && offsets.values().stream().anyMatch(Objects::nonNull)) {
            MongoDbOffsetContext offsetContext = loader.loadOffsets(offsets);
            logger.info("Found previous offsets {}", offsetContext);
            return offsetContext;
        }
        else {
            checkShardSpecificOffsetsIfNeeded(connectorConfig, replicaSets);
            return null;
        }
    }

    private void checkShardSpecificOffsetsIfNeeded(MongoDbConnectorConfig connectorConfig, ReplicaSets currentReplicaSets) {
        if (currentReplicaSets.size() != 1 || !currentReplicaSets.getSnapshotReplicaSet().isClusterRs()) {
            // We are not running in sharded connection mode, so no check is needed
            return;
        }

        logger.info("Previous offset not found, checking shard specific offsets from replica_set connection mode.");
        var discovery = new ReplicaSetDiscovery(taskContext);
        var replicaSetSpecs = new HashSet<ReplicaSet>();

        try (var client = taskContext.getConnectionContext().connect()) {
            discovery.readReplicaSetsFromShardedCluster(replicaSetSpecs, client);
        }
        catch (Throwable t) {
            logger.warn("Unable to read shard topology.");
            return;
        }

        var replicaSets = new ReplicaSets(replicaSetSpecs);
        var loader = new MongoDbOffsetContext.Loader(connectorConfig, replicaSets);
        Collection<Map<String, String>> partitions = loader.getPartitions();
        Map<Map<String, String>, Map<String, Object>> offsets = context.offsetStorageReader().offsets(partitions);

        if (offsets != null && offsets.values().stream().anyMatch(Objects::nonNull)) {
            logger.warn("Found at least one shard specific offset from previous run");
            if (connectorConfig.isOffsetInvalidationAllowed()) {
                logger.warn("Offset invalidation is allowed, previous shard specific offsets will be ignored and snapshot re-executed");
                return;
            }

            throw new DebeziumException("Found at least one shard specific offset from previous run." +
                    "The default connection mode for sharded has changed to 'sharded' and previous offsets would be invalidated." +
                    "Either explicitly set '" + MongoDbConnectorConfig.CONNECTION_MODE.name() + "=replica_set' to postpone the migration " +
                    "or set '" + MongoDbConnectorConfig.ALLOW_OFFSET_INVALIDATION.name() + "=true' to re-execute snapshot and reset offsets. " +
                    "In next release the 'replica_set' connection mode will be removed.");
        }
    }

    private ReplicaSets getReplicaSets(MongoDbConnectorConfig connectorConfig) {
        final ReplicaSets replicaSets = connectorConfig.getReplicaSets();
        if (replicaSets.size() == 0) {
            throw new DebeziumException("Unable to start MongoDB connector task since no replica sets were found");
        }
        return replicaSets;
    }

    @Override
    protected Configuration withMaskedSensitiveOptions(Configuration config) {
        return super.withMaskedSensitiveOptions(config).withMasked(MongoDbConnectorConfig.CONNECTION_STRING.name());
    }
}
