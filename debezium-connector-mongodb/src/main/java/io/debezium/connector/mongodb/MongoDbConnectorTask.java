/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static java.util.Comparator.comparing;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
import io.debezium.connector.mongodb.connection.ConnectionStrings;
import io.debezium.connector.mongodb.connection.MongoDbConnection;
import io.debezium.connector.mongodb.connection.MongoDbConnectionContext;
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
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.snapshot.Snapshotter;
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

    // These are all effectively constants between start(...) and stop(...)
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile String taskName;
    private volatile MongoDbTaskContext taskContext;
    private volatile MongoDbConnectionContext connectionContext;
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
        this.connectionContext = new MongoDbConnectionContext(config);

        final Schema structSchema = connectorConfig.getSourceInfoStructMaker().schema();
        this.schema = new MongoDbSchema(taskContext.getFilters(), taskContext.getTopicNamingStrategy(), structSchema, schemaNameAdjuster);

        final Offsets<MongoDbPartition, MongoDbOffsetContext> previousOffsets = getPreviousOffsets(connectorConfig);
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
                    previousOffsets);

            // Manually Register Beans
            connectorConfig.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);
            connectorConfig.getBeanRegistry().add(StandardBeanNames.DATABASE_SCHEMA, schema);
            connectorConfig.getBeanRegistry().add(StandardBeanNames.OFFSETS, previousOffsets);

            // Service providers
            registerServiceProviders(connectorConfig.getServiceRegistry());

            final SnapshotterService snapshotterService = connectorConfig.getServiceRegistry().tryGetService(SnapshotterService.class);

            final EventDispatcher<MongoDbPartition, CollectionId> dispatcher = new EventDispatcher<>(
                    connectorConfig,
                    taskContext.getTopicNamingStrategy(),
                    schema,
                    queue,
                    taskContext.getFilters().collectionFilter()::test,
                    DataChangeEvent::new,
                    metadataProvider,
                    schemaNameAdjuster,
                    signalProcessor);

            validate(connectorConfig, taskContext.getConnection(dispatcher, previousOffsets.getTheOnlyPartition()), previousOffsets,
                    snapshotterService.getSnapshotter());

            NotificationService<MongoDbPartition, MongoDbOffsetContext> notificationService = new NotificationService<>(getNotificationChannels(),
                    connectorConfig, SchemaFactory.get(), dispatcher::enqueueNotification);

            MongoDbChangeEventSourceMetricsFactory metricsFactory = new MongoDbChangeEventSourceMetricsFactory();

            ChangeEventSourceCoordinator<MongoDbPartition, MongoDbOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                    previousOffsets,
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
                            metricsFactory.getStreamingMetrics(taskContext, queue, metadataProvider),
                            snapshotterService),
                    metricsFactory,
                    dispatcher,
                    schema,
                    signalProcessor,
                    notificationService, snapshotterService);

            coordinator.start(taskContext, this.queue, metadataProvider);

            return coordinator;
        }
        finally {
            previousLogContext.restore();
        }
    }

    private Offsets<MongoDbPartition, MongoDbOffsetContext> getPreviousOffsets(MongoDbConnectorConfig connectorConfig) {
        var partitionProvider = new MongoDbPartition.Provider(connectorConfig, taskContext);
        var offsetLoader = new MongoDbOffsetContext.Loader(connectorConfig);
        var offsets = getPreviousOffsets(partitionProvider, offsetLoader);

        // Compare with previous generation
        if (connectorConfig.getMultiTaskEnabled() || connectorConfig.getMultiTaskPrevGen() >= 0) {
            int prevGen = connectorConfig.getMultiTaskPrevGen();
            var prevPartitionProvider = new MongoDbPartition.PrevGenProvider(connectorConfig, taskContext);
            var prevOffsets = getPreviousOffsets(prevPartitionProvider, offsetLoader);
            MongoDbOffsetContext oldestPrevOffset;
            if (connectorConfig.getMultiTaskPrevGen() < 0) {
                oldestPrevOffset = prevOffsets.getTheOnlyOffset();
            }
            else {
                if (prevOffsets.getOffsets().values().stream().anyMatch(Objects::isNull)) {
                    throw new IllegalStateException("Couldn't find saved offsets for previous generation");
                }
                oldestPrevOffset = prevOffsets.getOffsets()
                        .values()
                        .stream()
                        .min(MongoDbOffsetContext::compareTo)
                        .get();
            }
            if (Objects.compare(oldestPrevOffset, offsets.getTheOnlyOffset(), MongoDbOffsetContext::compareTo) > 0) {
                offsets.getOffsets().put(offsets.getTheOnlyPartition(), oldestPrevOffset);
                LOGGER.info("added offset {} = {} from generation {}",
                        offsets.getTheOnlyPartition(),
                        offsets.getTheOnlyOffset(),
                        prevGen);
            }
        }

        if (offsets.getTheOnlyOffset() != null) {
            return offsets;
        }
        LOGGER.info("Previous valid offset not found, checking compatible offsets from older versions");
        var name = connectionContext.getRequiredReplicaSetName()
                .orElse(ConnectionStrings.CLUSTER_RS_NAME);

        var compatibleOffset = getPreviousOffsets(
                new MongoDbPartition.Provider(connectorConfig, taskContext, Set.of(name)),
                new MongoDbOffsetContext.Loader(connectorConfig))
                .getTheOnlyOffset();

        if (compatibleOffset != null) {
            LOGGER.warn("Found compatible offset from previous version");
            offsets.getOffsets().put(offsets.getTheOnlyPartition(), compatibleOffset);
            return offsets;
        }

        LOGGER.info("Compatible offset not found, checking shard specific offsets from replica_set connection mode.");
        var shardNames = connectionContext.getShardNames();

        var shardOffsets = getPreviousOffsets(
                new MongoDbPartition.Provider(connectorConfig, taskContext, shardNames),
                new MongoDbOffsetContext.Loader(connectorConfig))
                .getOffsets();

        if (shardOffsets.values().stream().allMatch(Objects::isNull)) {
            LOGGER.info("No shard specific offsets found");
            return offsets;
        }

        LOGGER.warn("Found at least one shard specific offset from previous version");

        if (shardOffsets.values().stream().anyMatch(Objects::isNull)) {
            LOGGER.warn("At least one shard is missing previously recorded offset, so empty offset will be used");
            return offsets;
        }

        if (!connectorConfig.isOffsetInvalidationAllowed()) {
            LOGGER.warn("Offset invalidation is not allowed");
            throw new DebeziumException("Offsets from previous version are invalid, either manually delete them or " +
                    "set '" + MongoDbConnectorConfig.ALLOW_OFFSET_INVALIDATION.name() + "=true' " +
                    "to allow streaming to resume from the oldest shard specific offset");
        }

        LOGGER.warn("Offset invalidation is allowed");
        LOGGER.warn("The oldest shard specific offset will be used");

        var oldestOffset = shardOffsets.values()
                .stream()
                .filter(offset -> offset.lastTimestampOrTokenTime() != null)
                .min(comparing(MongoDbOffsetContext::lastTimestampOrTokenTime));

        oldestOffset.ifPresent(offset -> offsets.getOffsets().put(offsets.getTheOnlyPartition(), offset));

        return offsets;
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

    private void validate(MongoDbConnectorConfig connectorConfig, MongoDbConnection mongoDbConnection, Offsets<MongoDbPartition, MongoDbOffsetContext> previousOffsets,
                          Snapshotter snapshotter) {

        for (Map.Entry<MongoDbPartition, MongoDbOffsetContext> previousOffset : previousOffsets) {

            MongoDbOffsetContext offset = previousOffset.getValue();

            if (offset == null) {
                LOGGER.info("Connector started for the first time.");
                LOGGER.info("No previous offset has been found");
                return;
            }

            if (offset.isSnapshotRunning()) {
                // The last offset was an incomplete snapshot and now the snapshot was disabled
                if (!snapshotter.shouldSnapshotData(true, true)) {
                    // No snapshots are allowed
                    throw new DebeziumException("The connector previously stopped while taking a snapshot, but now the connector is configured "
                            + "to never allow snapshots. Reconfigure the connector to use snapshots initially or when needed.");
                }
                LOGGER.info("The previous snapshot was incomplete, so restarting the snapshot");

                return;
            }

            if (connectorConfig.isLogPositionCheckEnabled()) {
                boolean logPositionAvailable = mongoDbConnection.validateLogPosition(offset, taskContext);

                if (!logPositionAvailable) {
                    LOGGER.warn("Last recorded offset is no longer available on the server.");

                    if (snapshotter.shouldSnapshotOnDataError()) {

                        LOGGER.info("The last recorded offset is no longer available but we are in {} snapshot mode. " +
                                "Attempting to snapshot data to fill the gap.",
                                snapshotter.name());

                        previousOffsets.resetOffset(previousOffsets.getTheOnlyPartition());

                        return;
                    }

                    LOGGER.warn("The connector is trying to read change stream starting at " + offset + ", but this is no longer "
                            + "available on the server. Reconfigure the connector to use a snapshot when needed if you want to recover. " +
                            "If not the connector will streaming from the last available position in the log");
                }
            }
        }
    }
}
