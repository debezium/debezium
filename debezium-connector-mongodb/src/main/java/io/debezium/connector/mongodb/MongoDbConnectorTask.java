/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.mongodb.MongoDbConnectorConfig.CaptureMode;
import io.debezium.connector.mongodb.metrics.MongoDbChangeEventSourceMetricsFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext.PreviousContext;
import io.debezium.util.SchemaNameAdjuster;

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
        final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();

        this.taskName = "task" + config.getInteger(MongoDbConnectorConfig.TASK_ID);
        this.taskContext = new MongoDbTaskContext(config);

        final Schema structSchema = connectorConfig.getSourceInfoStructMaker().schema();
        this.schema = new MongoDbSchema(taskContext.filters(), taskContext.topicSelector(), structSchema);

        final ReplicaSets replicaSets = getReplicaSets(config);
        final MongoDbOffsetContext previousOffset = getPreviousOffset(connectorConfig, replicaSets);
        final Clock clock = Clock.system();

        if (previousOffset != null) {
            final List<ReplicaSetOffsetContext> oplogBasedOffsets = new ArrayList<>();
            final List<ReplicaSetOffsetContext> changeStreamBasedOffsets = new ArrayList<>();
            replicaSets.all().forEach(rs -> {
                final ReplicaSetOffsetContext offset = previousOffset.getReplicaSetOffsetContext(rs);
                if (rs == null) {
                    return;
                }
                if (offset.isFromChangeStream()) {
                    changeStreamBasedOffsets.add(offset);
                }
                if (offset.isFromOplog()) {
                    oplogBasedOffsets.add(offset);
                }
            });
            // TODO Add test
            if (!oplogBasedOffsets.isEmpty() && !changeStreamBasedOffsets.isEmpty()) {
                LOGGER.error(
                        "Replica set offests are partially from oplog and partially from change streams. This is not supported situation and can lead to unpredicable behaviour.");
            }
            else if (!oplogBasedOffsets.isEmpty() && connectorConfig.getCaptureMode().isChangeStreams()) {
                LOGGER.warn("Stored offsets were created using oplog capturing. Connector configuration expects change streams capturing.");
                LOGGER.warn("Switching configuration to '{}'", CaptureMode.OPLOG);
                LOGGER.warn("Either reconfigure the connector or remove the old offsets");
                connectorConfig.setCaptureMode(CaptureMode.OPLOG);
            }
            else if (!changeStreamBasedOffsets.isEmpty() && !connectorConfig.getCaptureMode().isChangeStreams()) {
                LOGGER.warn("Stored offsets were created using change streams capturing. Connector configuration expects oplog capturing.");
                LOGGER.warn("Switching configuration to '{}'", CaptureMode.CHANGE_STREAMS_UPDATE_FULL);
                LOGGER.warn("Either reconfigure the connector or remove the old offsets");
                connectorConfig.setCaptureMode(CaptureMode.CHANGE_STREAMS_UPDATE_FULL);
            }
        }

        PreviousContext previousLogContext = taskContext.configureLoggingContext(taskName);

        try {

            this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                    .pollInterval(connectorConfig.getPollInterval())
                    .maxBatchSize(connectorConfig.getMaxBatchSize())
                    .maxQueueSize(connectorConfig.getMaxQueueSize())
                    .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                    .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                    .build();

            errorHandler = new MongoDbErrorHandler(connectorConfig.getLogicalName(), queue);

            final MongoDbEventMetadataProvider metadataProvider = new MongoDbEventMetadataProvider();

            final EventDispatcher<CollectionId> dispatcher = new EventDispatcher<>(
                    connectorConfig,
                    taskContext.topicSelector(),
                    schema,
                    queue,
                    taskContext.filters().collectionFilter()::test,
                    DataChangeEvent::new,
                    metadataProvider,
                    schemaNameAdjuster);

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
                            taskContext),
                    new MongoDbChangeEventSourceMetricsFactory(),
                    dispatcher,
                    schema);

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
        if (offsets != null && !offsets.values().stream().filter(Objects::nonNull).collect(Collectors.toList()).isEmpty()) {
            MongoDbOffsetContext offsetContext = loader.loadOffsets(offsets);
            logger.info("Found previous offsets {}", offsetContext);
            return offsetContext;
        }
        else {
            return null;
        }
    }

    private ReplicaSets getReplicaSets(Configuration config) {
        final String hosts = config.getString(MongoDbConnectorConfig.HOSTS);
        final ReplicaSets replicaSets = ReplicaSets.parse(hosts);
        if (replicaSets.validReplicaSetCount() == 0) {
            throw new ConnectException("Unable to start MongoDB connector task since no replica sets were found at " + hosts);
        }
        return replicaSets;
    }
}
