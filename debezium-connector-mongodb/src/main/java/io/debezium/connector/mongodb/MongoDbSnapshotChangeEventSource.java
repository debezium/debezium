/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoChangeStreamException;
import com.mongodb.MongoCommandException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

import io.debezium.DebeziumException;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.mongodb.connection.MongoDbConnection;
import io.debezium.connector.mongodb.connection.ReplicaSet;
import io.debezium.connector.mongodb.recordemitter.MongoDbSnapshotRecordEmitter;
import io.debezium.connector.mongodb.snapshot.MongoDbIncrementalSnapshotContext;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.EventDispatcher.SnapshotReceiver;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.actions.snapshotting.AdditionalCondition;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import io.debezium.util.Threads;

/**
 * A {@link SnapshotChangeEventSource} that performs multithreaded snapshots of replica sets.
 *
 * @author Chris Cranford
 */
public class MongoDbSnapshotChangeEventSource extends AbstractSnapshotChangeEventSource<MongoDbPartition, MongoDbOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbSnapshotChangeEventSource.class);

    private final MongoDbConnectorConfig connectorConfig;
    private final MongoDbTaskContext taskContext;
    private final MongoDbConnection.ChangeEventSourceConnectionFactory connections;
    private final ReplicaSet replicaSet;
    private final EventDispatcher<MongoDbPartition, CollectionId> dispatcher;
    private final Clock clock;
    private final SnapshotProgressListener<MongoDbPartition> snapshotProgressListener;
    private final ErrorHandler errorHandler;

    public MongoDbSnapshotChangeEventSource(MongoDbConnectorConfig connectorConfig, MongoDbTaskContext taskContext,
                                            MongoDbConnection.ChangeEventSourceConnectionFactory connections,
                                            EventDispatcher<MongoDbPartition, CollectionId> dispatcher, Clock clock,
                                            SnapshotProgressListener<MongoDbPartition> snapshotProgressListener, ErrorHandler errorHandler,
                                            NotificationService<MongoDbPartition, MongoDbOffsetContext> notificationService) {
        super(connectorConfig, snapshotProgressListener, notificationService);
        this.connectorConfig = connectorConfig;
        this.taskContext = taskContext;
        this.connections = connections;
        this.replicaSet = connectorConfig.getReplicaSet();
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.snapshotProgressListener = snapshotProgressListener;
        this.errorHandler = errorHandler;
    }

    /*
     * This is required because MongoDbPartition and MongoDbOffsetContext are not managed well for MongoDB. They are only correctly initialized just before starting CDC streaming
     * In the future only ReplicaSetPartition and ReplicaSetOffset should be present and initialized in the MongoDbConnectorTask
     */
    @Override
    protected Offsets<MongoDbPartition, OffsetContext> getOffsets(SnapshotContext<MongoDbPartition, MongoDbOffsetContext> snapshotContext,
                                                                  MongoDbOffsetContext mongoDbOffsetContext, SnapshottingTask snapshottingTask) {

        final MongoDbSnapshotContext mongoDbSnapshotContext = (MongoDbSnapshotContext) snapshotContext;

        if (mongoDbOffsetContext == null) {
            initSnapshotStartOffsets(mongoDbSnapshotContext);
            return Offsets.of(snapshotContext.offset.getReplicaSetPartition(replicaSet),
                    snapshotContext.offset.getReplicaSetOffsetContext(replicaSet));
        }

        return Offsets.of(mongoDbOffsetContext.getReplicaSetPartition(replicaSet),
                mongoDbOffsetContext.getReplicaSetOffsetContext(replicaSet));
    }

    @Override
    protected SnapshotResult<MongoDbOffsetContext> doExecute(ChangeEventSourceContext context,
                                                             MongoDbOffsetContext prevOffsetCtx,
                                                             SnapshotContext<MongoDbPartition, MongoDbOffsetContext> snapshotContext,
                                                             SnapshottingTask snapshottingTask) {
        final MongoDbSnapshotContext mongoDbSnapshotContext = (MongoDbSnapshotContext) snapshotContext;

        LOGGER.info("Snapshot step 1 - Preparing");
        if (prevOffsetCtx != null && prevOffsetCtx.isSnapshotRunning()) {
            LOGGER.info("Previous snapshot was cancelled before completion; a new snapshot will be taken.");
        }

        LOGGER.info("Snapshot step 2 - Determining snapshot offsets");
        initSnapshotStartOffsets(mongoDbSnapshotContext);

        LOGGER.info("Snapshot step 3 - Snapshotting data");
        try {
            doSnapshot(context, mongoDbSnapshotContext, snapshottingTask);
        }
        catch (Throwable t) {
            LOGGER.error("Snapshot failed", t);
            errorHandler.setProducerThrowable(t);
            // TODO: is this correct?
            return SnapshotResult.aborted();
        }

        return SnapshotResult.completed(snapshotContext.offset);
    }

    @Override
    public SnapshottingTask getBlockingSnapshottingTask(MongoDbPartition partition, MongoDbOffsetContext previousOffset, SnapshotConfiguration snapshotConfiguration) {

        Map<String, String> filtersByTable = snapshotConfiguration.getAdditionalConditions().stream()
                .collect(Collectors.toMap(k -> k.getDataCollection().toString(), AdditionalCondition::getFilter));

        return new SnapshottingTask(false, true, snapshotConfiguration.getDataCollections(), filtersByTable, true);
    }

    @Override
    public SnapshottingTask getSnapshottingTask(MongoDbPartition partition, MongoDbOffsetContext offsetContext) {

        List<String> dataCollectionsToBeSnapshotted = connectorConfig.getDataCollectionsToBeSnapshotted();

        // If no snapshot should occur, return task with no replica sets
        if (this.connectorConfig.getSnapshotMode().equals(MongoDbConnectorConfig.SnapshotMode.NEVER)) {
            LOGGER.info("According to the connector configuration, no snapshot will occur.");
            return new SnapshottingTask(false, false, dataCollectionsToBeSnapshotted, Map.of(), false);
        }

        if (offsetContext == null) {
            LOGGER.info("No previous offset has been found");
            return new SnapshottingTask(false, true, dataCollectionsToBeSnapshotted, connectorConfig.getSnapshotFilterQueryByCollection(), false);
        }

        var snapshotData = isSnapshotExpected(partition, offsetContext);

        return new SnapshottingTask(false, snapshotData, dataCollectionsToBeSnapshotted, connectorConfig.getSnapshotFilterQueryByCollection(), false);
    }

    @Override
    protected SnapshotContext<MongoDbPartition, MongoDbOffsetContext> prepare(MongoDbPartition partition, boolean onDemand) {
        return new MongoDbSnapshotContext(partition);
    }

    private void doSnapshot(ChangeEventSourceContext sourceCtx, MongoDbSnapshotContext snapshotCtx, SnapshottingTask snapshottingTask)
            throws InterruptedException {
        try (MongoDbConnection mongo = connections.get(snapshotCtx.partition)) {
            initSnapshotStartOffsets(snapshotCtx, mongo);
            SnapshotReceiver<MongoDbPartition> snapshotReceiver = dispatcher.getSnapshotChangeEventReceiver();
            snapshotCtx.offset.preSnapshotStart();

            createDataEvents(sourceCtx, snapshotCtx, snapshotReceiver, mongo, snapshottingTask);

            snapshotCtx.offset.preSnapshotCompletion();
            snapshotReceiver.completeSnapshot();
            snapshotCtx.offset.postSnapshotCompletion();
        }
    }

    private boolean isSnapshotExpected(MongoDbPartition partition, MongoDbOffsetContext offsetContext) {
        // todo: Right now we implement when needed snapshot by default. In the future we should provide the same options as other connectors.
        final ReplicaSetOffsetContext rsOffsetContext = offsetContext.getReplicaSetOffsetContext(replicaSet);

        if (!rsOffsetContext.hasOffset()) {
            LOGGER.info("No existing offset found for replica set '{}', starting snapshot", rsOffsetContext.getReplicaSetName());
            return true;
        }

        if (rsOffsetContext.isSnapshotOngoing()) {
            // The latest snapshot was not completed, so restart it
            LOGGER.info("The previous snapshot was incomplete for '{}', so restarting the snapshot", rsOffsetContext.getReplicaSetName());
            return true;
        }

        LOGGER.info("Found existing offset for replica set '{}' at {}", rsOffsetContext.getReplicaSetName(), rsOffsetContext.getOffset());
        final BsonDocument token = rsOffsetContext.lastResumeTokenDoc();

        return isValidResumeToken(partition, token);
    }

    private boolean isValidResumeToken(MongoDbPartition partition, BsonDocument token) {
        if (token == null) {
            return false;
        }

        try (MongoDbConnection mongo = connections.get(partition)) {
            return mongo.execute("Checking change stream", client -> {
                ChangeStreamIterable<BsonDocument> stream = MongoUtil.openChangeStream(client, taskContext);
                stream.resumeAfter(token);

                try (var ignored = stream.cursor()) {
                    LOGGER.info("Valid resume token present, so no snapshot will be performed'");
                    return false;
                }
                catch (MongoCommandException | MongoChangeStreamException e) {
                    LOGGER.info("Invalid resume token present, snapshot will be performed'");
                    return true;
                }
            });
        }
        catch (InterruptedException e) {
            throw new DebeziumException("Interrupted while creating snapshotting task", e);
        }
    }

    private void initSnapshotStartOffsets(MongoDbSnapshotContext snapshotCtx) {
        LOGGER.info("Initializing empty Offset context");
        snapshotCtx.offset = new MongoDbOffsetContext(
                new SourceInfo(connectorConfig),
                new TransactionContext(),
                new MongoDbIncrementalSnapshotContext<>(false));
    }

    private void initSnapshotStartOffsets(MongoDbSnapshotContext snapshotCtx, MongoDbConnection mongo) throws InterruptedException {
        LOGGER.info("Determine Snapshot start offset");
        var rsOffsetCtx = snapshotCtx.offset.getReplicaSetOffsetContext(replicaSet);

        mongo.execute("Setting resume token", client -> {
            ChangeStreamIterable<BsonDocument> stream = MongoUtil.openChangeStream(client, taskContext);
            try (MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor = stream.cursor()) {
                rsOffsetCtx.initEvent(cursor);
            }
        });
        rsOffsetCtx.initFromOpTimeIfNeeded(mongo.hello());
    }

    /**
     * Dispatches the data change events for the records of a single replica-set.
     */
    private void createDataEvents(ChangeEventSourceContext sourceContext,
                                  MongoDbSnapshotContext snapshotContext,
                                  SnapshotReceiver<MongoDbPartition> snapshotReceiver,
                                  MongoDbConnection mongo,
                                  SnapshottingTask snapshottingTask)
            throws InterruptedException {

        final String rsName = replicaSet.replicaSetName();

        final MongoDbOffsetContext offsetContext = snapshotContext.offset;
        final ReplicaSetOffsetContext rsOffsetContext = offsetContext.getReplicaSetOffsetContext(replicaSet);

        snapshotContext.lastCollection = false;
        offsetContext.startReplicaSetSnapshot(replicaSet.replicaSetName());

        LOGGER.info("Beginning snapshot of '{}' at {}", rsName, rsOffsetContext.getOffset());

        Set<Pattern> dataCollectionPattern = getDataCollectionPattern(snapshottingTask.getDataCollections());

        final List<CollectionId> collections = determineDataCollectionsToBeSnapshotted(mongo.collections(), dataCollectionPattern)
                .collect(Collectors.toList());
        snapshotProgressListener.monitoredDataCollectionsDetermined(snapshotContext.partition, collections);

        // Since multiple snapshot threads are to be used, create a thread pool and initiate the snapshot.
        // The current thread will wait until the snapshot threads either have completed or an error occurred.
        final int numThreads = Math.min(collections.size(), connectorConfig.getSnapshotMaxThreads());
        final Queue<CollectionId> collectionsToCopy = new ConcurrentLinkedQueue<>(collections);

        final String snapshotThreadName = "snapshot-" + (replicaSet.hasReplicaSetName() ? replicaSet.replicaSetName() : "main");

        LOGGER.info("Creating snapshot worker pool with {} worker thread(s)", numThreads);
        final ExecutorService executorService = Threads.newFixedThreadPool(MongoDbConnector.class, taskContext.serverName(), snapshotThreadName,
                connectorConfig.getSnapshotMaxThreads());
        final CountDownLatch latch = new CountDownLatch(numThreads);
        final AtomicBoolean aborted = new AtomicBoolean(false);
        final AtomicInteger threadCounter = new AtomicInteger(0);

        LOGGER.info("Preparing to use {} thread(s) to snapshot {} collection(s): {}", numThreads, collections.size(),
                Strings.join(", ", collections));

        for (int i = 0; i < numThreads; ++i) {
            executorService.submit(() -> {
                taskContext.configureLoggingContext(replicaSet.replicaSetName() + "-snapshot" + threadCounter.incrementAndGet());
                try {
                    CollectionId id = null;
                    while (!aborted.get() && (id = collectionsToCopy.poll()) != null) {
                        if (!sourceContext.isRunning()) {
                            throw new InterruptedException("Interrupted while snapshotting replica set " + replicaSet.replicaSetName());
                        }

                        if (collectionsToCopy.isEmpty()) {
                            snapshotContext.lastCollection = true;
                        }

                        createDataEventsForCollection(
                                sourceContext,
                                snapshotContext,
                                snapshotReceiver,
                                id,
                                mongo, snapshottingTask.getFilterQueries());
                    }
                }
                catch (Throwable t) {
                    // Do nothing so that this thread is stopped
                    LOGGER.error("Snapshot failed", t);
                    errorHandler.setProducerThrowable(t);
                    aborted.set(true);
                }
                finally {
                    latch.countDown();
                }
            });
        }

        // wait for all copy threads to finish
        try {
            latch.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            aborted.set(true);
        }
        finally {
            executorService.shutdown();
        }

        offsetContext.stopReplicaSetSnapshot(replicaSet.replicaSetName());
    }

    @Override
    protected <T extends DataCollectionId> Stream<T> determineDataCollectionsToBeSnapshotted(final Collection<T> allDataCollections,
                                                                                             Set<Pattern> snapshotAllowedDataCollections) {
        if (snapshotAllowedDataCollections.isEmpty()) {
            return allDataCollections.stream();
        }
        else {
            return allDataCollections.stream()
                    .filter(dataCollectionId -> snapshotAllowedDataCollections.stream()
                            .anyMatch(s -> s.matcher(((CollectionId) dataCollectionId).namespace()).matches()));
        }
    }

    private void createDataEventsForCollection(ChangeEventSourceContext sourceContext,
                                               MongoDbSnapshotContext snapshotContext,
                                               SnapshotReceiver<MongoDbPartition> snapshotReceiver,
                                               CollectionId collectionId, MongoDbConnection mongo,
                                               Map<String, String> snapshotFilterQueryForCollection)
            throws InterruptedException {

        var rsPartition = snapshotContext.offset.getReplicaSetPartition(replicaSet);
        var rsOffset = snapshotContext.offset.getReplicaSetOffsetContext(replicaSet);

        long exportStart = clock.currentTimeInMillis();
        LOGGER.info("\t Exporting data for collection '{}'", collectionId);
        notificationService.initialSnapshotNotificationService().notifyTableInProgress(rsPartition, rsOffset, collectionId.namespace());

        mongo.execute("sync '" + collectionId + "'", client -> {
            final MongoDatabase database = client.getDatabase(collectionId.dbName());
            final MongoCollection<BsonDocument> collection = database.getCollection(collectionId.name(), BsonDocument.class);

            final int batchSize = taskContext.getConnectorConfig().getSnapshotFetchSize();

            long docs = 0;
            Optional<String> snapshotFilterForCollectionId = Optional.ofNullable(snapshotFilterQueryForCollection.get(collectionId.dbName() + "." + collectionId.name()));
            Bson filterQuery = Document.parse(snapshotFilterForCollectionId.orElse("{}"));

            try (MongoCursor<BsonDocument> cursor = collection.find(filterQuery).batchSize(batchSize).iterator()) {
                snapshotContext.lastRecordInCollection = false;
                if (cursor.hasNext()) {
                    while (cursor.hasNext()) {
                        if (!sourceContext.isRunning()) {
                            throw new InterruptedException("Interrupted while snapshotting collection " + collectionId.name());
                        }

                        BsonDocument document = cursor.next();
                        docs++;

                        snapshotContext.lastRecordInCollection = !cursor.hasNext();

                        if (snapshotContext.lastCollection && snapshotContext.lastRecordInCollection) {
                            snapshotContext.offset.markSnapshotRecord(SnapshotRecord.LAST);
                        }

                        dispatcher.dispatchSnapshotEvent(snapshotContext.partition, collectionId,
                                getChangeRecordEmitter(snapshotContext, collectionId, document),
                                snapshotReceiver);
                    }
                }
                else if (snapshotContext.lastCollection) {
                    // if the last collection does not contain any records we still need to mark the last processed event as last one
                    snapshotContext.offset.markSnapshotRecord(SnapshotRecord.LAST);
                }

                notificationService.initialSnapshotNotificationService().notifyCompletedTableSuccessfully(rsPartition, rsOffset, collectionId.namespace());
                LOGGER.info("\t Finished snapshotting {} records for collection '{}'; total duration '{}'", docs, collectionId,
                        Strings.duration(clock.currentTimeInMillis() - exportStart));
                snapshotProgressListener.dataCollectionSnapshotCompleted(snapshotContext.partition, collectionId, docs);
            }
        });
    }

    private ChangeRecordEmitter<MongoDbPartition> getChangeRecordEmitter(SnapshotContext<MongoDbPartition, MongoDbOffsetContext> snapshotContext,
                                                                         CollectionId collectionId, BsonDocument document) {
        final MongoDbOffsetContext offsetContext = snapshotContext.offset;

        final ReplicaSetPartition replicaSetPartition = offsetContext.getReplicaSetPartition(replicaSet);
        final ReplicaSetOffsetContext replicaSetOffsetContext = offsetContext.getReplicaSetOffsetContext(replicaSet);
        replicaSetOffsetContext.readEvent(collectionId, getClock().currentTime());

        return new MongoDbSnapshotRecordEmitter(replicaSetPartition, replicaSetOffsetContext, getClock(), document, connectorConfig);
    }

    private Clock getClock() {
        return clock;
    }

    /**
     * A configuration describing the task to be performed during snapshotting.
     *
     * @see SnapshottingTask
     */
    public static class MongoDbSnapshottingTask extends SnapshottingTask {

        public MongoDbSnapshottingTask(boolean snapshotData, List<String> dataCollections, Map<String, String> filterQueries, boolean isBlocking) {
            super(false, snapshotData, dataCollections, filterQueries, isBlocking);
        }

        @Override
        public boolean shouldSkipSnapshot() {
            return !snapshotData();
        }
    }

    /**
     * Mutable context that is populated in the course of snapshotting.
     */
    private static class MongoDbSnapshotContext extends SnapshotContext<MongoDbPartition, MongoDbOffsetContext> {
        public boolean lastCollection;
        public boolean lastRecordInCollection;

        MongoDbSnapshotContext(MongoDbPartition partition) {
            super(partition);
        }
    }
}
