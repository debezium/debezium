/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
import io.debezium.connector.mongodb.connection.ConnectionContext;
import io.debezium.connector.mongodb.connection.MongoDbConnection;
import io.debezium.connector.mongodb.connection.ReplicaSet;
import io.debezium.connector.mongodb.recordemitter.MongoDbSnapshotRecordEmitter;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.EventDispatcher.SnapshotReceiver;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import io.debezium.util.Threads;

/**
 * A {@link SnapshotChangeEventSource} that performs multi-threaded snapshots of replica sets.
 *
 * @author Chris Cranford
 */
public class MongoDbSnapshotChangeEventSource extends AbstractSnapshotChangeEventSource<MongoDbPartition, MongoDbOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbSnapshotChangeEventSource.class);

    private final MongoDbConnectorConfig connectorConfig;
    private final MongoDbTaskContext taskContext;
    private final MongoDbConnection.ChangeEventSourceConnectionFactory connections;
    private final ConnectionContext connectionContext;
    private final ReplicaSets replicaSets;
    private final EventDispatcher<MongoDbPartition, CollectionId> dispatcher;
    private final Clock clock;
    private final SnapshotProgressListener<MongoDbPartition> snapshotProgressListener;
    private final ErrorHandler errorHandler;
    private AtomicBoolean aborted = new AtomicBoolean(false);

    public MongoDbSnapshotChangeEventSource(MongoDbConnectorConfig connectorConfig, MongoDbTaskContext taskContext,
                                            MongoDbConnection.ChangeEventSourceConnectionFactory connections, ReplicaSets replicaSets,
                                            EventDispatcher<MongoDbPartition, CollectionId> dispatcher, Clock clock,
                                            SnapshotProgressListener<MongoDbPartition> snapshotProgressListener, ErrorHandler errorHandler) {
        super(connectorConfig, snapshotProgressListener);
        this.connectorConfig = connectorConfig;
        this.taskContext = taskContext;
        this.connections = connections;
        this.connectionContext = taskContext.getConnectionContext();
        this.replicaSets = replicaSets;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.snapshotProgressListener = snapshotProgressListener;
        this.errorHandler = errorHandler;
    }

    @Override
    protected SnapshotResult<MongoDbOffsetContext> doExecute(ChangeEventSourceContext context,
                                                             MongoDbOffsetContext prevOffsetCtx,
                                                             SnapshotContext<MongoDbPartition, MongoDbOffsetContext> snapshotContext,
                                                             SnapshottingTask snapshottingTask) {
        final MongoDbSnapshottingTask mongoDbSnapshottingTask = (MongoDbSnapshottingTask) snapshottingTask;
        final MongoDbSnapshotContext mongoDbSnapshotContext = (MongoDbSnapshotContext) snapshotContext;

        LOGGER.info("Snapshot step 1 - Preparing");
        if (prevOffsetCtx != null && prevOffsetCtx.isSnapshotRunning()) {
            LOGGER.info("Previous snapshot was cancelled before completion; a new snapshot will be taken.");
        }

        LOGGER.info("Snapshot step 2 - Determining snapshot offsets");
        List<ReplicaSet> replicaSetsToSnapshot = mongoDbSnapshottingTask.getReplicaSetsToSnapshot();
        initSnapshotStartOffsets(mongoDbSnapshotContext);

        final int threads = replicaSetsToSnapshot.size();
        LOGGER.info("Starting {} thread(s) to snapshot replica sets: {}", threads, replicaSetsToSnapshot);
        final ExecutorService executor = Threads.newFixedThreadPool(MongoDbConnector.class, taskContext.serverName(), "replicator-snapshot", threads);
        final CountDownLatch latch = new CountDownLatch(threads);

        LOGGER.info("Snapshot step 3 - Snapshotting data");
        replicaSetsToSnapshot.forEach(replicaSet -> {
            executor.submit(() -> {
                try {
                    taskContext.configureLoggingContext(replicaSet.replicaSetName());
                    snapshotReplicaSet(context, mongoDbSnapshotContext, replicaSet);
                }
                catch (Throwable t) {
                    LOGGER.error("Snapshot for replica set {} failed", replicaSet.replicaSetName(), t);
                    errorHandler.setProducerThrowable(t);
                }
                finally {
                    latch.countDown();
                }
            });
        });

        // Wait for the executor service threads to end.
        try {
            latch.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            aborted.set(true);
        }

        // Shutdown the executor
        executor.shutdown();

        if (aborted.get()) {
            return SnapshotResult.aborted();
        }

        return SnapshotResult.completed(snapshotContext.offset);
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(MongoDbPartition partition, MongoDbOffsetContext offsetContext) {
        // If no snapshot should occur, return task with no replica sets
        if (connectorConfig.getSnapshotMode().equals(MongoDbConnectorConfig.SnapshotMode.NEVER)) {
            LOGGER.info("According to the connector configuration, no snapshot will occur.");
            return new MongoDbSnapshottingTask(Collections.emptyList());
        }

        if (offsetContext == null) {
            LOGGER.info("No previous offset has been found");
            return new MongoDbSnapshottingTask(replicaSets.all());
        }

        // Collect which replica-sets require being snapshotted
        final var replicaSetsToSnapshot = replicaSets.all().stream()
                .filter(replicaSet -> isSnapshotExpected(partition, replicaSet, offsetContext))
                .collect(Collectors.toList());

        return new MongoDbSnapshottingTask(replicaSetsToSnapshot);
    }

    @Override
    protected SnapshotContext<MongoDbPartition, MongoDbOffsetContext> prepare(MongoDbPartition partition) {
        return new MongoDbSnapshotContext(partition);
    }

    private void snapshotReplicaSet(ChangeEventSourceContext sourceCtx, MongoDbSnapshotContext snapshotCtx, ReplicaSet replicaSet) throws InterruptedException {
        try (MongoDbConnection mongo = connections.get(replicaSet, snapshotCtx.partition)) {
            createDataEvents(sourceCtx, snapshotCtx, replicaSet, mongo);
        }
    }

    private boolean isSnapshotExpected(MongoDbPartition partition, ReplicaSet replicaSet, MongoDbOffsetContext offsetContext) {
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

        return isValidResumeToken(partition, replicaSet, token);
    }

    private boolean isValidResumeToken(MongoDbPartition partition, ReplicaSet replicaSet, BsonDocument token) {
        if (token == null) {
            return false;
        }

        try (MongoDbConnection mongo = connections.get(replicaSet, partition)) {
            return mongo.execute("Checking change stream", client -> {
                ChangeStreamIterable<BsonDocument> stream = client.watch(BsonDocument.class);
                stream.resumeAfter(token);

                try (var ignored = stream.cursor()) {
                    LOGGER.info("Valid resume token present for replica set '{}, so no snapshot will be performed'", replicaSet.replicaSetName());
                    return false;
                }
                catch (MongoCommandException | MongoChangeStreamException e) {
                    LOGGER.info("Invalid resume token present for replica set '{}, snapshot will be performed'", replicaSet.replicaSetName());
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

    private void initReplicaSetSnapshotStartOffsets(MongoDbSnapshotContext snapshotCtx, ReplicaSet replicaSet, MongoDbConnection mongo) throws InterruptedException {
        LOGGER.info("Determine Snapshot start offset for replica-set {}", replicaSet.replicaSetName());
        var rsOffsetCtx = snapshotCtx.offset.getReplicaSetOffsetContext(replicaSet);

        mongo.execute("Setting resume token", client -> {
            ChangeStreamIterable<BsonDocument> stream = client.watch(BsonDocument.class);
            try (MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor = stream.cursor()) {
                rsOffsetCtx.initEvent(cursor);
            }
            rsOffsetCtx.initFromOpTimeIfNeeded(client);
        });
    }

    private void createDataEvents(ChangeEventSourceContext sourceCtx, MongoDbSnapshotContext snapshotCtx, ReplicaSet replicaSet,
                                  MongoDbConnection mongo)
            throws InterruptedException {
        initReplicaSetSnapshotStartOffsets(snapshotCtx, replicaSet, mongo);
        SnapshotReceiver<MongoDbPartition> snapshotReceiver = dispatcher.getSnapshotChangeEventReceiver();
        snapshotCtx.offset.preSnapshotStart();

        createDataEventsForReplicaSet(sourceCtx, snapshotCtx, snapshotReceiver, replicaSet, mongo);

        snapshotCtx.offset.preSnapshotCompletion();
        snapshotReceiver.completeSnapshot();
        snapshotCtx.offset.postSnapshotCompletion();
    }

    /**
     * Dispatches the data change events for the records of a single replica-set.
     */
    private void createDataEventsForReplicaSet(ChangeEventSourceContext sourceContext,
                                               MongoDbSnapshotContext snapshotContext,
                                               SnapshotReceiver<MongoDbPartition> snapshotReceiver,
                                               ReplicaSet replicaSet, MongoDbConnection mongo)
            throws InterruptedException {

        final String rsName = replicaSet.replicaSetName();

        final MongoDbOffsetContext offsetContext = (MongoDbOffsetContext) snapshotContext.offset;
        final ReplicaSetOffsetContext rsOffsetContext = offsetContext.getReplicaSetOffsetContext(replicaSet);

        snapshotContext.lastCollection = false;
        offsetContext.startReplicaSetSnapshot(replicaSet.replicaSetName());

        LOGGER.info("Beginning snapshot of '{}' at {}", rsName, rsOffsetContext.getOffset());

        final List<CollectionId> collections = determineDataCollectionsToBeSnapshotted(mongo.collections()).collect(Collectors.toList());
        snapshotProgressListener.monitoredDataCollectionsDetermined(snapshotContext.partition, collections);
        if (connectorConfig.getSnapshotMaxThreads() > 1) {
            // Since multiple snapshot threads are to be used, create a thread pool and initiate the snapshot.
            // The current thread will wait until the snapshot threads either have completed or an error occurred.
            final int numThreads = Math.min(collections.size(), connectorConfig.getSnapshotMaxThreads());
            final Queue<CollectionId> collectionsToCopy = new ConcurrentLinkedQueue<>(collections);

            final String snapshotThreadName = "snapshot-" + (replicaSet.hasReplicaSetName() ? replicaSet.replicaSetName() : "main");
            final ExecutorService snapshotThreads = Threads.newFixedThreadPool(MongoDbConnector.class, taskContext.serverName(),
                    snapshotThreadName, connectorConfig.getSnapshotMaxThreads());
            final CountDownLatch latch = new CountDownLatch(numThreads);
            final AtomicBoolean aborted = new AtomicBoolean(false);
            final AtomicInteger threadCounter = new AtomicInteger(0);

            LOGGER.info("Preparing to use {} thread(s) to snapshot {} collection(s): {}", numThreads, collections.size(),
                    Strings.join(", ", collections));

            for (int i = 0; i < numThreads; ++i) {
                snapshotThreads.submit(() -> {
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
                                    replicaSet,
                                    id,
                                    mongo);
                        }
                    }
                    catch (InterruptedException e) {
                        // Do nothing so that this thread is stopped
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

            snapshotThreads.shutdown();
        }
        else {
            // Only 1 thread should be used for snapshotting collections.
            // In this use case since the replica-set snapshot is already in a separate thread, there is not
            // a real reason to spawn additional threads but instead just run within the current thread.
            for (Iterator<CollectionId> it = collections.iterator(); it.hasNext();) {
                final CollectionId collectionId = it.next();

                if (!sourceContext.isRunning()) {
                    throw new InterruptedException("Interrupted while snapshotting replica set " + replicaSet.replicaSetName());
                }

                if (!it.hasNext()) {
                    snapshotContext.lastCollection = true;
                }

                createDataEventsForCollection(
                        sourceContext,
                        snapshotContext,
                        snapshotReceiver,
                        replicaSet,
                        collectionId,
                        mongo);
            }
        }

        offsetContext.stopReplicaSetSnapshot(replicaSet.replicaSetName());
    }

    private void createDataEventsForCollection(ChangeEventSourceContext sourceContext,
                                               MongoDbSnapshotContext snapshotContext,
                                               SnapshotReceiver<MongoDbPartition> snapshotReceiver,
                                               ReplicaSet replicaSet, CollectionId collectionId, MongoDbConnection mongo)
            throws InterruptedException {

        long exportStart = clock.currentTimeInMillis();
        LOGGER.info("\t Exporting data for collection '{}'", collectionId);

        mongo.execute("sync '" + collectionId + "'", client -> {
            final MongoDatabase database = client.getDatabase(collectionId.dbName());
            final MongoCollection<BsonDocument> collection = database.getCollection(collectionId.name(), BsonDocument.class);

            final int batchSize = taskContext.getConnectorConfig().getSnapshotFetchSize();

            long docs = 0;
            Bson filterQuery = Document.parse(connectorConfig.getSnapshotFilterQueryForCollection(collectionId).orElseGet(() -> "{}"));

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
                                getChangeRecordEmitter(snapshotContext, collectionId, document, replicaSet),
                                snapshotReceiver);
                    }
                }
                else if (snapshotContext.lastCollection) {
                    // if the last collection does not contain any records we still need to mark the last processed event as last one
                    snapshotContext.offset.markSnapshotRecord(SnapshotRecord.LAST);
                }

                LOGGER.info("\t Finished snapshotting {} records for collection '{}'; total duration '{}'", docs, collectionId,
                        Strings.duration(clock.currentTimeInMillis() - exportStart));
                snapshotProgressListener.dataCollectionSnapshotCompleted(snapshotContext.partition, collectionId, docs);
            }
        });
    }

    private ChangeRecordEmitter<MongoDbPartition> getChangeRecordEmitter(SnapshotContext<MongoDbPartition, MongoDbOffsetContext> snapshotContext,
                                                                         CollectionId collectionId, BsonDocument document,
                                                                         ReplicaSet replicaSet) {
        final MongoDbOffsetContext offsetContext = snapshotContext.offset;

        final ReplicaSetPartition replicaSetPartition = offsetContext.getReplicaSetPartition(replicaSet);
        final ReplicaSetOffsetContext replicaSetOffsetContext = offsetContext.getReplicaSetOffsetContext(replicaSet);
        replicaSetOffsetContext.readEvent(collectionId, getClock().currentTime());

        return new MongoDbSnapshotRecordEmitter(replicaSetPartition, replicaSetOffsetContext, getClock(), document);
    }

    private Clock getClock() {
        return clock;
    }

    /**
     * A configuration describing the task to be performed during snapshotting.
     *
     * @see AbstractSnapshotChangeEventSource.SnapshottingTask
     */
    public static class MongoDbSnapshottingTask extends SnapshottingTask {

        private final List<ReplicaSet> replicaSetsToSnapshot;

        public MongoDbSnapshottingTask(List<ReplicaSet> replicaSetsToSnapshot) {
            super(false, !replicaSetsToSnapshot.isEmpty());
            this.replicaSetsToSnapshot = replicaSetsToSnapshot;
        }

        public List<ReplicaSet> getReplicaSetsToSnapshot() {
            return Collections.unmodifiableList(replicaSetsToSnapshot);
        }

        @Override
        public boolean shouldSkipSnapshot() {
            return !snapshotData();
        }

        @Override
        public String toString() {
            return "SnapshottingTask [replicaSetsToSnapshot=" + replicaSetsToSnapshot + "]";
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
