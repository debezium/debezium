/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import io.debezium.connector.mongodb.ConnectionContext.MongoPrimary;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.EventDispatcher.SnapshotReceiver;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
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
public class MongoDbSnapshotChangeEventSource extends AbstractSnapshotChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbSnapshotChangeEventSource.class);

    private static final String AUTHORIZATION_FAILURE_MESSAGE = "Command failed with error 13";

    private final MongoDbConnectorConfig connectorConfig;
    private final MongoDbTaskContext taskContext;
    private final MongoDbOffsetContext previousOffset;
    private final ConnectionContext connectionContext;
    private final ReplicaSets replicaSets;
    private final EventDispatcher<CollectionId> dispatcher;
    protected final Clock clock;
    private final SnapshotProgressListener snapshotProgressListener;
    private final ErrorHandler errorHandler;
    private AtomicBoolean aborted = new AtomicBoolean(false);

    public MongoDbSnapshotChangeEventSource(MongoDbConnectorConfig connectorConfig, MongoDbTaskContext taskContext,
                                            ReplicaSets replicaSets, MongoDbOffsetContext previousOffset,
                                            EventDispatcher<CollectionId> dispatcher, Clock clock,
                                            SnapshotProgressListener snapshotProgressListener, ErrorHandler errorHandler) {
        super(connectorConfig, previousOffset, snapshotProgressListener);
        this.connectorConfig = connectorConfig;
        this.taskContext = taskContext;
        this.connectionContext = taskContext.getConnectionContext();
        this.previousOffset = previousOffset;
        this.replicaSets = replicaSets;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.snapshotProgressListener = snapshotProgressListener;
        this.errorHandler = errorHandler;
    }

    @Override
    protected SnapshotResult doExecute(ChangeEventSourceContext context, SnapshotContext snapshotContext, SnapshottingTask snapshottingTask)
            throws Exception {
        final MongoDbSnapshottingTask mongoDbSnapshottingTask = (MongoDbSnapshottingTask) snapshottingTask;
        final MongoDbSnapshotContext mongoDbSnapshotContext = (MongoDbSnapshotContext) snapshotContext;

        LOGGER.info("Snapshot step 1 - Preparing");
        snapshotProgressListener.snapshotStarted();

        if (previousOffset != null && previousOffset.isSnapshotRunning()) {
            LOGGER.info("Previous snapshot was cancelled before completion; a new snapshot will be taken.");
        }

        LOGGER.info("Snapshot step 2 - Determining snapshot offsets");
        determineSnapshotOffsets(mongoDbSnapshotContext, replicaSets);

        List<ReplicaSet> replicaSetsToSnapshot = mongoDbSnapshottingTask.getReplicaSetsToSnapshot();

        final int threads = replicaSetsToSnapshot.size();
        final ExecutorService executor = Threads.newFixedThreadPool(MongoDbConnector.class, taskContext.serverName(), "replicator-snapshot", threads);
        final CountDownLatch latch = new CountDownLatch(threads);

        LOGGER.info("Ignoring unnamed replica sets: {}", replicaSets.unnamedReplicaSets());
        LOGGER.info("Starting {} thread(s) to snapshot replica sets: {}", threads, replicaSetsToSnapshot);

        LOGGER.info("Snapshot step 3 - Snapshotting data");
        replicaSetsToSnapshot.forEach(replicaSet -> {
            executor.submit(() -> {
                try {
                    taskContext.configureLoggingContext(replicaSet.replicaSetName());
                    try {
                        snapshotReplicaSet(context, mongoDbSnapshotContext, replicaSet);
                    }
                    finally {
                        final MongoDbOffsetContext offset = (MongoDbOffsetContext) snapshotContext.offset;
                        // todo: DBZ-1726 - this causes MongoDbConnectorIT#shouldEmitHeartbeatMessages to fail
                        // omitted for now since it does not appear we did this in previous connector code.
                        // dispatcher.alwaysDispatchHeartbeatEvent(offset.getReplicaSetOffsetContext(replicaSet));
                    }
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

        // Shutdown executor and close connections
        try {
            executor.shutdown();
        }
        finally {
            LOGGER.info("Stopping mongodb connections");
            taskContext.getConnectionContext().shutdown();
        }

        if (aborted.get()) {
            return SnapshotResult.aborted();
        }

        snapshotProgressListener.snapshotCompleted();

        return SnapshotResult.completed(snapshotContext.offset);
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(OffsetContext previousOffset) {
        if (previousOffset == null) {
            LOGGER.info("No previous offset has been found");
            if (connectorConfig.getSnapshotMode().equals(MongoDbConnectorConfig.SnapshotMode.NEVER)) {
                LOGGER.info("According to the connector configuration, no snapshot will occur.");
                return new MongoDbSnapshottingTask(Collections.emptyList());
            }
            return new MongoDbSnapshottingTask(replicaSets.all());
        }

        // Even if there are previous offsets, if no snapshot should occur, return task with no replica sets
        if (connectorConfig.getSnapshotMode().equals(MongoDbConnectorConfig.SnapshotMode.NEVER)) {
            LOGGER.info("According to the connector configuration, no snapshot will occur.");
            return new MongoDbSnapshottingTask(Collections.emptyList());
        }

        // Collect which replica-sets require being snapshotted
        final List<ReplicaSet> replicaSetSnapshots = new ArrayList<>();
        final MongoDbOffsetContext offsetContext = (MongoDbOffsetContext) previousOffset;
        try {
            replicaSets.onEachReplicaSet(replicaSet -> {
                MongoPrimary primary = null;
                try {
                    primary = establishConnectionToPrimary(replicaSet);
                    final ReplicaSetOffsetContext rsOffsetContext = offsetContext.getReplicaSetOffsetContext(replicaSet);
                    if (primary != null && isSnapshotExpected(primary, rsOffsetContext)) {
                        replicaSetSnapshots.add(replicaSet);
                    }
                }
                finally {
                    if (primary != null) {
                        primary.stop();
                    }
                }
            });
        }
        finally {
            taskContext.getConnectionContext().shutdown();
        }

        return new MongoDbSnapshottingTask(replicaSetSnapshots);
    }

    @Override
    protected SnapshotContext prepare(ChangeEventSourceContext sourceContext) throws Exception {
        return new MongoDbSnapshotContext();
    }

    @Override
    protected void complete(SnapshotContext snapshotContext) {
    }

    private void snapshotReplicaSet(ChangeEventSourceContext sourceContext, MongoDbSnapshotContext ctx, ReplicaSet replicaSet) throws InterruptedException {
        MongoPrimary primaryClient = null;
        try {
            primaryClient = establishConnectionToPrimary(replicaSet);
            if (primaryClient != null) {
                createDataEvents(sourceContext, ctx, replicaSet, primaryClient);
            }
        }
        finally {
            if (primaryClient != null) {
                primaryClient.stop();
            }
        }
    }

    private MongoPrimary establishConnectionToPrimary(ReplicaSet replicaSet) {
        return connectionContext.primaryFor(replicaSet, taskContext.filters(), (desc, error) -> {
            // propagate authorization failures
            if (error.getMessage() != null && error.getMessage().startsWith(AUTHORIZATION_FAILURE_MESSAGE)) {
                throw new ConnectException("Error while attempting to " + desc, error);
            }
            else {
                dispatcher.dispatchConnectorEvent(new DisconnectEvent());
                LOGGER.error("Error while attempting to {}: ", desc, error.getMessage(), error);
                throw new ConnectException("Error while attempting to " + desc, error);
            }
        });
    }

    private boolean isSnapshotExpected(MongoPrimary primaryClient, ReplicaSetOffsetContext offsetContext) {
        boolean performSnapshot = true;
        if (offsetContext.hasOffset()) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Found existing offset for replica set '{}' at {}", offsetContext.getReplicaSetName(), offsetContext.getOffset());
            }
            performSnapshot = false;
            if (offsetContext.isSnapshotOngoing()) {
                // The latest snapshot was not completed, so restart it
                LOGGER.info("The previous snapshot was incomplete for '{}', so restarting the snapshot", offsetContext.getReplicaSetName());
                performSnapshot = true;
            }
            else {
                // todo: Right now we implement when needed snapshot by default. In the future we should provide the
                // same options as other connectors and this is where when_needed functionality would go.
                // There is no ongoing snapshot, so look to see if our last recorded offset still exists in the oplog.
                BsonTimestamp lastRecordedTs = offsetContext.lastOffsetTimestamp();
                BsonTimestamp firstAvailableTs = primaryClient.execute("get oplog position", primary -> {
                    MongoCollection<Document> oplog = primary.getDatabase("local").getCollection("oplog.rs");
                    Document firstEvent = oplog.find().sort(new Document("$natural", 1)).limit(1).first();
                    return SourceInfo.extractEventTimestamp(firstEvent);
                });

                if (firstAvailableTs == null) {
                    LOGGER.info("The oplog contains no entries, so performing snapshot of replica set '{}'", offsetContext.getReplicaSetName());
                    performSnapshot = true;
                }
                else if (lastRecordedTs.compareTo(firstAvailableTs) < 0) {
                    // The last recorded timestamp is *before* the first existing oplog event, which means there is
                    // almost certainly some history lost since the oplog was last processed.
                    LOGGER.info("Snapshot is required since the oplog for replica set '{}' starts at {}, which is later than the timestamp of the last offset {}",
                            offsetContext.getReplicaSetName(), firstAvailableTs, lastRecordedTs);
                    performSnapshot = true;
                }
                else {
                    // No snapshot required
                    LOGGER.info("The oplog contains the last entry previously read for '{}', so no snapshot will be performed",
                            offsetContext.getReplicaSetName());
                }
            }
        }
        else {
            LOGGER.info("No existing offset found for replica set '{}', starting snapshot", offsetContext.getReplicaSetName());
            performSnapshot = true;
        }

        return performSnapshot;
    }

    protected void determineSnapshotOffsets(MongoDbSnapshotContext ctx, ReplicaSets replicaSets) {
        final Map<ReplicaSet, Document> positions = new LinkedHashMap<>();
        replicaSets.onEachReplicaSet(replicaSet -> {
            LOGGER.info("Determine Snapshot Offset for replica-set {}", replicaSet.replicaSetName());
            MongoPrimary primaryClient = establishConnectionToPrimary(replicaSet);
            if (primaryClient != null) {
                try {
                    primaryClient.execute("get oplog position", primary -> {
                        MongoCollection<Document> oplog = primary.getDatabase("local").getCollection("oplog.rs");
                        Document last = oplog.find().sort(new Document("$natural", -1)).limit(1).first(); // may be null
                        positions.put(replicaSet, last);
                    });
                }
                finally {
                    LOGGER.info("Stopping primary client");
                    primaryClient.stop();
                }
            }
        });

        ctx.offset = new MongoDbOffsetContext(new SourceInfo(connectorConfig), new TransactionContext(), positions);
    }

    private void createDataEvents(ChangeEventSourceContext sourceContext, MongoDbSnapshotContext snapshotContext, ReplicaSet replicaSet,
                                  MongoPrimary primaryClient)
            throws InterruptedException {
        SnapshotReceiver snapshotReceiver = dispatcher.getSnapshotChangeEventReceiver();
        snapshotContext.offset.preSnapshotStart();

        createDataEventsForReplicaSet(sourceContext, snapshotContext, snapshotReceiver, replicaSet, primaryClient);

        snapshotContext.offset.preSnapshotCompletion();
        snapshotReceiver.completeSnapshot();
        snapshotContext.offset.postSnapshotCompletion();
    }

    /**
     * Dispatches the data change events for the records of a single replica-set.
     */
    private void createDataEventsForReplicaSet(ChangeEventSourceContext sourceContext, MongoDbSnapshotContext snapshotContext,
                                               SnapshotReceiver snapshotReceiver, ReplicaSet replicaSet, MongoPrimary primaryClient)
            throws InterruptedException {

        final String rsName = replicaSet.replicaSetName();

        final MongoDbOffsetContext offsetContext = (MongoDbOffsetContext) snapshotContext.offset;
        final ReplicaSetOffsetContext rsOffsetContext = offsetContext.getReplicaSetOffsetContext(replicaSet);

        snapshotContext.lastCollection = false;
        offsetContext.startReplicaSetSnapshot(replicaSet.replicaSetName());

        LOGGER.info("Beginning snapshot of '{}' at {}", rsName, rsOffsetContext.getOffset());

        final List<CollectionId> collections = determineDataCollectionsToBeSnapshotted(primaryClient.collections()).collect(Collectors.toList());
        snapshotProgressListener.monitoredDataCollectionsDetermined(collections);
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
                                    primaryClient);
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
                        primaryClient);
            }
        }

        offsetContext.stopReplicaSetSnapshot(replicaSet.replicaSetName());
    }

    private void createDataEventsForCollection(ChangeEventSourceContext sourceContext, MongoDbSnapshotContext snapshotContext, SnapshotReceiver snapshotReceiver,
                                               ReplicaSet replicaSet, CollectionId collectionId, MongoPrimary primaryClient)
            throws InterruptedException {

        long exportStart = clock.currentTimeInMillis();
        LOGGER.info("\t Exporting data for collection '{}'", collectionId);

        primaryClient.executeBlocking("sync '" + collectionId + "'", primary -> {
            final MongoDatabase database = primary.getDatabase(collectionId.dbName());
            final MongoCollection<Document> collection = database.getCollection(collectionId.name());

            final int batchSize = taskContext.getConnectorConfig().getSnapshotFetchSize();

            long docs = 0;
            Bson filterQuery = Document.parse(connectorConfig.getSnapshotFilterQueryForCollection(collectionId).orElseGet(() -> "{}"));

            try (MongoCursor<Document> cursor = collection.find(filterQuery).batchSize(batchSize).iterator()) {
                snapshotContext.lastRecordInCollection = false;
                if (cursor.hasNext()) {
                    while (cursor.hasNext()) {
                        if (!sourceContext.isRunning()) {
                            throw new InterruptedException("Interrupted while snapshotting collection " + collectionId.name());
                        }

                        Document document = cursor.next();
                        docs++;

                        snapshotContext.lastRecordInCollection = !cursor.hasNext();

                        if (snapshotContext.lastCollection && snapshotContext.lastRecordInCollection) {
                            snapshotContext.offset.markLastSnapshotRecord();
                        }

                        dispatcher.dispatchSnapshotEvent(collectionId, getChangeRecordEmitter(snapshotContext, collectionId, document, replicaSet), snapshotReceiver);
                    }
                }
                else if (snapshotContext.lastCollection) {
                    // if the last collection does not contain any records we still need to mark the last processed event as last one
                    snapshotContext.offset.markLastSnapshotRecord();
                }

                LOGGER.info("\t Finished snapshotting {} records for collection '{}'; total duration '{}'", docs, collectionId,
                        Strings.duration(clock.currentTimeInMillis() - exportStart));
                snapshotProgressListener.dataCollectionSnapshotCompleted(collectionId, docs);
            }
        });
    }

    protected ChangeRecordEmitter getChangeRecordEmitter(SnapshotContext snapshotContext, CollectionId collectionId, Document document, ReplicaSet replicaSet) {
        final MongoDbOffsetContext offsetContext = ((MongoDbOffsetContext) snapshotContext.offset);

        final ReplicaSetOffsetContext replicaSetOffsetContext = offsetContext.getReplicaSetOffsetContext(replicaSet);
        replicaSetOffsetContext.readEvent(collectionId, getClock().currentTime());

        return new MongoDbChangeRecordEmitter(replicaSetOffsetContext, getClock(), document, true);
    }

    protected Clock getClock() {
        return clock;
    }

    /**
     * A configuration describing the task to be performed during snapshotting.
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
    private static class MongoDbSnapshotContext extends SnapshotContext {
        public boolean lastCollection;
        public boolean lastRecordInCollection;
    }
}
