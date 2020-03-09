/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.time.Duration;
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

import org.apache.kafka.connect.errors.ConnectException;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import io.debezium.config.ConfigurationDefaults;
import io.debezium.connector.mongodb.ConnectionContext.MongoPrimary;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.EventDispatcher.SnapshotReceiver;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;

/**
 * A {@link SnapshotChangeEventSource} that performs multi-threaded snapshots of replica sets.
 *
 * @author Chris Cranford
 */
public class MongoDbSnapshotChangeEventSource implements SnapshotChangeEventSource {

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
    public SnapshotResult execute(ChangeEventSourceContext context) throws InterruptedException {
        SnapshottingTask snapshottingTask = getSnapshottingTask(previousOffset, replicaSets);
        if (!snapshottingTask.snapshotData()) {
            LOGGER.debug("Skipping snapshotting");
            return SnapshotResult.skipped(previousOffset);
        }

        delaySnapshotIfNeeded(context);

        final SnapshotContext ctx;
        try {
            ctx = prepare(context);
        }
        catch (Exception e) {
            LOGGER.error("Failed to initialize snapshot context.", e);
            throw new RuntimeException(e);
        }

        try {
            LOGGER.info("Snapshot step 1 - Preparing");
            snapshotProgressListener.snapshotStarted();

            if (previousOffset != null && previousOffset.isSnapshotRunning()) {
                LOGGER.info("Previous snapshot was cancelled before completion; a new snapshot will be taken.");
            }

            LOGGER.info("Snapshot step 2 - Determining snapshot offsets");
            determineSnapshotOffsets(ctx, replicaSets);

            List<ReplicaSet> replicaSetsToSnapshot = snapshottingTask.getReplicaSetsToSnapshot();

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
                            snapshotReplicaSet(context, ctx, replicaSet);
                        }
                        finally {
                            final MongoDbOffsetContext offset = (MongoDbOffsetContext) ctx.offset;
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

            return SnapshotResult.completed(ctx.offset);
        }
        catch (InterruptedException e) {
            LOGGER.warn("Snapshot was interrupted before completion");
            snapshotProgressListener.snapshotAborted();
            throw e;
        }
        catch (RuntimeException e) {
            snapshotProgressListener.snapshotAborted();
            throw e;
        }
        catch (Throwable t) {
            snapshotProgressListener.snapshotAborted();
            throw new RuntimeException(t);
        }
        finally {
            LOGGER.info("Snapshot step 4 - Finalizing");
            complete(ctx);
        }
    }

    protected SnapshottingTask getSnapshottingTask(OffsetContext previousOffset, ReplicaSets replicaSets) {
        if (previousOffset == null) {
            LOGGER.info("No previous offset has been found");
            if (connectorConfig.getSnapshotMode().equals(MongoDbConnectorConfig.SnapshotMode.NEVER)) {
                LOGGER.info("According to the connector configuration, no snapshot will occur.");
                return new SnapshottingTask(Collections.emptyList());
            }
            return new SnapshottingTask(replicaSets.all());
        }

        // Even if there are previous offsets, if no snapshot should occur, return task with no replica sets
        if (connectorConfig.getSnapshotMode().equals(MongoDbConnectorConfig.SnapshotMode.NEVER)) {
            LOGGER.info("According to the connector configuration, no snapshot will occur.");
            return new SnapshottingTask(Collections.emptyList());
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
                    if (primary != null && isInitialSyncExpected(primary, rsOffsetContext)) {
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

        return new SnapshottingTask(replicaSetSnapshots);
    }

    private void delaySnapshotIfNeeded(ChangeEventSourceContext context) throws InterruptedException {
        Duration snapshotDelay = connectorConfig.getSnapshotDelay();

        if (snapshotDelay.isZero() || snapshotDelay.isNegative()) {
            return;
        }

        Timer timer = Threads.timer(Clock.SYSTEM, snapshotDelay);
        Metronome metronome = Metronome.parker(ConfigurationDefaults.RETURN_CONTROL_INTERVAL, Clock.SYSTEM);

        while (!timer.expired()) {
            if (!context.isRunning()) {
                throw new InterruptedException("Interrupted while awaiting initial snapshot delay");
            }

            LOGGER.info("The connector will wait for {}s before proceeding", timer.remaining().getSeconds());
            metronome.pause();
        }
    }

    protected SnapshotContext prepare(ChangeEventSourceContext sourceContext) throws Exception {
        return new MongoDbSnapshotContext();
    }

    protected void complete(SnapshotContext snapshotContext) {
    }

    private void snapshotReplicaSet(ChangeEventSourceContext sourceContext, SnapshotContext ctx, ReplicaSet replicaSet) throws InterruptedException {
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
                LOGGER.error("Error while attempting to {}: ", desc, error.getMessage(), error);
                throw new ConnectException("Error while attempting to " + desc, error);
            }
        });
    }

    private boolean isInitialSyncExpected(MongoPrimary primaryClient, ReplicaSetOffsetContext offsetContext) {
        boolean performSnapshot = true;
        if (offsetContext.hasOffset()) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Found existing offset for replica set '{}' at {}", offsetContext.getReplicaSetName(), offsetContext.getOffset());
            }
            performSnapshot = false;
            if (connectionContext.performSnapshotEvenIfNotNeeded()) {
                LOGGER.info("Configured to perform initial sync of replica set '{}'", offsetContext.getReplicaSetName());
                performSnapshot = true;
            }
            else {
                if (offsetContext.isInitialSyncOngoing()) {
                    // The latest snapshot was not completed, so restart it
                    LOGGER.info("The previous initial sync was incomplete for '{}', so initiating another initial sync", offsetContext.getReplicaSetName());
                    performSnapshot = true;
                }
                else {
                    // There is no ongoing initial sync, so look to see if our last recorded offset still exists in the oplog.
                    BsonTimestamp lastRecordedTs = offsetContext.lastOffsetTimestamp();
                    BsonTimestamp firstAvailableTs = primaryClient.execute("get oplog position", primary -> {
                        MongoCollection<Document> oplog = primary.getDatabase("local").getCollection("oplog.rs");
                        Document firstEvent = oplog.find().sort(new Document("$natural", 1)).limit(1).first();
                        return SourceInfo.extractEventTimestamp(firstEvent);
                    });

                    if (firstAvailableTs == null) {
                        LOGGER.info("The oplog contains no entries, so performing initial sync of replica set '{}'", offsetContext.getReplicaSetName());
                        performSnapshot = true;
                    }
                    else if (lastRecordedTs.compareTo(firstAvailableTs) < 0) {
                        // The last recorded timestamp is *before* the first existing oplog event, which means there is
                        // almost certainly some history lost since the oplog was last processed.
                        LOGGER.info("Initial sync is required since the oplog for replica set '{}' starts at {}, which is later than the timestamp of the last offset {}",
                                offsetContext.getReplicaSetName(), firstAvailableTs, lastRecordedTs);
                        performSnapshot = true;
                    }
                    else {
                        // No initial sync required
                        LOGGER.info("The oplog contains the last entry previously read for '{}', so no initial sync will be performed",
                                offsetContext.getReplicaSetName());
                    }
                }
            }
        }
        else {
            LOGGER.info("No existing offset found for replica set '{}', starting initial sync", offsetContext.getReplicaSetName());
            performSnapshot = true;
        }

        return performSnapshot;
    }

    protected void determineSnapshotOffsets(SnapshotContext ctx, ReplicaSets replicaSets) throws Exception {
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

        ctx.offset = new MongoDbOffsetContext(connectorConfig, new SourceInfo(connectorConfig), new TransactionContext(), positions);
    }

    private void createDataEvents(ChangeEventSourceContext sourceContext, SnapshotContext snapshotContext, ReplicaSet replicaSet,
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
    private void createDataEventsForReplicaSet(ChangeEventSourceContext sourceContext, SnapshotContext snapshotContext,
                                               SnapshotReceiver snapshotReceiver, ReplicaSet replicaSet, MongoPrimary primaryClient)
            throws InterruptedException {

        final String rsName = replicaSet.replicaSetName();

        final MongoDbOffsetContext offsetContext = (MongoDbOffsetContext) snapshotContext.offset;
        final ReplicaSetOffsetContext rsOffsetContext = offsetContext.getReplicaSetOffsetContext(replicaSet);

        snapshotContext.lastCollection = false;
        offsetContext.startInitialSync(replicaSet.replicaSetName());

        LOGGER.info("Beginning initial sync of '{}' at {}", rsName, rsOffsetContext.getOffset());

        final List<CollectionId> collections = primaryClient.collections();
        if (connectionContext.maxNumberOfCopyThreads() > 1) {
            // Since multiple copy threads are to be used, create a thread pool and initiate the copy.
            // The current thread will wait until the copy threads either have completed or an error occurred.
            final int numThreads = Math.min(collections.size(), connectionContext.maxNumberOfCopyThreads());
            final Queue<CollectionId> collectionsToCopy = new ConcurrentLinkedQueue<>(collections);

            final String copyThreadName = "copy-" + (replicaSet.hasReplicaSetName() ? replicaSet.replicaSetName() : "main");
            final ExecutorService copyThreads = Threads.newFixedThreadPool(MongoDbConnector.class, taskContext.serverName(),
                    copyThreadName, connectionContext.maxNumberOfCopyThreads());
            final CountDownLatch latch = new CountDownLatch(numThreads);
            final AtomicBoolean aborted = new AtomicBoolean(false);
            final AtomicInteger threadCounter = new AtomicInteger(0);

            LOGGER.info("Preparing to use {} thread(s) to sync {} collection(s): {}", numThreads, collections.size(),
                    Strings.join(", ", collections));

            for (int i = 0; i < numThreads; ++i) {
                copyThreads.submit(() -> {
                    taskContext.configureLoggingContext(replicaSet.replicaSetName() + "-sync" + threadCounter.incrementAndGet());
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

            copyThreads.shutdown();
        }
        else {
            // Only 1 thread should be used for copying collections.
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

        offsetContext.stopInitialSync(replicaSet.replicaSetName());
    }

    private void createDataEventsForCollection(ChangeEventSourceContext sourceContext, SnapshotContext snapshotContext, SnapshotReceiver snapshotReceiver,
                                               ReplicaSet replicaSet, CollectionId collectionId, MongoPrimary primaryClient)
            throws InterruptedException {

        long exportStart = clock.currentTimeInMillis();
        LOGGER.info("\t Exporting data for collection '{}'", collectionId);

        primaryClient.executeBlocking("sync '" + collectionId + "'", primary -> {
            final MongoDatabase database = primary.getDatabase(collectionId.dbName());
            final MongoCollection<Document> collection = database.getCollection(collectionId.name());

            final int batchSize = taskContext.getConnectorConfig().getSnapshotFetchSize();

            long docs = 0;
            try (MongoCursor<Document> cursor = collection.find().batchSize(batchSize).iterator()) {
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

                LOGGER.info("\t Finished exporting {} records for collection '{}'; total duration '{}'", docs, collectionId,
                        Strings.duration(clock.currentTimeInMillis() - exportStart));
                snapshotProgressListener.dataCollectionSnapshotCompleted(collectionId, docs);
            }
        });
    }

    protected ChangeRecordEmitter getChangeRecordEmitter(SnapshotContext snapshotContext, CollectionId collectionId, Document document, ReplicaSet replicaSet) {
        final MongoDbOffsetContext offsetContext = ((MongoDbOffsetContext) snapshotContext.offset);

        final ReplicaSetOffsetContext replicaSetOffsetContext = offsetContext.getReplicaSetOffsetContext(replicaSet);
        replicaSetOffsetContext.readEvent(collectionId, getClock().currentTime());

        return new MongoDbChangeRecordEmitter(replicaSetOffsetContext, getClock(), document);
    }

    protected Clock getClock() {
        return clock;
    }

    /**
     * A configuration describing the task to be performed during snapshotting.
     * @see io.debezium.relational.RelationalSnapshotChangeEventSource.SnapshottingTask
     */
    public static class SnapshottingTask {

        private final List<ReplicaSet> replicaSetsToSnapshot;

        public SnapshottingTask(List<ReplicaSet> replicaSetsToSnapshot) {
            this.replicaSetsToSnapshot = replicaSetsToSnapshot;
        }

        /**
         * Whether data (rows in captured collections) should be snapshotted.
         * @return
         */
        public boolean snapshotData() {
            return !replicaSetsToSnapshot.isEmpty();
        }

        public List<ReplicaSet> getReplicaSetsToSnapshot() {
            return Collections.unmodifiableList(replicaSetsToSnapshot);
        }

        @Override
        public String toString() {
            return "SnapshottingTask [replicaSetsToSnapshot=" + replicaSetsToSnapshot + "]";
        }
    }

    /**
     * Mutable context which is populated in the course of snapshotting
     * @see io.debezium.relational.RelationalSnapshotChangeEventSource.SnapshotContext
     */
    public static class SnapshotContext implements AutoCloseable {
        public boolean lastCollection;
        public boolean lastRecordInCollection;
        public OffsetContext offset;

        public SnapshotContext() {
        }

        @Override
        public void close() throws Exception {
        }
    }

    /**
     * Mutable context that is populated in the course of snapshotting.
     */
    private static class MongoDbSnapshotContext extends SnapshotContext {
    }
}
