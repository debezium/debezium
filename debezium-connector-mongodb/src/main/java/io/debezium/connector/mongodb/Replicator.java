/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.mongodb.RecordMakers.RecordsForCollection;
import io.debezium.function.BlockingConsumer;
import io.debezium.function.BufferedBlockingConsumer;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import io.debezium.util.Threads;

/**
 * A component that replicates the content of a replica set, starting with an initial sync or continuing to read the oplog where
 * it last left off.
 *
 * <h2>Initial Sync</h2>
 * If no offsets have been recorded for this replica set, replication begins with an
 * <a href="https://docs.mongodb.com/manual/core/replica-set-sync/#initial-sync">MongoDB initial sync</a> of the replica set.
 * <p>
 * The logic used in this component to perform an initial sync is similar to that of the
 * <a href="https://github.com/mongodb/mongo/blob/master/src/mongo/db/repl/rs_initialsync.cpp">official initial sync</a>
 * functionality used by secondary MongoDB nodes, although our logic can be simpler since we are not storing state nor building
 * indexes:
 * <ol>
 * <li>Read the primary node's current oplog time. This is our <em>start time</em>.</li>
 * <li>Clone all of the databases and collections from the primary node, using multiple threads. This steps is completed only
 * after <em>all</em> collections are successfully copied.</li>
 * <li>Start reading the primary node's oplog from <em>start time</em> and applying the changes.</li>
 * </ol>
 * <p>
 * It is important to understand that step 2 is not performing a consistent snapshot. That means that once we start copying a
 * collection, clients can make changes and we may or may not see those changes in our copy. However, this is not a problem
 * because the MongoDB replication process -- and our logic -- relies upon the fact that every change recorded in the MongoDB
 * oplog is <a href="https://docs.mongodb.com/manual/core/replica-set-oplog/">idempotent</a>. So, as long as we read the oplog
 * from the same point in time (or earlier) than we <em>started</em> our copy operation, and apply <em>all</em> of the changes
 * <em>in the same order</em>, then the state of all documents described by this connector will be the same.
 *
 * <h2>Restart</h2>
 * If prior runs of the replicator have recorded offsets in the {@link MongoDbTaskContext#source() source information}, then
 * when the replicator starts it will simply start reading the primary's oplog starting at the same point it last left off.
 *
 * <h2>Handling problems</h2>
 * <p>
 * This replicator does each of its tasks using a connection to the primary. If the replicator is not able to establish a
 * connection to the primary (e.g., there is no primary, or the replicator cannot communicate with the primary), the replicator
 * will continue to try to establish a connection, using an exponential back-off strategy to prevent saturating the system.
 * After a {@link ConnectionContext#maxConnectionAttemptsForPrimary() configurable} number of failed attempts, the replicator
 * will fail by throwing a {@link ConnectException}.
 *
 * @author Randall Hauch
 */
@ThreadSafe
public class Replicator {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String AUTHORIZATION_FAILURE_MESSAGE = "Command failed with error 13";

    private final MongoDbTaskContext context;
    private final ExecutorService copyThreads;
    private final ReplicaSet replicaSet;
    private final String rsName;
    private final AtomicBoolean running = new AtomicBoolean();
    private final SourceInfo source;
    private final RecordMakers recordMakers;
    private final BufferableRecorder bufferedRecorder;
    private final Clock clock;
    private ConnectionContext.MongoPrimary primaryClient;
    private final Consumer<Throwable> onFailure;

    /**
     * @param context the replication context; may not be null
     * @param replicaSet the replica set to be replicated; may not be null
     * @param recorder the recorder for source record produced by this replicator; may not be null
     * @param onFailure listener of exceptions thrown by replicator task
     */
    public Replicator(MongoDbTaskContext context, ReplicaSet replicaSet, BlockingConsumer<SourceRecord> recorder, Consumer<Throwable> onFailure) {
        assert context != null;
        assert replicaSet != null;
        assert recorder != null;
        this.context = context;
        this.source = context.source();
        this.replicaSet = replicaSet;
        this.rsName = replicaSet.replicaSetName();
        final String copyThreadName = "copy-" + (replicaSet.hasReplicaSetName() ? replicaSet.replicaSetName() : "main");
        this.copyThreads = Threads.newFixedThreadPool(MongoDbConnector.class, context.serverName(), copyThreadName, context.getConnectionContext().maxNumberOfCopyThreads());
        this.bufferedRecorder = new BufferableRecorder(recorder);
        this.recordMakers = new RecordMakers(context.filters(), this.source, context.topicSelector(), this.bufferedRecorder, context.isEmitTombstoneOnDelete());
        this.clock = this.context.getClock();
        this.onFailure = onFailure;
    }

    /**
     * Stop the replication from running.
     * <p>
     * This method does nothing if the snapshot is not running
     */
    public void stop() {
        this.copyThreads.shutdownNow();
        this.running.set(false);
    }

    /**
     * Perform the replication logic. This can be run once.
     */
    public void run() {
        if (this.running.compareAndSet(false, true)) {
            try {
                if (establishConnectionToPrimary()) {
                    if (isInitialSyncExpected()) {
                        recordCurrentOplogPosition();
                        if (!performInitialSync()) {
                            return;
                        }
                    }
                    readOplog();
                }
            }
            catch (Throwable t) {
                logger.error("Replicator for replica set {} failed", rsName, t);
                onFailure.accept(t);
            }
            finally {
                if (primaryClient != null) {
                    primaryClient.stop();
                }
                this.running.set(false);
            }
        }
    }

    /**
     * Establish a connection to the primary.
     *
     * @return {@code true} if a connection was established, or {@code false} otherwise
     */
    protected boolean establishConnectionToPrimary() {
        logger.info("Connecting to '{}'", replicaSet);
        primaryClient = context.getConnectionContext().primaryFor(
                replicaSet,
                context.filters(),
                (desc, error) -> {
                    // propagate authorization failures
                    if (error.getMessage() != null && error.getMessage().startsWith(AUTHORIZATION_FAILURE_MESSAGE)) {
                        throw new ConnectException("Error while attempting to " + desc, error);
                    }
                    else {
                        logger.error("Error while attempting to {}: {}", desc, error.getMessage(), error);
                    }
                });

        return primaryClient != null;
    }

    /**
     * Obtain the current position of the oplog, and record it in the source.
     */
    protected void recordCurrentOplogPosition() {
        primaryClient.execute("get oplog position", primary -> {
            MongoCollection<Document> oplog = primary.getDatabase("local").getCollection("oplog.rs");
            Document last = oplog.find().sort(new Document("$natural", -1)).limit(1).first(); // may be null
            source.offsetStructForEvent(replicaSet.replicaSetName(), last);
        });
    }

    /**
     * Determine if an initial sync should be performed. An initial sync is expected if the {@link #source} has no previously
     * recorded offsets for this replica set, or if {@link ConnectionContext#performSnapshotEvenIfNotNeeded() a snapshot should
     * always be performed}.
     *
     * @return {@code true} if the initial sync should be performed, or {@code false} otherwise
     */
    protected boolean isInitialSyncExpected() {
        boolean performSnapshot = true;
        if (source.hasOffset(rsName)) {
            logger.info("Found existing offset for replica set '{}' at {}", rsName, source.lastOffset(rsName));
            performSnapshot = false;
            if (context.getConnectionContext().performSnapshotEvenIfNotNeeded()) {
                logger.info("Configured to performing initial sync of replica set '{}'", rsName);
                performSnapshot = true;
            } else {
                if (source.isInitialSyncOngoing(rsName)) {
                    // The last snapshot was not completed, so do it again ...
                    logger.info("The previous initial sync was incomplete for '{}', so initiating another initial sync", rsName);
                    performSnapshot = true;
                } else {
                    // There is no ongoing initial sync, so look to see if our last recorded offset still exists in the oplog.
                    BsonTimestamp lastRecordedTs = source.lastOffsetTimestamp(rsName);

                    BsonTimestamp firstAvailableTs = primaryClient.execute("get oplog position", primary -> {
                        MongoCollection<Document> oplog = primary.getDatabase("local").getCollection("oplog.rs");
                        Document firstEvent = oplog.find().sort(new Document("$natural", 1)).limit(1).first(); // may be null
                        return SourceInfo.extractEventTimestamp(firstEvent);
                    });

                    if (firstAvailableTs == null) {
                        logger.info("The oplog contains no entries, so performing initial sync of replica set '{}'", rsName);
                        performSnapshot = true;
                    } else if (lastRecordedTs.compareTo(firstAvailableTs) < 0) {
                        // The last recorded timestamp is *before* the first existing oplog event, which means there is
                        // almost certainly some history lost since we last processed the oplog ...
                        logger.info("Initial sync is required since the oplog for replica set '{}' starts at {}, which is later than the timestamp of the last offset {}",
                                    rsName, firstAvailableTs, lastRecordedTs);
                        performSnapshot = true;
                    } else {
                        // Otherwise we'll not perform an initial sync
                        logger.info("The oplog contains the last entry previously read for '{}', so no initial sync will be performed",
                                    rsName);
                    }
                }
            }
        } else {
            logger.info("No existing offset found for replica set '{}', starting initial sync", rsName);
            performSnapshot = true;
        }
        return performSnapshot;
    }

    /**
     * Perform the initial sync of the collections in the replica set.
     *
     * @return {@code true} if the initial sync was completed, or {@code false} if it was stopped for any reason
     */
    protected boolean performInitialSync() {
        logger.info("Beginning initial sync of '{}' at {}", rsName, source.lastOffset(rsName));
        source.startInitialSync(replicaSet.replicaSetName());

        // Set up our recorder to buffer the last record ...
        try {
            bufferedRecorder.startBuffering();
        } catch (InterruptedException e) {
            // Do nothing so that this thread is terminated ...
            logger.info("Interrupted while waiting to flush the buffer before starting an initial sync of '{}'", rsName);
            return false;
        }

        // Get the current timestamp of this processor ...
        final long syncStart = clock.currentTimeInMillis();

        // We need to copy each collection, so put the collection IDs into a queue ...
        final List<CollectionId> collections = primaryClient.collections();
        final Queue<CollectionId> collectionsToCopy = new ConcurrentLinkedQueue<>(collections);
        final int numThreads = Math.min(collections.size(), context.getConnectionContext().maxNumberOfCopyThreads());
        final CountDownLatch latch = new CountDownLatch(numThreads);
        final AtomicBoolean aborted = new AtomicBoolean(false);
        final AtomicInteger replicatorThreadCounter = new AtomicInteger(0);
        final AtomicInteger numCollectionsCopied = new AtomicInteger();
        final AtomicLong numDocumentsCopied = new AtomicLong();

        // And start threads to pull collection IDs from the queue and perform the copies ...
        logger.info("Preparing to use {} thread(s) to sync {} collection(s): {}",
                    numThreads, collections.size(), Strings.join(", ", collections));
        for (int i = 0; i != numThreads; ++i) {
            copyThreads.submit(() -> {
                context.configureLoggingContext(replicaSet.replicaSetName() + "-sync" + replicatorThreadCounter.incrementAndGet());
                // Continue to pull a collection ID and copy the collection ...
                try {
                    CollectionId id = null;
                    while (!aborted.get() && (id = collectionsToCopy.poll()) != null) {
                        long start = clock.currentTimeInMillis();
                        logger.info("Starting initial sync of '{}'", id);
                        long numDocs = copyCollection(id, syncStart);
                        numCollectionsCopied.incrementAndGet();
                        numDocumentsCopied.addAndGet(numDocs);
                        long duration = clock.currentTimeInMillis() - start;
                        logger.info("Completing initial sync of {} documents from '{}' in {}", numDocs, id, Strings.duration(duration));
                    }
                } catch (InterruptedException e) {
                    // Do nothing so that this thread is terminated ...
                    aborted.set(true);
                } finally {
                    latch.countDown();
                }
            });
        }

        // Wait for all of the threads to complete ...
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.interrupted();
            aborted.set(true);
        }
        this.copyThreads.shutdown();

        // Stopping the replicator does not interrupt *our* thread but does interrupt the copy threads.
        // Therefore, check the aborted state here ...
        long syncDuration = clock.currentTimeInMillis() - syncStart;
        if (aborted.get()) {
            int remaining = collections.size() - numCollectionsCopied.get();
            logger.info("Initial sync aborted after {} with {} of {} collections incomplete",
                        Strings.duration(syncDuration), remaining, collections.size());
            return false;
        }

        // We completed the initial sync, so record this in the source ...
        source.stopInitialSync(replicaSet.replicaSetName());
        try {
            // And immediately flush the last buffered source record with the updated offset ...
            bufferedRecorder.stopBuffering(source.lastOffset(rsName));
        } catch (InterruptedException e) {
            logger.info("Interrupted while waiting for last initial sync record from replica set '{}' to be recorded", rsName);
            return false;
        }

        logger.info("Initial sync of {} collections with a total of {} documents completed in {}",
                    collections.size(), numDocumentsCopied.get(), Strings.duration(syncDuration));
        return true;
    }

    /**
     * Copy the collection, sending to the recorder a record for each document.
     *
     * @param collectionId the identifier of the collection to be copied; may not be null
     * @param timestamp the timestamp in milliseconds at which the copy operation was started
     * @return number of documents that were copied
     * @throws InterruptedException if the thread was interrupted while the copy operation was running
     */
    protected long copyCollection(CollectionId collectionId, long timestamp) throws InterruptedException {
        AtomicLong docCount = new AtomicLong();
        primaryClient.executeBlocking("sync '" + collectionId + "'", primary -> {
            docCount.set(copyCollection(primary, collectionId, timestamp));
        });
        return docCount.get();
    }

    /**
     * Copy the collection, sending to the recorder a record for each document.
     *
     * @param primary the connection to the replica set's primary node; may not be null
     * @param collectionId the identifier of the collection to be copied; may not be null
     * @param timestamp the timestamp in milliseconds at which the copy operation was started
     * @return number of documents that were copied
     * @throws InterruptedException if the thread was interrupted while the copy operation was running
     */
    protected long copyCollection(MongoClient primary, CollectionId collectionId, long timestamp) throws InterruptedException {
        RecordsForCollection factory = recordMakers.forCollection(collectionId);
        MongoDatabase db = primary.getDatabase(collectionId.dbName());
        MongoCollection<Document> docCollection = db.getCollection(collectionId.name());
        long counter = 0;
        try (MongoCursor<Document> cursor = docCollection.find().iterator()) {
            while (running.get() && cursor.hasNext()) {
                Document doc = cursor.next();
                logger.trace("Found existing doc in {}: {}", collectionId, doc);
                counter += factory.recordObject(collectionId, doc, timestamp);
            }
        }
        return counter;
    }

    /**
     * Repeatedly obtain a connection to the replica set's current primary and use that primary to read the oplog.
     * This method will continue to run even if there are errors or problems. The method will return when a sufficient
     * number of errors occur or if the replicator should stop reading the oplog. The latter occurs when a new primary
     * is elected (as identified by an oplog event), of if the current thread doing the reading is interrupted.
     */
    protected void readOplog() {
        primaryClient.execute("read from oplog on '" + replicaSet + "'", (Consumer<MongoClient>)this::readOplog);
    }

    /**
     * Use the given primary to read the oplog.
     *
     * @param primary the connection to the replica set's primary node; may not be null
     */
    protected void readOplog(MongoClient primary) {
        BsonTimestamp oplogStart = source.lastOffsetTimestamp(replicaSet.replicaSetName());
        logger.info("Reading oplog for '{}' primary {} starting at {}", replicaSet, primary.getAddress(), oplogStart);

        // Include none of the cluster-internal operations and only those events since the previous timestamp ...
        MongoCollection<Document> oplog = primary.getDatabase("local").getCollection("oplog.rs");
        Bson filter = Filters.and(Filters.gt("ts", oplogStart), // start just after our last position
                                  Filters.exists("fromMigrate", false)); // skip internal movements across shards
        FindIterable<Document> results = oplog.find(filter)
                                              .sort(new Document("$natural", 1)) // force forwards collection scan
                                              .oplogReplay(true) // tells Mongo to not rely on indexes
                                              .cursorType(CursorType.TailableAwait); // tail and await new data
        // Read as much of the oplog as we can ...
        ServerAddress primaryAddress = primary.getAddress();
        try (MongoCursor<Document> cursor = results.iterator()) {
            while (running.get() && cursor.hasNext()) {
                if (!handleOplogEvent(primaryAddress, cursor.next())) {
                    // Something happened, and we're supposed to stop reading
                    return;
                }
            }
        }
    }

    /**
     * Handle a single oplog event.
     *
     * @param primaryAddress the address of the primary server from which the event was obtained; may not be null
     * @param event the oplog event; may not be null
     * @return {@code true} if additional events should be processed, or {@code false} if the caller should stop
     *         processing events
     */
    protected boolean handleOplogEvent(ServerAddress primaryAddress, Document event) {
        logger.debug("Found event: {}", event);
        String ns = event.getString("ns");
        Document object = event.get("o", Document.class);
        if (object == null) {
            logger.warn("Missing 'o' field in event, so skipping {}", event.toJson());
            return true;
        }
        if (ns == null || ns.isEmpty()) {
            // These are replica set events ...
            String msg = object.getString("msg");
            if ("new primary".equals(msg)) {
                AtomicReference<ServerAddress> address = new AtomicReference<>();
                try {
                    primaryClient.executeBlocking("conn", mongoClient -> {
                        ServerAddress currentPrimary = mongoClient.getAddress();
                        address.set(currentPrimary);
                    });
                } catch (InterruptedException e) {
                    logger.error("Get current primary executeBlocking", e);
                }
                ServerAddress serverAddress = address.get();

                if (serverAddress != null && !serverAddress.equals(primaryAddress)) {
                    //primary switch will be handled automatically by mongo driver.
                    logger.info("Found new primary event in oplog, so stopping use of {} to continue with new primary {}",
                            primaryAddress, serverAddress);
                } else {
                    logger.info("Found new primary event in oplog, current {} is new primary. " +
                                "Continue to process oplog event.", primaryAddress);
                }
            }
            // Otherwise, ignore this event ...
            logger.debug("Skipping event with no namespace: {}", event.toJson());
            return true;
        }
        int delimIndex = ns.indexOf('.');
        if (delimIndex > 0) {
            assert (delimIndex + 1) < ns.length();
            String dbName = ns.substring(0, delimIndex);
            String collectionName = ns.substring(delimIndex + 1);
            if ("$cmd".equals(collectionName)) {
                // This is a command on a database ...
                // TODO: Probably want to handle some of these when we track creation/removal of collections
                logger.debug("Skipping database command event: {}", event.toJson());
                return true;
            }
            // Otherwise, it is an event on a document in a collection ...
            if (!context.filters().databaseFilter().test(dbName)){
                logger.debug("Skipping the event for database {} based on database.whitelist");
                return true;
            }
            CollectionId collectionId = new CollectionId(rsName, dbName, collectionName);
            if (context.filters().collectionFilter().test(collectionId)) {
                RecordsForCollection factory = recordMakers.forCollection(collectionId);
                try {
                    factory.recordEvent(event, clock.currentTimeInMillis());
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * A {@link BlockingConsumer BlockingConsumer<SourceRecord>} implementation that will buffer the last source record
     * only during an initial sync, so that when the initial sync is complete the last record's offset can be updated
     * to reflect the completion of the initial sync before it is flushed.
     */
    protected final class BufferableRecorder implements BlockingConsumer<SourceRecord> {
        private final BlockingConsumer<SourceRecord> actual;
        private BufferedBlockingConsumer<SourceRecord> buffered;
        private volatile BlockingConsumer<SourceRecord> current;

        public BufferableRecorder(BlockingConsumer<SourceRecord> actual) {
            this.actual = actual;
            this.current = this.actual;
        }

        /**
         * Start buffering the most recently source record so it can be updated before the {@link #stopBuffering(Map) initial sync
         * is completed}.
         *
         * @throws InterruptedException if the thread is interrupted while waiting for any existing record to be flushed
         */
        protected synchronized void startBuffering() throws InterruptedException {
            this.buffered = BufferedBlockingConsumer.bufferLast(actual);
            this.current = this.buffered;
        }

        /**
         * Stop buffering source records, and flush any buffered records by replacing their offset with the provided offset.
         * Note that this only replaces the record's {@link SourceRecord#sourceOffset() offset} and does not change the
         * value of the record, which may contain information about the snapshot.
         *
         * @param newOffset the offset that reflects that the snapshot has been completed; may not be null
         * @throws InterruptedException if the thread is interrupted while waiting for the new record to be flushed
         */
        protected synchronized void stopBuffering(Map<String, ?> newOffset) throws InterruptedException {
            assert newOffset != null;
            this.buffered.close(record -> {
                if (record == null) return null;
                return new SourceRecord(record.sourcePartition(),
                        newOffset,
                        record.topic(),
                        record.kafkaPartition(),
                        record.keySchema(),
                        record.key(),
                        record.valueSchema(),
                        record.value());
            });
            this.current = this.actual;
        }

        @Override
        public void accept(SourceRecord t) throws InterruptedException {
            this.current.accept(t);
        }
    }
}
