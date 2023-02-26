/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.errors.ConnectException;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.selector.ReadPreferenceServerSelector;

import io.debezium.connector.mongodb.ConnectionContext.MongoPreferredNode;
import io.debezium.connector.mongodb.recordemitter.MongoDbChangeRecordEmitter;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.RateLimiter;
import io.debezium.util.Threads;

/**
 * @author Chris Cranford
 */
public class MongoDbStreamingChangeEventSource implements StreamingChangeEventSource<MongoDbPartition, MongoDbOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbStreamingChangeEventSource.class);

    private static final String AUTHORIZATION_FAILURE_MESSAGE = "Command failed with error 13";

    private static final String OPERATION_FIELD = "op";
    private static final String OBJECT_FIELD = "o";
    private static final String OPERATION_CONTROL = "c";
    private static final String TX_OPS = "applyOps";

    private final MongoDbConnectorConfig connectorConfig;
    private final EventDispatcher<MongoDbPartition, CollectionId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final ConnectionContext connectionContext;
    private final ReplicaSets replicaSets;
    private final MongoDbTaskContext taskContext;

    public MongoDbStreamingChangeEventSource(MongoDbConnectorConfig connectorConfig, MongoDbTaskContext taskContext,
                                             ReplicaSets replicaSets,
                                             EventDispatcher<MongoDbPartition, CollectionId> dispatcher,
                                             ErrorHandler errorHandler, Clock clock) {
        this.connectorConfig = connectorConfig;
        this.connectionContext = taskContext.getConnectionContext();
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.replicaSets = replicaSets;
        this.taskContext = taskContext;
    }

    @Override
    public void execute(ChangeEventSourceContext context, MongoDbPartition partition, MongoDbOffsetContext offsetContext)
            throws InterruptedException {
        final List<ReplicaSet> validReplicaSets = replicaSets.validReplicaSets();

        if (offsetContext == null) {
            offsetContext = initializeOffsets(connectorConfig, partition, replicaSets);
        }

        try {
            if (validReplicaSets.size() == 1) {
                // Streams the replica-set changes in the current thread
                streamChangesForReplicaSet(context, partition, validReplicaSets.get(0), offsetContext);
            }
            else if (validReplicaSets.size() > 1) {
                // Starts a thread for each replica-set and executes the streaming process
                streamChangesForReplicaSets(context, partition, validReplicaSets, offsetContext);
            }
        }
        finally {
            taskContext.getConnectionContext().shutdown();
        }
    }

    private void streamChangesForReplicaSet(ChangeEventSourceContext context, MongoDbPartition partition,
                                            ReplicaSet replicaSet, MongoDbOffsetContext offsetContext) {
        MongoPreferredNode mongo = null;
        try {
            mongo = establishConnection(partition, replicaSet, ReadPreference.secondaryPreferred());
            if (mongo != null) {
                final AtomicReference<MongoPreferredNode> mongoReference = new AtomicReference<>(mongo);
                mongo.execute("read from change stream on '" + replicaSet + "'", client -> {
                    readChangeStream(client, mongoReference.get(), replicaSet, context, offsetContext);
                });
            }
        }
        catch (Throwable t) {
            LOGGER.error("Streaming for replica set {} failed", replicaSet.replicaSetName(), t);
            errorHandler.setProducerThrowable(t);
        }
        finally {
            if (mongo != null) {
                mongo.stop();
            }
        }
    }

    private void streamChangesForReplicaSets(ChangeEventSourceContext context, MongoDbPartition partition,
                                             List<ReplicaSet> replicaSets, MongoDbOffsetContext offsetContext) {
        final int threads = replicaSets.size();
        final ExecutorService executor = Threads.newFixedThreadPool(MongoDbConnector.class, taskContext.serverName(), "replicator-streaming", threads);
        final CountDownLatch latch = new CountDownLatch(threads);

        LOGGER.info("Starting {} thread(s) to stream changes for replica sets: {}", threads, replicaSets);

        replicaSets.forEach(replicaSet -> {
            executor.submit(() -> {
                try {
                    streamChangesForReplicaSet(context, partition, replicaSet, offsetContext);
                }
                finally {
                    latch.countDown();
                }
            });
        });

        // Wait for the executor service to terminate.
        try {
            latch.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        executor.shutdown();
    }

    private MongoPreferredNode establishConnection(MongoDbPartition partition, ReplicaSet replicaSet, ReadPreference preference) {
        return connectionContext.preferredFor(replicaSet, preference, taskContext.filters(), (desc, error) -> {
            // propagate authorization failures
            if (error.getMessage() != null && error.getMessage().startsWith(AUTHORIZATION_FAILURE_MESSAGE)) {
                throw new ConnectException("Error while attempting to " + desc, error);
            }
            else {
                dispatcher.dispatchConnectorEvent(partition, new DisconnectEvent());
                LOGGER.error("Error while attempting to {}: {}", desc, error.getMessage(), error);
                throw new ConnectException("Error while attempting to " + desc, error);
            }
        });
    }

    private void readChangeStream(MongoClient client, MongoPreferredNode mongo, ReplicaSet replicaSet, ChangeEventSourceContext context,
                                  MongoDbOffsetContext offsetContext) {
        final ReplicaSetPartition rsPartition = offsetContext.getReplicaSetPartition(replicaSet);
        final ReplicaSetOffsetContext rsOffsetContext = offsetContext.getReplicaSetOffsetContext(replicaSet);

        final BsonTimestamp oplogStart = rsOffsetContext.lastOffsetTimestamp();

        ReplicaSetChangeStreamsContext oplogContext = new ReplicaSetChangeStreamsContext(rsPartition, rsOffsetContext, mongo, replicaSet);

        final ServerAddress nodeAddress = MongoUtil.getPreferredAddress(client, mongo.getPreference());
        LOGGER.info("Reading change stream for '{}'/{} from {} starting at {}", replicaSet, mongo.getPreference().getName(), nodeAddress, oplogStart);

        var rsChangeStream = createChangeStream(client, rsOffsetContext);
        var cursor = rsChangeStream.cursor();

        // Once every 10 seconds
        var preferenceRateLimiter = new RateLimiter(1, 0.1F);
        try {
            // In Replicator, this used cursor.hasNext() but this is a blocking call and I observed that this can
            // delay the shutdown of the connector by up to 15 seconds or longer. By introducing a Metronome, we
            // can respond to the stop request much faster and without much overhead.
            Metronome pause = Metronome.sleeper(Duration.ofMillis(500), clock);
            while (context.isRunning()) {
                // Use tryNext which will return null if no document is yet available from the cursor.
                // In this situation if not document is available, we'll pause.
                final ChangeStreamDocument<BsonDocument> event = cursor.tryNext();
                if (event != null) {
                    LOGGER.trace("Arrived Change Stream event: {}", event);

                    oplogContext.getOffset().changeStreamEvent(event);
                    oplogContext.getOffset().getOffset();
                    CollectionId collectionId = new CollectionId(
                            replicaSet.replicaSetName(),
                            event.getNamespace().getDatabaseName(),
                            event.getNamespace().getCollectionName());

                    try {
                        // Note that this will trigger a heartbeat request
                        dispatcher.dispatchDataChangeEvent(
                                oplogContext.getPartition(),
                                collectionId,
                                new MongoDbChangeRecordEmitter(
                                        oplogContext.getPartition(),
                                        oplogContext.getOffset(),
                                        clock,
                                        event));
                    }
                    catch (Exception e) {
                        errorHandler.setProducerThrowable(e);
                        return;
                    }
                }
                else {
                    // No event was returned, so trigger a heartbeat
                    try {
                        // Guard against `null` to be protective of issues like SERVER-63772, and situations called out in the Javadocs:
                        // > resume token [...] can be null if the cursor has either not been iterated yet, or the cursor is closed.
                        if (cursor.getResumeToken() != null) {
                            oplogContext.getOffset().noEvent(cursor);
                            dispatcher.dispatchHeartbeatEvent(oplogContext.getPartition(), oplogContext.getOffset());
                        }
                    }
                    catch (InterruptedException e) {
                        LOGGER.info("Replicator thread is interrupted");
                        Thread.currentThread().interrupt();
                        return;
                    }

                    try {
                        pause.pause();
                    }
                    catch (InterruptedException e) {
                        break;
                    }
                }

                if (taskContext.getConnectorConfig().isCursorReadPreferenceMonitoringEnabled() && preferenceRateLimiter.tryConsume(1)
                        && !isSelectedReadPreference(client, mongo.getPreference(), cursor)) {
                    LOGGER.info("Closing and recreating cursor due to election invalidating read preference");
                    cursor.close();
                    // Note: There is an edge case currently unaccounted for when an election happens during the first iteration of the cursor.
                    // In this case, we need to set resume token in rsOffsetContext for this to skip the initial snapshot last timestamp.
                    // But this is currently immutable.
                    rsChangeStream = createChangeStream(client, rsOffsetContext);
                    cursor = rsChangeStream.cursor();
                }
            }
        }
        finally {
            cursor.close();
        }
    }

    private ChangeStreamIterable<BsonDocument> createChangeStream(MongoClient client, ReplicaSetOffsetContext rsOffsetContext) {
        final List<Bson> pipeline = new ChangeStreamPipelineFactory(rsOffsetContext, taskContext.getConnectorConfig(), taskContext.filters().getConfig()).create();

        final ChangeStreamIterable<BsonDocument> rsChangeStream = client.watch(pipeline, BsonDocument.class);
        if (taskContext.getCaptureMode().isFullUpdate()) {
            rsChangeStream.fullDocument(FullDocument.UPDATE_LOOKUP);
        }
        if (taskContext.getCaptureMode().isIncludePreImage()) {
            rsChangeStream.fullDocumentBeforeChange(FullDocumentBeforeChange.WHEN_AVAILABLE);
        }

        if (rsOffsetContext.lastResumeToken() != null) {
            LOGGER.info("Resuming streaming from token '{}'", rsOffsetContext.lastResumeToken());
            rsChangeStream.resumeAfter(ResumeTokens.fromData(rsOffsetContext.lastResumeToken()));
        }
        else if (rsOffsetContext.lastOffsetTimestamp().getTime() > 0) {
            LOGGER.info("Resume token not available, starting streaming from time '{}'", rsOffsetContext.lastOffsetTimestamp());
            rsChangeStream.startAtOperationTime(rsOffsetContext.lastOffsetTimestamp());
        }

        if (connectorConfig.getCursorMaxAwaitTime() > 0) {
            rsChangeStream.maxAwaitTime(connectorConfig.getCursorMaxAwaitTime(), TimeUnit.MILLISECONDS);
        }
        return rsChangeStream;
    }

    private static boolean isSelectedReadPreference(MongoClient client, ReadPreference readPreference,
                                                    MongoCursor<?> cursor) {
        // Find all remaining nodes that match our preference
        var candidates = new ReadPreferenceServerSelector(readPreference)
                .select(client.getClusterDescription());

        // Determine if the cursor matches any one of these candidates
        return candidates.stream()
                .map(ServerDescription::getAddress)
                .anyMatch(preferredAddress -> {
                    // TODO: In general, the host names returned between commands can be different (DNS or IP potentially).
                    // For example, we see test-mongo1:27017 (defined in rs.init()) and e253ef34bd93:27017 (the container’s hostname).
                    // Thus, canonicalization must be performed when making comparison.
                    var cursorAddress = cursor.getServerCursor() == null ? null : cursor.getServerCursor().getAddress();
                    return preferredAddress.equals(cursorAddress);
                });
    }

    protected MongoDbOffsetContext initializeOffsets(MongoDbConnectorConfig connectorConfig, MongoDbPartition partition,
                                                     ReplicaSets replicaSets) {
        final Map<ReplicaSet, BsonDocument> positions = new LinkedHashMap<>();
        replicaSets.onEachReplicaSet(replicaSet -> {
            LOGGER.info("Determine Snapshot Offset for replica-set {}", replicaSet.replicaSetName());
            MongoPreferredNode mongo = establishConnection(partition, replicaSet, ReadPreference.primaryPreferred());
            if (mongo != null) {
                try {
                    mongo.execute("get oplog position", client -> {
                        positions.put(replicaSet, MongoUtil.getOplogEntry(client, -1, LOGGER));
                    });
                }
                finally {
                    LOGGER.info("Stopping primary client");
                    mongo.stop();
                }
            }
        });

        return new MongoDbOffsetContext(new SourceInfo(connectorConfig), new TransactionContext(),
                new MongoDbIncrementalSnapshotContext<>(false), positions);
    }

    /**
     * A context associated with a given replica set oplog read operation.
     */
    private static class ReplicaSetChangeStreamsContext {
        private final ReplicaSetPartition partition;
        private final ReplicaSetOffsetContext offset;
        private final MongoPreferredNode mongo;
        private final ReplicaSet replicaSet;

        ReplicaSetChangeStreamsContext(ReplicaSetPartition partition, ReplicaSetOffsetContext offsetContext,
                                       MongoPreferredNode mongo, ReplicaSet replicaSet) {
            this.partition = partition;
            this.offset = offsetContext;
            this.mongo = mongo;
            this.replicaSet = replicaSet;
        }

        ReplicaSetPartition getPartition() {
            return partition;
        }

        ReplicaSetOffsetContext getOffset() {
            return offset;
        }

        MongoPreferredNode getMongo() {
            return mongo;
        }

        String getReplicaSetName() {
            return replicaSet.replicaSetName();
        }
    }
}
