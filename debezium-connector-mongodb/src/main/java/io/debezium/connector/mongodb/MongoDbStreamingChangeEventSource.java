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
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterType;

import io.debezium.connector.mongodb.ConnectionContext.MongoPreferredNode;
import io.debezium.connector.mongodb.recordemitter.MongoDbChangeRecordEmitter;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
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
        while (true) {
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
            catch (MongoDbCursorReadPreferenceViolationException e) {
                LOGGER.warn("Restarting streaming due to read preference violation with server {}...", e.getServer());
                continue;
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

            break;
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
                if (error instanceof MongoDbCursorReadPreferenceViolationException) {
                    throw (MongoDbCursorReadPreferenceViolationException) error;
                }
                else {
                    LOGGER.error("Error while attempting to {}: {}", desc, error.getMessage(), error);
                    throw new ConnectException("Error while attempting to " + desc, error);
                }
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
        LOGGER.info("Reading change stream at offset {} for '{}'/{} from node {}",
                rsOffsetContext.getOffset(), replicaSet, mongo.getPreference().getName(), nodeAddress);

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
        else if (oplogStart.getTime() > 0) {
            LOGGER.info("Resume token not available, starting streaming from time '{}'", oplogStart);
            rsChangeStream.startAtOperationTime(oplogStart);
        }

        if (connectorConfig.getCursorMaxAwaitTime() > 0) {
            rsChangeStream.maxAwaitTime(connectorConfig.getCursorMaxAwaitTime(), TimeUnit.MILLISECONDS);
        }

        try (MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor = rsChangeStream.cursor()) {
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

                if (taskContext.getConnectorConfig().isCursorReadPreferenceMonitoringEnabled()) {
                    checkReadPreference(client, rsOffsetContext);
                }
            }
        }
    }

    private static void checkReadPreference(MongoClient client, ReplicaSetOffsetContext rsOffsetContext) {
        // Some information contained in this value gets updated asynchronously in the background. Most importantly
        // for this method is primary status
        var cluster = client.getClusterDescription();

        // For replica sets, only single connection mode is supported until DBZ-6032 can be resolved
        // See
        if (cluster.getType() != ClusterType.REPLICA_SET || cluster.getConnectionMode() != ClusterConnectionMode.SINGLE) {
            return;
        }

        // Since this is SINGLE, only one returned
        var servers = cluster.getServerDescriptions();
        var server = servers.get(0);
        if (server.isPrimary()) {
            LOGGER.warn("Closing cursor due to election invalidating read preference at offsets: {}", rsOffsetContext.getOffset());
            throw new MongoDbCursorReadPreferenceViolationException(server);
        }
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
