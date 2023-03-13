/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;

import io.debezium.connector.mongodb.connection.ConnectionContext;
import io.debezium.connector.mongodb.connection.MongoDbConnection;
import io.debezium.connector.mongodb.connection.ReplicaSet;
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

    private final MongoDbConnectorConfig connectorConfig;
    private final EventDispatcher<MongoDbPartition, CollectionId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final ConnectionContext connectionContext;
    private final ReplicaSets replicaSets;
    private final MongoDbTaskContext taskContext;
    private final MongoDbConnection.ChangeEventSourceConnectionFactory connections;

    public MongoDbStreamingChangeEventSource(MongoDbConnectorConfig connectorConfig, MongoDbTaskContext taskContext,
                                             MongoDbConnection.ChangeEventSourceConnectionFactory connections, ReplicaSets replicaSets,
                                             EventDispatcher<MongoDbPartition, CollectionId> dispatcher,
                                             ErrorHandler errorHandler, Clock clock) {
        this.connectorConfig = connectorConfig;
        this.connectionContext = taskContext.getConnectionContext();
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.replicaSets = replicaSets;
        this.taskContext = taskContext;
        this.connections = connections;
    }

    @Override
    public void execute(ChangeEventSourceContext context, MongoDbPartition partition, MongoDbOffsetContext offsetContext)
            throws InterruptedException {
        final List<ReplicaSet> validReplicaSets = replicaSets.all();

        if (offsetContext == null) {
            offsetContext = emptyOffsets(connectorConfig);
        }

        if (validReplicaSets.size() == 1) {
            // Streams the replica-set changes in the current thread
            streamChangesForReplicaSet(context, partition, validReplicaSets.get(0), offsetContext);
        }
        else if (validReplicaSets.size() > 1) {
            // Starts a thread for each replica-set and executes the streaming process
            streamChangesForReplicaSets(context, partition, validReplicaSets, offsetContext);
        }
    }

    private void streamChangesForReplicaSet(ChangeEventSourceContext context, MongoDbPartition partition,
                                            ReplicaSet replicaSet, MongoDbOffsetContext offsetContext) {
        try (MongoDbConnection mongo = connections.get(replicaSet, partition)) {
            mongo.execute("read from change stream on '" + replicaSet + "'", client -> {
                readChangeStream(client, replicaSet, context, offsetContext);
            });
        }
        catch (Throwable t) {
            LOGGER.error("Streaming for replica set {} failed", replicaSet.replicaSetName(), t);
            errorHandler.setProducerThrowable(t);
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

    private void readChangeStream(MongoClient client, ReplicaSet replicaSet, ChangeEventSourceContext context,
                                  MongoDbOffsetContext offsetContext) {
        final ReplicaSetPartition rsPartition = offsetContext.getReplicaSetPartition(replicaSet);
        final ReplicaSetOffsetContext rsOffsetContext = offsetContext.getReplicaSetOffsetContext(replicaSet);

        LOGGER.info("Reading change stream for '{}'", replicaSet);

        final ChangeStreamPipeline pipeline = new ChangeStreamPipelineFactory(rsOffsetContext, taskContext.getConnectorConfig(), taskContext.filters().getConfig())
                .create();
        final ChangeStreamIterable<BsonDocument> rsChangeStream = client.watch(pipeline.getStages(), BsonDocument.class);
        if (taskContext.getCaptureMode().isFullUpdate()) {
            rsChangeStream.fullDocument(FullDocument.UPDATE_LOOKUP);
        }
        if (taskContext.getCaptureMode().isIncludePreImage()) {
            rsChangeStream.fullDocumentBeforeChange(FullDocumentBeforeChange.WHEN_AVAILABLE);
        }
        if (rsOffsetContext.lastResumeToken() != null) {
            LOGGER.info("Resuming streaming from token '{}'", rsOffsetContext.lastResumeToken());

            final BsonDocument doc = new BsonDocument();
            doc.put("_data", new BsonString(rsOffsetContext.lastResumeToken()));
            rsChangeStream.resumeAfter(doc);
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

                    rsOffsetContext.changeStreamEvent(event);
                    rsOffsetContext.getOffset();
                    CollectionId collectionId = new CollectionId(
                            replicaSet.replicaSetName(),
                            event.getNamespace().getDatabaseName(),
                            event.getNamespace().getCollectionName());

                    try {
                        // Note that this will trigger a heartbeat request
                        dispatcher.dispatchDataChangeEvent(
                                rsPartition,
                                collectionId,
                                new MongoDbChangeRecordEmitter(
                                        rsPartition,
                                        rsOffsetContext,
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
                            rsOffsetContext.noEvent(cursor);
                            dispatcher.dispatchHeartbeatEvent(rsPartition, rsOffsetContext);
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

            }
        }
    }

    protected MongoDbOffsetContext emptyOffsets(MongoDbConnectorConfig connectorConfig) {
        LOGGER.info("Initializing empty Offset context");
        return new MongoDbOffsetContext(
                new SourceInfo(connectorConfig),
                new TransactionContext(),
                new MongoDbIncrementalSnapshotContext<>(false));
    }
}
