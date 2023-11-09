/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

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

import io.debezium.DebeziumException;
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
    private MongoDbOffsetContext effectiveOffset;

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
    public void init(MongoDbOffsetContext offsetContext) {

        this.effectiveOffset = offsetContext == null ? emptyOffsets(connectorConfig) : offsetContext;
    }

    @Override
    public void execute(ChangeEventSourceContext context, MongoDbPartition partition, MongoDbOffsetContext offsetContext)
            throws InterruptedException {
        final List<ReplicaSet> validReplicaSets = replicaSets.all();

        if (validReplicaSets.size() == 1) {
            // Streams the replica-set changes in the current thread
            streamChangesForReplicaSet(context, partition, validReplicaSets.get(0), offsetContext);
        }
        else if (validReplicaSets.size() > 1) {
            // Starts a thread for each replica-set and executes the streaming process
            streamChangesForReplicaSets(context, partition, validReplicaSets, offsetContext);
        }
    }

    @Override
    public MongoDbOffsetContext getOffsetContext() {
        return effectiveOffset;
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
        final ReplicaSetPartition rsPartition = effectiveOffset.getReplicaSetPartition(replicaSet);
        final ReplicaSetOffsetContext rsOffsetContext = effectiveOffset.getReplicaSetOffsetContext(replicaSet);

        LOGGER.info("Reading change stream for '{}'", replicaSet);

        final ChangeStreamIterable<BsonDocument> rsChangeStream = MongoUtil.openChangeStream(client, taskContext);
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
        else if (rsOffsetContext.lastTimestamp() != null) {
            LOGGER.info("Resuming streaming from operation time '{}'", rsOffsetContext.lastTimestamp());
            rsChangeStream.startAtOperationTime(rsOffsetContext.lastTimestamp());
        }

        if (connectorConfig.getCursorMaxAwaitTime() > 0) {
            rsChangeStream.maxAwaitTime(connectorConfig.getCursorMaxAwaitTime(), TimeUnit.MILLISECONDS);
        }

        final List<ChangeStreamDocument<BsonDocument>> fragmentBuffer = new ArrayList<>(16);

        try (MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor = rsChangeStream.cursor()) {
            // In Replicator, this used cursor.hasNext() but this is a blocking call and I observed that this can
            // delay the shutdown of the connector by up to 15 seconds or longer. By introducing a Metronome, we
            // can respond to the stop request much faster and without much overhead.
            Metronome pause = Metronome.sleeper(Duration.ofMillis(500), clock);
            while (context.isRunning()) {
                // Use tryNext which will return null if no document is yet available from the cursor.
                // In this situation if not document is available, we'll pause.
                ChangeStreamDocument<BsonDocument> event = cursor.tryNext();
                if (event != null) {
                    LOGGER.trace("Arrived Change Stream event: {}", event);
                    var split = event.getSplitEvent();

                    if (split != null) {
                        var currentFragment = split.getFragment();
                        var totalFragments = split.getOf();
                        LOGGER.trace("Change Stream event is a fragment: {} of {}", currentFragment, totalFragments);
                        fragmentBuffer.add(event);

                        // move to the next fragment if expected
                        if (currentFragment != totalFragments) {
                            continue;
                        }

                        // reconstruct the event
                        event = mergeEventFragments(fragmentBuffer);

                        // clear the fragment buffer
                        fragmentBuffer.clear();
                    }

                    if (split == null && !fragmentBuffer.isEmpty()) {
                        LOGGER.error("Expected event fragment but a new event arrived");
                        errorHandler.setProducerThrowable(new DebeziumException("Missing event fragment"));
                        return;
                    }

                    rsOffsetContext.changeStreamEvent(event);
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
                                        event, connectorConfig));
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
                        if (context.isPaused()) {
                            LOGGER.info("Streaming will now pause");
                            context.streamingPaused();
                            context.waitSnapshotCompletion();
                            LOGGER.info("Streaming resumed");
                        }
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

    @SuppressWarnings("DuplicatedCode")
    private static ChangeStreamDocument<BsonDocument> mergeEventFragments(List<ChangeStreamDocument<BsonDocument>> events) {
        var operationTypeString = firstOrNull(events, ChangeStreamDocument::getOperationTypeString);
        var resumeToken = events.get(events.size() - 1).getResumeToken();
        var namespaceDocument = firstOrNull(events, ChangeStreamDocument::getNamespaceDocument);
        var destinationNamespaceDocument = firstOrNull(events, ChangeStreamDocument::getDestinationNamespaceDocument);
        var fullDocument = firstOrNull(events, ChangeStreamDocument::getFullDocument);
        var fullDocumentBeforeChange = firstOrNull(events, ChangeStreamDocument::getFullDocumentBeforeChange);
        var documentKey = firstOrNull(events, ChangeStreamDocument::getDocumentKey);
        var clusterTime = firstOrNull(events, ChangeStreamDocument::getClusterTime);
        var updateDescription = firstOrNull(events, ChangeStreamDocument::getUpdateDescription);
        var txnNumber = firstOrNull(events, ChangeStreamDocument::getTxnNumber);
        var lsid = firstOrNull(events, ChangeStreamDocument::getLsid);
        var wallTime = firstOrNull(events, ChangeStreamDocument::getWallTime);
        var extraElements = firstOrNull(events, ChangeStreamDocument::getExtraElements);

        return new ChangeStreamDocument<>(
                operationTypeString,
                resumeToken,
                namespaceDocument,
                destinationNamespaceDocument,
                fullDocument,
                fullDocumentBeforeChange,
                documentKey,
                clusterTime,
                updateDescription,
                txnNumber,
                lsid,
                wallTime,
                null,
                extraElements);
    }

    private static <T> T firstOrNull(Collection<ChangeStreamDocument<BsonDocument>> events, Function<ChangeStreamDocument<BsonDocument>, T> getter) {
        return events.stream()
                .map(getter)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }
}
