/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.concurrent.TimeUnit;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;

import io.debezium.connector.mongodb.connection.ConnectionContext;
import io.debezium.connector.mongodb.connection.MongoDbConnection;
import io.debezium.connector.mongodb.connection.ReplicaSet;
import io.debezium.connector.mongodb.events.BufferingChangeStreamCursor;
import io.debezium.connector.mongodb.events.BufferingChangeStreamCursor.ResumableChangeStreamEvent;
import io.debezium.connector.mongodb.events.SplitEventHandler;
import io.debezium.connector.mongodb.metrics.MongoDbStreamingChangeEventSourceMetrics;
import io.debezium.connector.mongodb.recordemitter.MongoDbChangeRecordEmitter;
import io.debezium.connector.mongodb.snapshot.MongoDbIncrementalSnapshotContext;
import io.debezium.function.BlockingRunnable;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.util.Clock;

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
    private final ReplicaSet replicaSet;
    private final MongoDbTaskContext taskContext;
    private final MongoDbConnection.ChangeEventSourceConnectionFactory connections;
    private final MongoDbStreamingChangeEventSourceMetrics streamingMetrics;
    private MongoDbOffsetContext effectiveOffset;

    public MongoDbStreamingChangeEventSource(MongoDbConnectorConfig connectorConfig, MongoDbTaskContext taskContext,
                                             MongoDbConnection.ChangeEventSourceConnectionFactory connections,
                                             EventDispatcher<MongoDbPartition, CollectionId> dispatcher,
                                             ErrorHandler errorHandler, Clock clock, MongoDbStreamingChangeEventSourceMetrics streamingMetrics) {
        this.connectorConfig = connectorConfig;
        this.connectionContext = taskContext.getConnectionContext();
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.replicaSet = connectorConfig.getReplicaSet();
        this.taskContext = taskContext;
        this.connections = connections;
        this.streamingMetrics = streamingMetrics;
    }

    @Override
    public void init(MongoDbOffsetContext offsetContext) {
        this.effectiveOffset = offsetContext == null ? emptyOffsets(connectorConfig) : offsetContext;
    }

    /**
     *
     * @param context contextual information for this source's execution
     * @param partition the source partition from which the changes should be streamed
     * @param offsetContext unused as effective offset is build by {@link #init(MongoDbOffsetContext)}
     */
    @Override
    public void execute(ChangeEventSourceContext context, MongoDbPartition partition, MongoDbOffsetContext offsetContext)
            throws InterruptedException {
        streamChangesForReplicaSet(context, partition, replicaSet);
    }

    @Override
    public MongoDbOffsetContext getOffsetContext() {
        return effectiveOffset;
    }

    private void streamChangesForReplicaSet(ChangeEventSourceContext context, MongoDbPartition partition, ReplicaSet replicaSet) {
        try (MongoDbConnection mongo = connections.get(replicaSet, partition)) {
            mongo.execute("read from change stream on '" + replicaSet + "'", client -> {
                readChangeStream(client, replicaSet, context);
            });
        }
        catch (Throwable t) {
            LOGGER.error("Streaming for replica set {} failed", replicaSet.replicaSetName(), t);
            errorHandler.setProducerThrowable(t);
        }
    }

    private void readChangeStream(MongoClient client, ReplicaSet replicaSet, ChangeEventSourceContext context) {
        LOGGER.info("Reading change stream for '{}'", replicaSet);
        final ReplicaSetPartition rsPartition = effectiveOffset.getReplicaSetPartition(replicaSet);
        final ReplicaSetOffsetContext rsOffsetContext = effectiveOffset.getReplicaSetOffsetContext(replicaSet);
        final SplitEventHandler<BsonDocument> splitHandler = new SplitEventHandler<>();
        final ChangeStreamIterable<BsonDocument> rsChangeStream = initChangeStream(client, rsOffsetContext);

        try (var cursor = BufferingChangeStreamCursor.fromIterable(rsChangeStream, taskContext, streamingMetrics, clock).start()) {
            while (context.isRunning()) {
                waitWhenStreamingPaused(context);
                var resumableEvent = cursor.tryNext();
                if (resumableEvent == null) {
                    continue;
                }

                var result = resumableEvent.document
                        .map(doc -> processChangeStreamDocument(doc, splitHandler, replicaSet, rsPartition, rsOffsetContext))
                        .orElseGet(() -> errorHandled(() -> dispatchHeartbeatEvent(resumableEvent, rsPartition, rsOffsetContext)));

                if (result == StreamStatus.ERROR) {
                    return;
                }
            }
        }
    }

    private void waitWhenStreamingPaused(ChangeEventSourceContext context) {
        if (context.isPaused()) {
            errorHandled(() -> {
                LOGGER.info("Streaming will now pause");
                context.streamingPaused();
                context.waitSnapshotCompletion();
                LOGGER.info("Streaming resumed");
            });
        }
    }

    private StreamStatus processChangeStreamDocument(
                                                     ChangeStreamDocument<BsonDocument> document,
                                                     SplitEventHandler<BsonDocument> splitHandler,
                                                     ReplicaSet replicaSet,
                                                     ReplicaSetPartition rsPartition,
                                                     ReplicaSetOffsetContext rsOffsetContext) {
        LOGGER.trace("Arrived Change Stream event: {}", document);
        return splitHandler
                .handle(document)
                .map(event -> errorHandled(() -> dispatchChangeEvent(event, replicaSet, rsPartition, rsOffsetContext)))
                .orElse(StreamStatus.NEXT);
    }

    private void dispatchChangeEvent(
                                     ChangeStreamDocument<BsonDocument> event,
                                     ReplicaSet replicaSet,
                                     ReplicaSetPartition rsPartition,
                                     ReplicaSetOffsetContext rsOffsetContext)
            throws InterruptedException {
        var collectionId = new CollectionId(
                replicaSet.replicaSetName(),
                event.getNamespace().getDatabaseName(),
                event.getNamespace().getCollectionName());

        var emitter = new MongoDbChangeRecordEmitter(rsPartition, rsOffsetContext, clock, event, connectorConfig);
        rsOffsetContext.changeStreamEvent(event);
        dispatcher.dispatchDataChangeEvent(rsPartition, collectionId, emitter);
    }

    private void dispatchHeartbeatEvent(
                                        ResumableChangeStreamEvent<BsonDocument> event,
                                        ReplicaSetPartition rsPartition,
                                        ReplicaSetOffsetContext rsOffsetContext)
            throws InterruptedException {
        LOGGER.trace("No Change Stream event arrived");
        rsOffsetContext.noEvent(event);
        dispatcher.dispatchHeartbeatEvent(rsPartition, rsOffsetContext);
    }

    private StreamStatus errorHandled(BlockingRunnable action) {
        try {
            action.run();
            return StreamStatus.DISPATCHED;
        }
        catch (InterruptedException e) {
            LOGGER.info("Replicator thread is interrupted");
            Thread.currentThread().interrupt();
            return StreamStatus.ERROR;
        }
        catch (Exception e) {
            errorHandler.setProducerThrowable(e);
            return StreamStatus.ERROR;
        }
    }

    protected ChangeStreamIterable<BsonDocument> initChangeStream(MongoClient client, ReplicaSetOffsetContext offsetContext) {
        final ChangeStreamIterable<BsonDocument> stream = MongoUtil.openChangeStream(client, taskContext);

        if (taskContext.getCaptureMode().isFullUpdate()) {
            stream.fullDocument(FullDocument.UPDATE_LOOKUP);
        }
        if (taskContext.getCaptureMode().isIncludePreImage()) {
            stream.fullDocumentBeforeChange(FullDocumentBeforeChange.WHEN_AVAILABLE);
        }
        if (offsetContext.lastResumeToken() != null) {
            LOGGER.info("Resuming streaming from token '{}'", offsetContext.lastResumeToken());

            final BsonDocument doc = new BsonDocument();
            doc.put("_data", new BsonString(offsetContext.lastResumeToken()));
            stream.resumeAfter(doc);
        }
        else if (offsetContext.lastTimestamp() != null) {
            LOGGER.info("Resuming streaming from operation time '{}'", offsetContext.lastTimestamp());
            stream.startAtOperationTime(offsetContext.lastTimestamp());
        }

        if (connectorConfig.getCursorMaxAwaitTime() > 0) {
            stream.maxAwaitTime(connectorConfig.getCursorMaxAwaitTime(), TimeUnit.MILLISECONDS);
        }

        return stream;
    }

    protected MongoDbOffsetContext emptyOffsets(MongoDbConnectorConfig connectorConfig) {
        LOGGER.info("Initializing empty Offset context");
        return new MongoDbOffsetContext(
                new SourceInfo(connectorConfig),
                new TransactionContext(),
                new MongoDbIncrementalSnapshotContext<>(false));
    }

    /**
     * Indicates the status of event processing
     */
    protected enum StreamStatus {
        /**
         * Event successfully dispatched
         */
        DISPATCHED,
        /**
         * No event was dispatched and processing loop should advance to the next iteration immediately
         */
        NEXT,
        /**
         * An error occurred and processing loop should be terminated
         */
        ERROR,
    }
}
