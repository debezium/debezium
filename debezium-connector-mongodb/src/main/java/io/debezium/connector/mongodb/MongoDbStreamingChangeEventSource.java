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

import io.debezium.connector.mongodb.connection.MongoDbConnection;
import io.debezium.connector.mongodb.events.BufferingChangeStreamCursor;
import io.debezium.connector.mongodb.events.BufferingChangeStreamCursor.ResumableChangeStreamEvent;
import io.debezium.connector.mongodb.events.SplitEventHandler;
import io.debezium.connector.mongodb.metrics.MongoDbStreamingChangeEventSourceMetrics;
import io.debezium.connector.mongodb.recordemitter.MongoDbChangeRecordEmitter;
import io.debezium.function.BlockingRunnable;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
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

    private final MongoDbTaskContext taskContext;
    private final MongoDbStreamingChangeEventSourceMetrics streamingMetrics;
    private MongoDbOffsetContext effectiveOffset;

    public MongoDbStreamingChangeEventSource(MongoDbConnectorConfig connectorConfig, MongoDbTaskContext taskContext,
                                             EventDispatcher<MongoDbPartition, CollectionId> dispatcher,
                                             ErrorHandler errorHandler, Clock clock, MongoDbStreamingChangeEventSourceMetrics streamingMetrics) {
        this.connectorConfig = connectorConfig;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.taskContext = taskContext;
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
    public void execute(ChangeEventSourceContext context, MongoDbPartition partition, MongoDbOffsetContext offsetContext) {
        try (MongoDbConnection mongo = taskContext.getConnection(dispatcher, partition)) {
            mongo.execute("Reading change stream", client -> {
                readChangeStream(client, context, partition);
            });
        }
        catch (Throwable t) {
            LOGGER.error("Streaming failed", t);
            errorHandler.setProducerThrowable(t);
        }
    }

    @Override
    public MongoDbOffsetContext getOffsetContext() {
        return effectiveOffset;
    }

    private void readChangeStream(MongoClient client, ChangeEventSourceContext context, MongoDbPartition partition) {
        LOGGER.info("Reading change stream");
        final SplitEventHandler<BsonDocument> splitHandler = new SplitEventHandler<>();
        final ChangeStreamIterable<BsonDocument> stream = initChangeStream(client, effectiveOffset);

        try (var cursor = BufferingChangeStreamCursor.fromIterable(stream, taskContext, streamingMetrics, clock).start()) {
            while (context.isRunning()) {
                waitWhenStreamingPaused(context);
                var resumableEvent = cursor.tryNext();
                if (resumableEvent == null) {
                    continue;
                }

                var result = resumableEvent.document
                        .map(doc -> processChangeStreamDocument(doc, splitHandler, partition, effectiveOffset))
                        .orElseGet(() -> errorHandled(() -> dispatchHeartbeatEvent(resumableEvent, partition, effectiveOffset)));

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
                                                     MongoDbPartition partition,
                                                     MongoDbOffsetContext offsetContext) {
        LOGGER.trace("Arrived Change Stream event: {}", document);
        return splitHandler
                .handle(document)
                .map(event -> errorHandled(() -> dispatchChangeEvent(event, partition, offsetContext)))
                .orElse(StreamStatus.NEXT);
    }

    private void dispatchChangeEvent(
                                     ChangeStreamDocument<BsonDocument> event,
                                     MongoDbPartition partition,
                                     MongoDbOffsetContext offsetContext)
            throws InterruptedException {
        var collectionId = new CollectionId(
                event.getNamespace().getDatabaseName(),
                event.getNamespace().getCollectionName());

        var emitter = new MongoDbChangeRecordEmitter(partition, offsetContext, clock, event, connectorConfig);
        offsetContext.changeStreamEvent(event);
        dispatcher.dispatchDataChangeEvent(partition, collectionId, emitter);
    }

    private void dispatchHeartbeatEvent(
                                        ResumableChangeStreamEvent<BsonDocument> event,
                                        MongoDbPartition partition,
                                        MongoDbOffsetContext offsetContext)
            throws InterruptedException {
        LOGGER.trace("No Change Stream event arrived");
        offsetContext.noEvent(event);
        dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
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

    protected ChangeStreamIterable<BsonDocument> initChangeStream(MongoClient client, MongoDbOffsetContext offsetContext) {
        final ChangeStreamIterable<BsonDocument> stream = MongoUtils.openChangeStream(client, taskContext);

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
        return MongoDbOffsetContext.empty(connectorConfig);
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
