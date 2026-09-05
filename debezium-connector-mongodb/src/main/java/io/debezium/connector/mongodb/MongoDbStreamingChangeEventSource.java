/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;

import io.debezium.connector.mongodb.connection.MongoDbConnection;
import io.debezium.connector.mongodb.connection.MongoDbConnections;
import io.debezium.connector.mongodb.events.BufferingChangeStreamCursor;
import io.debezium.connector.mongodb.events.BufferingChangeStreamCursor.ResumableChangeStreamEvent;
import io.debezium.connector.mongodb.events.SplitEventHandler;
import io.debezium.connector.mongodb.metrics.MongoDbStreamingChangeEventSourceMetrics;
import io.debezium.connector.mongodb.recordemitter.MongoDbChangeRecordEmitter;
import io.debezium.function.BlockingRunnable;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.monitor.OffsetActivityMonitor;
import io.debezium.pipeline.monitor.OffsetActivityMonitorService;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

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
    private final SnapshotterService snapshotterService;
    private final OffsetActivityMonitorService offsetActivityMonitorService;
    private MongoDbOffsetContext effectiveOffset;
    private OffsetActivityMonitor<MongoDbPartition, MongoDbOffsetContext> offsetActivityMonitor;

    public MongoDbStreamingChangeEventSource(MongoDbConnectorConfig connectorConfig, MongoDbTaskContext taskContext,
                                             EventDispatcher<MongoDbPartition, CollectionId> dispatcher,
                                             ErrorHandler errorHandler, Clock clock, MongoDbStreamingChangeEventSourceMetrics streamingMetrics,
                                             SnapshotterService snapshotterService) {
        this.connectorConfig = connectorConfig;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.taskContext = taskContext;
        this.streamingMetrics = streamingMetrics;
        this.snapshotterService = snapshotterService;
        this.offsetActivityMonitorService = OffsetActivityMonitorService.lookup(connectorConfig.getServiceRegistry());
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

        if (!snapshotterService.getSnapshotter().shouldStream()) {
            LOGGER.info("Streaming is not enabled in configuration");
            return;
        }

        try (MongoDbConnection mongo = MongoDbConnections.create(taskContext.getRawConfig(), dispatcher, partition)) {
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

    @Override
    public Optional<OffsetActivityMonitor<MongoDbPartition, MongoDbOffsetContext>> getOffsetActivityMonitor() {
        if (offsetActivityMonitor == null) {
            offsetActivityMonitor = new MongoDbOffsetActivityMonitor(connectorConfig.getOffsetActivityMonitorInterval());
        }
        return Optional.of(offsetActivityMonitor);
    }

    private void readChangeStream(MongoClient client, ChangeEventSourceContext context, MongoDbPartition partition) {
        LOGGER.info("Reading change stream");
        final SplitEventHandler<BsonDocument> splitHandler = new SplitEventHandler<>();
        final ReadPreference readPreference = Optional.ofNullable(connectorConfig.getConnectionString().getReadPreference())
                .orElse(ReadPreference.primary());
        final MongoDbReadPreferenceMonitor readPreferenceMonitor = new MongoDbReadPreferenceMonitor(
                readPreference, connectorConfig.getHeartbeatFrequencyMs(), clock);

        try {
            while (context.isRunning()) {
                // The buffering cursor may have fetched beyond this point, so always resume from the last dispatched offset.
                final ChangeStreamIterable<BsonDocument> stream = initChangeStream(client, effectiveOffset);
                var nextAction = MongoDbReadPreferenceMonitor.Status.SATISFIED;

                try (var cursor = BufferingChangeStreamCursor.fromIterable(stream, taskContext, streamingMetrics, clock).start()) {
                    while (context.isRunning()) {
                        waitWhenStreamingPaused(context, cursor);
                        var resumableEvent = cursor.tryNext();
                        if (resumableEvent != null) {
                            var result = resumableEvent.document
                                    .map(doc -> processChangeStreamDocument(doc, splitHandler, partition, effectiveOffset))
                                    .orElseGet(() -> errorHandled(() -> dispatchHeartbeatEvent(resumableEvent, partition, effectiveOffset)));

                            if (result == StreamStatus.ERROR) {
                                return;
                            }
                        }

                        offsetActivityMonitorService.pulse(partition, effectiveOffset);

                        if (effectiveOffset.hasOffset() && splitHandler.isEmpty() && readPreferenceMonitor.isCheckDue()) {
                            var cursorAddress = cursor.getCurrentServerAddress();
                            if (cursorAddress.isPresent()) {
                                nextAction = readPreferenceMonitor.evaluate(client.getClusterDescription(), cursorAddress.get());
                                if (nextAction == MongoDbReadPreferenceMonitor.Status.UNVERIFIED) {
                                    LOGGER.debug("Unable to verify whether change stream server '{}' satisfies read preference '{}'; "
                                            + "the topology will be checked again at the next monitoring interval",
                                            cursorAddress.get(), readPreferenceMonitor.getReadPreference());
                                }
                                if (nextAction == MongoDbReadPreferenceMonitor.Status.RELOCATE) {
                                    LOGGER.info("Change stream server '{}' no longer matches read preference '{}'; reopening from offset '{}'",
                                            cursorAddress.get(), readPreferenceMonitor.getReadPreference(), effectiveOffset.getOffset());
                                    break;
                                }
                                if (nextAction == MongoDbReadPreferenceMonitor.Status.NO_ELIGIBLE_SERVER) {
                                    LOGGER.info("Change stream server '{}' no longer matches read preference '{}', and no eligible server is available; "
                                            + "pausing change stream consumption at offset '{}'",
                                            cursorAddress.get(), readPreferenceMonitor.getReadPreference(), effectiveOffset.getOffset());
                                    break;
                                }
                            }
                        }
                    }
                }

                if (nextAction == MongoDbReadPreferenceMonitor.Status.NO_ELIGIBLE_SERVER
                        && !waitForEligibleServer(client, context, readPreferenceMonitor, partition)) {
                    return;
                }
                if (nextAction != MongoDbReadPreferenceMonitor.Status.RELOCATE
                        && nextAction != MongoDbReadPreferenceMonitor.Status.NO_ELIGIBLE_SERVER) {
                    return;
                }
            }
        }
        catch (InterruptedException e) {
            LOGGER.info("Interrupted while waiting for a server that satisfies read preference '{}'", readPreferenceMonitor.getReadPreference());
            Thread.currentThread().interrupt();
        }
        catch (MongoException e) {
            LOGGER.error("Error while reading change stream", e);
            errorHandler.setProducerThrowable(e);
        }
    }

    private boolean waitForEligibleServer(MongoClient client, ChangeEventSourceContext context,
                                          MongoDbReadPreferenceMonitor readPreferenceMonitor, MongoDbPartition partition)
            throws InterruptedException {
        final Metronome metronome = Metronome.parker(Duration.ofMillis(readPreferenceMonitor.getCheckIntervalMs()), clock);

        while (context.isRunning()) {
            waitWhenStreamingPaused(context);
            if (!readPreferenceMonitor.shouldWaitForEligibleServer(client.getClusterDescription())) {
                LOGGER.info("An eligible server for read preference '{}' is available; resuming change stream consumption from offset '{}'",
                        readPreferenceMonitor.getReadPreference(), effectiveOffset.getOffset());
                return true;
            }

            offsetActivityMonitorService.pulse(partition, effectiveOffset);
            metronome.pause();
        }

        return false;
    }

    private void waitWhenStreamingPaused(ChangeEventSourceContext context) throws InterruptedException {
        if (context.isPaused()) {
            LOGGER.info("Streaming will now pause while waiting for an eligible change stream server");
            context.streamingPaused();
            context.waitSnapshotCompletion();
            LOGGER.info("Streaming resumed");
        }
    }

    private void waitWhenStreamingPaused(ChangeEventSourceContext context, BufferingChangeStreamCursor cursor) {
        if (context.isPaused()) {
            errorHandled(() -> {
                LOGGER.info("Streaming will now pause");
                cursor.pause();
                context.streamingPaused();
                context.waitSnapshotCompletion();
                cursor.resume();
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

        if (connectorConfig.getCaptureMode().isFullUpdate()) {
            if (connectorConfig.getCaptureModeFullUpdateType().isPostImage()) {
                stream.fullDocument(FullDocument.WHEN_AVAILABLE);
            }
            else {
                stream.fullDocument(FullDocument.UPDATE_LOOKUP);
            }
        }
        if (connectorConfig.getCaptureMode().isIncludePreImage()) {
            stream.fullDocumentBeforeChange(FullDocumentBeforeChange.WHEN_AVAILABLE);
        }
        if (offsetContext.lastResumeToken() != null) {
            LOGGER.info("Resuming streaming from token '{}'", offsetContext.lastResumeToken());
            stream.resumeAfter(offsetContext.lastResumeTokenDoc());
        }
        else if (offsetContext.lastTimestamp() != null) {
            LOGGER.info("Resuming streaming from operation time '{}'", offsetContext.lastTimestamp());
            stream.startAtOperationTime(offsetContext.lastTimestamp());
        }
        else if (connectorConfig.startAtOperationTime().isPresent()) {
            LOGGER.info("Resuming streaming from explicit operation time '{}'", offsetContext.lastTimestamp());
            stream.startAtOperationTime(connectorConfig.startAtOperationTime().get());
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
