/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.Notification;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.pipeline.spi.SnapshotResult.SnapshotResultStatus;
import io.debezium.schema.DatabaseSchema;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.LoggingContext;
import io.debezium.util.Threads;

/**
 * Coordinates one or more {@link ChangeEventSource}s and executes them in order.
 *
 * @author Gunnar Morling
 */
@ThreadSafe
public class ChangeEventSourceCoordinator<P extends Partition, O extends OffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEventSourceCoordinator.class);

    /**
     * Waiting period for the polling loop to finish. Will be applied twice, once gracefully, once forcefully.
     */
    public static final Duration SHUTDOWN_WAIT_TIMEOUT = Duration.ofSeconds(90);

    protected final Offsets<P, O> previousOffsets;
    protected final ErrorHandler errorHandler;
    protected final ChangeEventSourceFactory<P, O> changeEventSourceFactory;
    protected final ChangeEventSourceMetricsFactory<P> changeEventSourceMetricsFactory;
    protected final ExecutorService executor;
    protected final EventDispatcher<P, ?> eventDispatcher;
    protected final DatabaseSchema<?> schema;
    protected final SignalProcessor<P, O> signalProcessor;
    protected final NotificationService<P, O> notificationService;

    private volatile boolean running;
    protected volatile StreamingChangeEventSource<P, O> streamingSource;
    protected final ReentrantLock commitOffsetLock = new ReentrantLock();

    protected SnapshotChangeEventSourceMetrics<P> snapshotMetrics;
    protected StreamingChangeEventSourceMetrics<P> streamingMetrics;

    public ChangeEventSourceCoordinator(Offsets<P, O> previousOffsets, ErrorHandler errorHandler, Class<? extends SourceConnector> connectorType,
                                        CommonConnectorConfig connectorConfig,
                                        ChangeEventSourceFactory<P, O> changeEventSourceFactory,
                                        ChangeEventSourceMetricsFactory<P> changeEventSourceMetricsFactory, EventDispatcher<P, ?> eventDispatcher,
                                        DatabaseSchema<?> schema,
                                        SignalProcessor<P, O> signalProcessor, NotificationService<P, O> notificationService) {
        this.previousOffsets = previousOffsets;
        this.errorHandler = errorHandler;
        this.changeEventSourceFactory = changeEventSourceFactory;
        this.changeEventSourceMetricsFactory = changeEventSourceMetricsFactory;
        this.executor = Threads.newSingleThreadExecutor(connectorType, connectorConfig.getLogicalName(), "change-event-source-coordinator");
        this.eventDispatcher = eventDispatcher;
        this.schema = schema;
        this.signalProcessor = signalProcessor;
        this.notificationService = notificationService;
    }

    public synchronized void start(CdcSourceTaskContext taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                   EventMetadataProvider metadataProvider) {

        AtomicReference<LoggingContext.PreviousContext> previousLogContext = new AtomicReference<>();
        try {
            this.snapshotMetrics = changeEventSourceMetricsFactory.getSnapshotMetrics(taskContext, changeEventQueueMetrics, metadataProvider);
            this.streamingMetrics = changeEventSourceMetricsFactory.getStreamingMetrics(taskContext, changeEventQueueMetrics, metadataProvider);
            running = true;

            // run the snapshot source on a separate thread so start() won't block
            executor.submit(() -> {
                try {
                    previousLogContext.set(taskContext.configureLoggingContext("snapshot"));
                    snapshotMetrics.register();
                    streamingMetrics.register();
                    LOGGER.info("Metrics registered");

                    ChangeEventSourceContext context = new ChangeEventSourceContextImpl();
                    LOGGER.info("Context created");

                    SnapshotChangeEventSource<P, O> snapshotSource = changeEventSourceFactory.getSnapshotChangeEventSource(snapshotMetrics);
                    executeChangeEventSources(taskContext, snapshotSource, previousOffsets, previousLogContext, context);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.warn("Change event source executor was interrupted", e);
                }
                catch (Throwable e) {
                    errorHandler.setProducerThrowable(e);
                }
                finally {
                    streamingConnected(false);
                }
            });

            getSignalProcessor(previousOffsets).ifPresent(SignalProcessor::start); // this will run on a separate thread
        }
        finally {
            if (previousLogContext.get() != null) {
                previousLogContext.get().restore();
            }
        }
    }

    public Optional<SignalProcessor<P, O>> getSignalProcessor(Offsets<P, O> previousOffset) { // Signal processing only work with one partition
        return previousOffset == null || previousOffset.getOffsets().size() == 1 ? Optional.ofNullable(signalProcessor) : Optional.empty();
    }

    protected void executeChangeEventSources(CdcSourceTaskContext taskContext, SnapshotChangeEventSource<P, O> snapshotSource, Offsets<P, O> previousOffsets,
                                             AtomicReference<LoggingContext.PreviousContext> previousLogContext, ChangeEventSourceContext context)
            throws InterruptedException {
        final P partition = previousOffsets.getTheOnlyPartition();
        final O previousOffset = previousOffsets.getTheOnlyOffset();

        previousLogContext.set(taskContext.configureLoggingContext("snapshot", partition));
        SnapshotResult<O> snapshotResult = doSnapshot(snapshotSource, context, partition, previousOffset);

        try {
            notificationService.notify(Notification.Builder.builder()
                    .withId(UUID.randomUUID().toString())
                    .withAggregateType("Initial Snapshot")
                    .withType("Status " + snapshotResult.getStatus())
                    .build(),
                    Offsets.of(previousOffsets.getTheOnlyPartition(), snapshotResult.getOffset()));
        }
        catch (UnsupportedOperationException e) {
            LOGGER.warn("Initial Snapshot notification not currently supported for MongoDB");
        }

        getSignalProcessor(previousOffsets).ifPresent(s -> s.setContext(snapshotResult.getOffset()));

        LOGGER.debug("Snapshot result {}", snapshotResult);

        if (running && snapshotResult.isCompletedOrSkipped()) {
            previousLogContext.set(taskContext.configureLoggingContext("streaming", partition));
            streamEvents(context, partition, snapshotResult.getOffset());
        }
    }

    protected SnapshotResult<O> doSnapshot(SnapshotChangeEventSource<P, O> snapshotSource, ChangeEventSourceContext context, P partition, O previousOffset)
            throws InterruptedException {
        CatchUpStreamingResult catchUpStreamingResult = executeCatchUpStreaming(context, snapshotSource, partition, previousOffset);
        if (catchUpStreamingResult.performedCatchUpStreaming) {
            streamingConnected(false);
            commitOffsetLock.lock();
            streamingSource = null;
            commitOffsetLock.unlock();
        }
        eventDispatcher.setEventListener(snapshotMetrics);
        SnapshotResult<O> snapshotResult = snapshotSource.execute(context, partition, previousOffset);
        LOGGER.info("Snapshot ended with {}", snapshotResult);

        if (snapshotResult.getStatus() == SnapshotResultStatus.COMPLETED || schema.tableInformationComplete()) {
            schema.assureNonEmptySchema();
        }
        return snapshotResult;
    }

    protected CatchUpStreamingResult executeCatchUpStreaming(ChangeEventSourceContext context,
                                                             SnapshotChangeEventSource<P, O> snapshotSource,
                                                             P partition, O previousOffset)
            throws InterruptedException {
        return new CatchUpStreamingResult(false);
    }

    protected void streamEvents(ChangeEventSourceContext context, P partition, O offsetContext) throws InterruptedException {
        initStreamEvents(partition, offsetContext);
        LOGGER.info("Starting streaming");
        streamingSource.execute(context, partition, offsetContext);
        LOGGER.info("Finished streaming");
    }

    protected void initStreamEvents(P partition, O offsetContext) throws InterruptedException {

        streamingSource = changeEventSourceFactory.getStreamingChangeEventSource();
        eventDispatcher.setEventListener(streamingMetrics);
        streamingConnected(true);
        streamingSource.init(offsetContext);

        getSignalProcessor(previousOffsets).ifPresent(s -> s.setContext(streamingSource.getOffsetContext()));

        final Optional<IncrementalSnapshotChangeEventSource<P, ? extends DataCollectionId>> incrementalSnapshotChangeEventSource = changeEventSourceFactory
                .getIncrementalSnapshotChangeEventSource(offsetContext, snapshotMetrics, snapshotMetrics, notificationService);
        eventDispatcher.setIncrementalSnapshotChangeEventSource(incrementalSnapshotChangeEventSource);
        incrementalSnapshotChangeEventSource.ifPresent(x -> x.init(partition, offsetContext));
    }

    public void commitOffset(Map<String, ?> partition, Map<String, ?> offset) {
        if (!commitOffsetLock.isLocked() && streamingSource != null && offset != null) {
            streamingSource.commitOffset(partition, offset);
        }
    }

    /**
     * Stops this coordinator.
     */
    public synchronized void stop() throws InterruptedException {
        running = false;

        try {
            // Clear interrupt flag so the graceful termination is always attempted
            Thread.interrupted();
            executor.shutdown();
            boolean isShutdown = executor.awaitTermination(SHUTDOWN_WAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            if (!isShutdown) {
                LOGGER.warn("Coordinator didn't stop in the expected time, shutting down executor now");

                // Clear interrupt flag so the forced termination is always attempted
                Thread.interrupted();
                executor.shutdownNow();
                executor.awaitTermination(SHUTDOWN_WAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }

            Optional<SignalProcessor<P, O>> processor = getSignalProcessor(previousOffsets);
            if (processor.isPresent()) {
                processor.get().stop();
            }

            if (notificationService != null) {
                notificationService.stop();
            }
            eventDispatcher.close();
        }
        finally {
            snapshotMetrics.unregister();
            streamingMetrics.unregister();
        }
    }

    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    public class ChangeEventSourceContextImpl implements ChangeEventSourceContext {

        @Override
        public boolean isRunning() {
            return running;
        }
    }

    protected void streamingConnected(boolean status) {
        if (changeEventSourceMetricsFactory.connectionMetricHandledByCoordinator()) {
            streamingMetrics.connected(status);
            LOGGER.info("Connected metrics set to '{}'", status);
        }
    }

    protected class CatchUpStreamingResult {

        public boolean performedCatchUpStreaming;

        public CatchUpStreamingResult(boolean performedCatchUpStreaming) {
            this.performedCatchUpStreaming = performedCatchUpStreaming;
        }

    }
}
