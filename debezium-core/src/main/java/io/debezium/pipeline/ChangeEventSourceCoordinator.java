/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.signal.actions.SignalActionProvider;
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
    private final ExecutorService blockingSnapshotExecutor;
    protected final EventDispatcher<P, ?> eventDispatcher;
    protected final DatabaseSchema<?> schema;
    protected final SignalProcessor<P, O> signalProcessor;
    protected final NotificationService<P, O> notificationService;
    protected final CommonConnectorConfig connectorConfig;

    private volatile boolean running;
    private volatile boolean paused;
    private volatile boolean streaming;
    protected volatile StreamingChangeEventSource<P, O> streamingSource;
    protected final ReentrantLock commitOffsetLock = new ReentrantLock();

    protected SnapshotChangeEventSourceMetrics<P> snapshotMetrics;
    protected StreamingChangeEventSourceMetrics<P> streamingMetrics;
    private ChangeEventSourceContext context;
    private SnapshotChangeEventSource<P, O> snapshotSource;
    private AtomicReference<LoggingContext.PreviousContext> previousLogContext;
    private CdcSourceTaskContext taskContext;

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
        this.blockingSnapshotExecutor = Threads.newSingleThreadExecutor(connectorType, connectorConfig.getLogicalName(), "blocking-snapshot");
        this.eventDispatcher = eventDispatcher;
        this.schema = schema;
        this.signalProcessor = signalProcessor;
        this.notificationService = notificationService;
        this.connectorConfig = connectorConfig;
    }

    public synchronized void start(CdcSourceTaskContext taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                   EventMetadataProvider metadataProvider) {

        previousLogContext = new AtomicReference<>();
        try {
            this.taskContext = taskContext;
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

                    context = new ChangeEventSourceContextImpl();
                    LOGGER.info("Context created");

                    snapshotSource = changeEventSourceFactory.getSnapshotChangeEventSource(snapshotMetrics, notificationService);
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

            getSignalProcessor(previousOffsets).ifPresent(signalProcessor -> registerSignalActionsAndStartProcessor(signalProcessor,
                    eventDispatcher, this, connectorConfig));
        }
        finally {
            if (previousLogContext.get() != null) {
                previousLogContext.get().restore();
            }
        }
    }

    protected void registerSignalActionsAndStartProcessor(SignalProcessor<P, O> signalProcessor, EventDispatcher<P, ? extends DataCollectionId> dispatcher,
                                                          ChangeEventSourceCoordinator<P, ?> changeEventSourceCoordinator, CommonConnectorConfig connectorConfig) {

        // Maybe this can be moved on task
        List<SignalActionProvider> actionProviders = StreamSupport.stream(ServiceLoader.load(SignalActionProvider.class).spliterator(), false)
                .collect(Collectors.toList());

        actionProviders.stream()
                .map(provider -> provider.createActions(dispatcher, changeEventSourceCoordinator, connectorConfig))
                .flatMap(e -> e.entrySet().stream())
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()))
                .forEach(signalProcessor::registerSignalAction);

        signalProcessor.start(); // this will run on a separate thread

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

        getSignalProcessor(previousOffsets).ifPresent(s -> s.setContext(snapshotResult.getOffset()));

        LOGGER.debug("Snapshot result {}", snapshotResult);

        if (running && snapshotResult.isCompletedOrSkipped()) {
            previousLogContext.set(taskContext.configureLoggingContext("streaming", partition));
            streamEvents(context, partition, snapshotResult.getOffset());
        }
    }

    public void doBlockingSnapshot(P partition, OffsetContext offsetContext) {
        // TODO
        // 1: Pass partition and offset from signal DONE!
        // 2: In streaming while loop check if needed to pause DONE for PostgreSQL
        // 3: Call doSnapshot only when I am sure the streaming is in pause (maybe a condition) DONE!
        // 4: resume the streaming DONE!
        // 5: This method should not block. Run it in a separate thread. DONE!

        blockingSnapshotExecutor.submit(() -> {

            previousLogContext.set(taskContext.configureLoggingContext("streaming", partition));

            paused = true;
            streaming = true;

            try {

                context.waitStreamingPaused();

                previousLogContext.set(taskContext.configureLoggingContext("snapshot"));
                LOGGER.info("Starting snapshot");
                SnapshotResult<O> snapshotResult = doSnapshot(snapshotSource, context, partition, (O) offsetContext);

                if (running && snapshotResult.isCompletedOrSkipped()) {
                    previousLogContext.set(taskContext.configureLoggingContext("streaming", partition));
                    paused = false;
                    context.resumeStreaming();
                }
            }
            catch (InterruptedException e) { // TODO manage it
                throw new RuntimeException(e);
            }
        });
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
        // Maybe add a pause and restart method that should be called from the action through the coordinator
        streamingSource.execute(context, partition, offsetContext);
        LOGGER.info("Finished streaming");
    }

    protected void initStreamEvents(P partition, O offsetContext) throws InterruptedException {

        streamingSource = changeEventSourceFactory.getStreamingChangeEventSource();
        eventDispatcher.setEventListener(streamingMetrics);
        streamingConnected(true); // TODO call this when pausing streaming?
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

        private final Lock lock = new ReentrantLock();
        private final Condition snapshotFinished = lock.newCondition();
        private final Condition streamingPaused = lock.newCondition();

        @Override
        public boolean isPaused() {
            return paused;
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public void resumeStreaming() {
            lock.lock();
            try {
                snapshotFinished.signalAll();
                LOGGER.trace("Streaming will now resumes.");
            }
            finally {
                lock.unlock();
            }
        }

        @Override
        public void waitSnapshotCompletion() throws InterruptedException {
            lock.lock();
            try {
                while (paused) {
                    LOGGER.trace("Waiting for snapshot to be completed.");
                    snapshotFinished.await();
                    streaming = true;
                }
            }
            finally {
                lock.unlock();
            }
        }

        @Override
        public void streamingPaused() {
            lock.lock();
            try {
                LOGGER.trace("Streaming paused. Blocking snapshot can now start.");
                streaming = false;
                streamingPaused.signalAll();
            }
            finally {
                lock.unlock();
            }
        }

        @Override
        public void waitStreamingPaused() throws InterruptedException {
            lock.lock();
            try {
                while (streaming) {
                    LOGGER.trace("Requested a blocking snapshot. Waiting for streaming to be paused.");
                    streamingPaused.await();
                }
            }
            finally {
                lock.unlock();
            }
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
