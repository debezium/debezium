/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.TaskOffsetContext;
import io.debezium.connector.common.TaskPartition;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.pipeline.spi.SnapshotResult.SnapshotResultStatus;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Threads;

/**
 * Coordinates one or more {@link ChangeEventSource}s and executes them in order.
 *
 * @author Gunnar Morling
 */
@ThreadSafe
public class ChangeEventSourceCoordinator<P extends TaskPartition, O extends OffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEventSourceCoordinator.class);

    /**
     * Waiting period for the polling loop to finish. Will be applied twice, once gracefully, once forcefully.
     */
    public static final Duration SHUTDOWN_WAIT_TIMEOUT = Duration.ofSeconds(90);

    private final TaskOffsetContext<P, O> previousOffsetContext;
    private final ErrorHandler errorHandler;
    private final ChangeEventSourceFactory<P, O> changeEventSourceFactory;
    private final ChangeEventSourceMetricsFactory changeEventSourceMetricsFactory;
    private final ExecutorService executor;
    private final EventDispatcher<?> eventDispatcher;
    private final DatabaseSchema<?> schema;

    private volatile boolean running;
    private volatile StreamingChangeEventSource<P, O> streamingSource;
    private final ReentrantLock commitOffsetLock = new ReentrantLock();

    private SnapshotChangeEventSourceMetrics snapshotMetrics;
    private StreamingChangeEventSourceMetrics streamingMetrics;

    public ChangeEventSourceCoordinator(TaskOffsetContext<P, O> previousOffsetContext, ErrorHandler errorHandler, Class<? extends SourceConnector> connectorType,
                                        CommonConnectorConfig connectorConfig,
                                        ChangeEventSourceFactory<P, O> changeEventSourceFactory,
                                        ChangeEventSourceMetricsFactory changeEventSourceMetricsFactory, EventDispatcher<?> eventDispatcher, DatabaseSchema<?> schema) {
        this.previousOffsetContext = previousOffsetContext;
        this.errorHandler = errorHandler;
        this.changeEventSourceFactory = changeEventSourceFactory;
        this.changeEventSourceMetricsFactory = changeEventSourceMetricsFactory;
        this.executor = Threads.newSingleThreadExecutor(connectorType, connectorConfig.getLogicalName(), "change-event-source-coordinator");
        this.eventDispatcher = eventDispatcher;
        this.schema = schema;
    }

    public synchronized <T extends CdcSourceTaskContext> void start(T taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                    EventMetadataProvider metadataProvider) {
        O tempOffset = null;
        P tempPartition = null;

        // TODO: iterate over all entries instead of breaking
        for (Map.Entry<P, O> entry : previousOffsetContext.getOffsets().entrySet()) {
            tempPartition = entry.getKey();
            tempOffset = entry.getValue();
            break;
        }

        this.snapshotMetrics = changeEventSourceMetricsFactory.getSnapshotMetrics(taskContext, changeEventQueueMetrics, metadataProvider);
        this.streamingMetrics = changeEventSourceMetricsFactory.getStreamingMetrics(taskContext, changeEventQueueMetrics, metadataProvider);
        running = true;

        P partition = tempPartition;
        O previousOffset = tempOffset;

        // run the snapshot source on a separate thread so start() won't block
        executor.submit(() -> {
            try {
                snapshotMetrics.register(LOGGER);
                streamingMetrics.register(LOGGER);
                LOGGER.info("Metrics registered");

                ChangeEventSourceContext context = new ChangeEventSourceContextImpl();
                LOGGER.info("Context created");

                SnapshotChangeEventSource<P, O> snapshotSource = changeEventSourceFactory.getSnapshotChangeEventSource(snapshotMetrics);
                CatchUpStreamingResult catchUpStreamingResult = executeCatchUpStreaming(previousOffset, context, snapshotSource);
                if (catchUpStreamingResult.performedCatchUpStreaming) {
                    streamingConnected(false);
                    commitOffsetLock.lock();
                    streamingSource = null;
                    commitOffsetLock.unlock();
                }
                eventDispatcher.setEventListener(snapshotMetrics);
                SnapshotResult<O> snapshotResult = snapshotSource.execute(context, previousOffset, partition);
                LOGGER.info("Snapshot ended with {}", snapshotResult);

                if (snapshotResult.getStatus() == SnapshotResultStatus.COMPLETED || schema.tableInformationComplete()) {
                    schema.assureNonEmptySchema();
                }

                if (running && snapshotResult.isCompletedOrSkipped()) {
                    streamEvents(snapshotResult.getOffset(), context, partition);
                }
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
    }

    protected CatchUpStreamingResult executeCatchUpStreaming(OffsetContext previousOffset, ChangeEventSourceContext context,
                                                             SnapshotChangeEventSource<P, O> snapshotSource)
            throws InterruptedException {
        return new CatchUpStreamingResult(false);
    }

    protected void streamEvents(O offsetContext, ChangeEventSourceContext context, P partition) throws InterruptedException {
        streamingSource = changeEventSourceFactory.getStreamingChangeEventSource();
        eventDispatcher.setEventListener(streamingMetrics);
        streamingConnected(true);
        LOGGER.info("Starting streaming");
        streamingSource.execute(context, offsetContext, partition);
        LOGGER.info("Finished streaming");
    }

    public void commitOffset(Map<String, ?> offset) {
        if (!commitOffsetLock.isLocked() && streamingSource != null && offset != null) {
            streamingSource.commitOffset(offset);
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
        }
        finally {
            snapshotMetrics.unregister(LOGGER);
            streamingMetrics.unregister(LOGGER);
        }
    }

    private class ChangeEventSourceContextImpl implements ChangeEventSourceContext {

        @Override
        public boolean isRunning() {
            return running;
        }
    }

    private void streamingConnected(boolean status) {
        if (changeEventSourceMetricsFactory.connectionMetricHandledByCoordinator()) {
            streamingMetrics.connected(status);
        }
    }

    protected class CatchUpStreamingResult {

        public boolean performedCatchUpStreaming;

        public CatchUpStreamingResult(boolean performedCatchUpStreaming) {
            this.performedCatchUpStreaming = performedCatchUpStreaming;
        }

    }
}
