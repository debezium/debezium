/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.GuardedBy;
import io.debezium.annotation.ThreadSafe;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.util.Threads;

/**
 * Coordinates one or more {@link ChangeEventSource}s and executes them in order.
 *
 * @author Gunnar Morling
 */
@ThreadSafe
public class ChangeEventSourceCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEventSourceCoordinator.class);

    private final ErrorHandler errorHandler;
    private final ChangeEventSourceFactory changeEventSourceFactory;
    private final ExecutorService executor;

    @GuardedBy("this")
    private StreamingChangeEventSource streamingSource;

    @GuardedBy("this")
    private boolean running = true;

    public ChangeEventSourceCoordinator(ErrorHandler errorHandler, Class<? extends SourceConnector> connectorType, String logicalName, ChangeEventSourceFactory changeEventSourceFactory) {
        this.errorHandler = errorHandler;
        this.changeEventSourceFactory = changeEventSourceFactory;
        this.executor = Threads.newSingleThreadExecutor(connectorType, logicalName, "change-event-source-coordinator");
    }

    public synchronized void start() {
        SnapshotChangeEventSource snapshotSource = changeEventSourceFactory.getSnapshotChangeEventSource();

        // run the snapshot source on a separate thread so start() won't block
        executor.submit(() -> {
            try {
                SnapshotResult snapshotResult = snapshotSource.execute();

                // if the snapshot source has reacted to interruption before, we woudn't get here
                startStreamingEventSource(snapshotResult.getOffset());
            }
            catch(Exception e) {
                errorHandler.setProducerThrowable(e);
            }
        });
    }

    /**
     * Starts the streaming source at the given offset, but only if the connector hasn't been stopped before.
     * It's expected that {@link StreamingChangeEventSource#start()} returns quickly, setting of its own asynchronous
     * event handling loop.
     */
    private synchronized void startStreamingEventSource(OffsetContext offset) {
        if (running) {
            streamingSource = changeEventSourceFactory.getStreamingChangeEventSource(offset);
            streamingSource.start();
        }
    }

    /**
     * Stops this coordinator.
     */
    public synchronized void stop() throws InterruptedException {
        running = false;

        // stop the streaming source if it's running
        if (streamingSource != null) {
            try {
                streamingSource.stop();
            }
            catch(InterruptedException e) {
                Thread.interrupted();
                LOGGER.error("Interrupted while stopping streaming event source", e);
            }
        }

        executor.shutdownNow();
        executor.awaitTermination(60, TimeUnit.SECONDS);
    }
}
