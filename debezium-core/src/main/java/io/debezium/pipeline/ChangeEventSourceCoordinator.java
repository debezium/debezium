/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.source.SourceConnector;

import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.SnapshotResult.SnapshotResultStatus;
import io.debezium.util.Threads;

/**
 * Coordinates one or more {@link ChangeEventSource}s and executes them in order.
 *
 * @author Gunnar Morling
 */
public class ChangeEventSourceCoordinator {

    private final ErrorHandler errorHandler;
    private final ChangeEventSourceFactory changeEventSourceFactory;
    private final ExecutorService executor;
    private final CountDownLatch stopped;

    public ChangeEventSourceCoordinator(ErrorHandler errorHandler, Class<? extends SourceConnector> connectorType, String logicalName, ChangeEventSourceFactory changeEventSourceFactory) {
        this.errorHandler = errorHandler;
        this.changeEventSourceFactory = changeEventSourceFactory;
        this.executor = Threads.newSingleThreadExecutor(connectorType, logicalName, "change-event-source-coordinator");

        this.stopped = new CountDownLatch(1);
    }

    public void start() {
        SnapshotChangeEventSource snapshotSource = changeEventSourceFactory.getSnapshotChangeEventSource();

        CompletableFuture.supplyAsync(() -> snapshotSource.execute(new SnapshotContext()), executor)
            .whenComplete((snapshotResult, throwable) -> {
                // TODO
                if (throwable != null) {
                    errorHandler.setProducerThrowable(throwable);
                }
                else if (snapshotResult.getStatus() == SnapshotResultStatus.COMPLETED) {
                    StreamingChangeEventSource streamingSource = changeEventSourceFactory.getStreamingChangeEventSource(snapshotResult.getOffset());
                    streamingSource.start();

                    try {
                        stopped.await();
                        streamingSource.stop();
                    }
                    catch (InterruptedException e) {
                        Thread.interrupted();
                    }

                }
            })
            .exceptionally(throwable -> {
                errorHandler.setProducerThrowable(throwable);
                return null;
            });
    }

    public void stop() throws InterruptedException {
        stopped.countDown();
        executor.shutdownNow();
        executor.awaitTermination(60, TimeUnit.SECONDS);
    }

    private final class SnapshotContext implements SnapshotChangeEventSource.SnapshotContext {

        @Override
        public boolean isAborted() {
            return stopped.getCount() == 0;
        }
    }
}
