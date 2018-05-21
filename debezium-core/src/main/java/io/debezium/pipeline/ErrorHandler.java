/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.util.Threads;

public class ErrorHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ErrorHandler.class);

    private final ChangeEventQueue<?> queue;
    private final Runnable onThrowable;
    private final AtomicReference<Throwable> producerThrowable;
    private final ExecutorService executor;

    public ErrorHandler(Class<? extends SourceConnector> connectorType, String logicalName, ChangeEventQueue<?> queue, Runnable onThrowable) {
        this.queue = queue;
        this.onThrowable = onThrowable;
        this.executor = Threads.newSingleThreadExecutor(connectorType, logicalName, "error-handler");
        this.producerThrowable = new AtomicReference<>();
    }

    public void setProducerThrowable(Throwable producerThrowable) {
        LOGGER.error("Producer failure", producerThrowable);

        boolean first = this.producerThrowable.compareAndSet(null, producerThrowable);

        if (first) {
            queue.producerFailure(producerThrowable);
            executor.execute(() -> onThrowable.run());
        }
    }

    public Throwable getProducerThrowable() {
        return producerThrowable.get();
    }

    public void stop() throws InterruptedException {
        executor.shutdownNow();
        executor.awaitTermination(60, TimeUnit.SECONDS);
    }
}
