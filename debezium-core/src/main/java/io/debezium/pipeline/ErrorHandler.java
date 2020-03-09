/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.base.ChangeEventQueue;

public class ErrorHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ErrorHandler.class);

    private final ChangeEventQueue<?> queue;
    private final AtomicReference<Throwable> producerThrowable;

    public ErrorHandler(Class<? extends SourceConnector> connectorType, String logicalName, ChangeEventQueue<?> queue) {
        this.queue = queue;
        this.producerThrowable = new AtomicReference<>();
    }

    public void setProducerThrowable(Throwable producerThrowable) {
        LOGGER.error("Producer failure", producerThrowable);

        boolean first = this.producerThrowable.compareAndSet(null, producerThrowable);

        if (first) {
            queue.producerFailure(producerThrowable);
        }
    }

    public Throwable getProducerThrowable() {
        return producerThrowable.get();
    }
}
