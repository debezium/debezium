/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.Sizeable;

/**
 * Default implementation of {@link QueueProvider} using an {@link ArrayDeque}.
 *
 * @param <T> the type of elements in the queue, must implement {@link Sizeable}
 */
public class DefaultQueueProvider<T extends Sizeable> implements QueueProvider<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultQueueProvider.class);

    /** Internal queue for storing events */
    private final Queue<T> queue;

    /**
     * Constructs a DefaultQueueProvider with the specified maximum queue size.
     *
     * @param maxQueueSize the initial capacity of the queue
     */
    public DefaultQueueProvider(int maxQueueSize) {
        this.queue = new ArrayDeque<>(maxQueueSize);
    }

    /**
     * Configures the queue provider with custom properties.
     * Queue initialization logic can be added here when supporting custom configuration-based queue providers.
     *
     * @param properties configuration properties
     */
    @Override
    public void configure(Map<String, ?> properties) {
        // Queue initialization logic can be added here when supporting custom configuration-based queue providers.
    }

    /**
     * Adds a record to the queue.
     *
     * @param record the event to enqueue
     * @throws InterruptedException if interrupted while waiting to enqueue
     */
    @Override
    public void enqueue(T record) throws InterruptedException {
        queue.add(record);
    }

    /**
     * Retrieves and removes the next event from the queue.
     *
     * @return the next event, or null if the queue is empty
     * @throws InterruptedException if interrupted while waiting to poll
     */
    @Override
    public T poll() throws InterruptedException {
        return queue.poll();
    }

    /**
     * Returns the number of events currently in the queue.
     *
     * @return the queue size
     */
    @Override
    public int size() {
        return queue.size();
    }
}
