/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

import io.debezium.spi.common.Configurable;

/**
 * Provides queue operations for change events.
 *
 * @param <T> the type of event stored in the queue
 */
public interface QueueProvider<T> extends Configurable {

    /**
     * Adds an event to the queue.
     *
     * @param event the event to enqueue
     * @throws InterruptedException if interrupted while waiting to enqueue
     */
    void enqueue(T event) throws InterruptedException;

    /**
     * Retrieves and removes the next event from the queue.
     *
     * @return the next event, or null if the queue is empty
     * @throws InterruptedException if interrupted while waiting to poll
     */
    T poll() throws InterruptedException;

    /**
     * Returns the number of events currently in the queue.
     *
     * @return the queue size
     */
    int size();
}