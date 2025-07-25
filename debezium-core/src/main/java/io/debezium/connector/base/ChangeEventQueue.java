/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

import java.util.List;
import java.util.function.Function;

import io.debezium.annotation.ThreadSafe;
import io.debezium.pipeline.Sizeable;

/**
 * A queue which serves as handover point between producer threads (e.g. MySQL's
 * binlog reader thread) and the Kafka Connect polling loop.
 * <p>
 * The queue is configurable in different aspects, e.g. its maximum size and the
 * time to sleep (block) between two subsequent poll calls. The queue applies back-pressure
 * semantics, i.e. if it holds the maximum number of elements, subsequent calls
 * to {@link #enqueue(T)} will block until elements have been removed from
 * the queue.
 * <p>
 * If an exception occurs on the producer side, the producer should make that
 * exception known by calling {@link #producerException(RuntimeException)} before stopping its
 * operation. Upon the next call to {@link #poll()}, that exception will be
 * raised, causing Kafka Connect to stop the connector and mark it as
 * {@code FAILED}.
 *
 * @author Gunnar Morling
 *
 * @param <T>
 *            the type of events in this queue. Usually {@link Sizeable} is
 *            used, but in cases where additional metadata must be passed from
 *            producers to the consumer, a custom type extending source records
 *            may be used.
 */
@ThreadSafe
public interface ChangeEventQueue<T extends Sizeable> extends ChangeEventQueueMetrics {

    /**
     * Enqueues a record so that it can be obtained via {@link #poll()}. This method
     * will block if the queue is full.
     *
     * @param record
     *            the record to be enqueued
     * @throws InterruptedException
     *             if this thread has been interrupted
     */
    void enqueue(T record) throws InterruptedException;

    /**
     * Applies a function to the event and the buffer and adds it to the queue. Buffer is emptied.
     *
     * @param recordModifier
     * @throws InterruptedException
     */
    void flushBuffer(Function<T, T> recordModifier) throws InterruptedException;

    /**
     * Disable buffering for the queue
     */
    void disableBuffering();

    /**
     * Enable buffering for the queue
     */
    void enableBuffering();

    boolean isBuffered();

    /**
     * Returns the next batch of elements from this queue. May be empty in case no
     * elements have arrived in the maximum waiting time.
     *
     * @throws InterruptedException
     *             if this thread has been interrupted while waiting for more
     *             elements to arrive
     */
    List<T> poll() throws InterruptedException;

    void producerException(RuntimeException producerException);
}
