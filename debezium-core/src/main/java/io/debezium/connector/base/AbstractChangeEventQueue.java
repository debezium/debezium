/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.SingleThreadAccess;
import io.debezium.pipeline.Sizeable;
import io.debezium.util.LoggingContext;

public abstract class AbstractChangeEventQueue<T extends Sizeable> implements ChangeEventQueue<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractChangeEventQueue.class);

    private final ChangeEventQueueConfig changeEventQueueConfig;

    @SingleThreadAccess("producer thread")
    protected boolean buffering;

    protected final AtomicReference<T> bufferedEvent = new AtomicReference<>();

    protected volatile RuntimeException producerException;

    public AbstractChangeEventQueue(ChangeEventQueueConfig changeEventQueueConfig) {
        this.changeEventQueueConfig = changeEventQueueConfig;
        this.buffering = changeEventQueueConfig.isBuffering();
    }

    /**
     * Enqueues a record so that it can be obtained via {@link #poll()}. This method
     * will block if the queue is full.
     *
     * @param record
     *            the record to be enqueued
     * @throws InterruptedException
     *             if this thread has been interrupted
     */
    @Override
    public void enqueue(T record) throws InterruptedException {
        if (record == null) {
            return;
        }

        // The calling thread has been interrupted, let's abort
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }

        if (buffering) {
            record = bufferedEvent.getAndSet(record);
            if (record == null) {
                // Can happen only for the first coming event
                return;
            }
        }

        doEnqueue(record);
    }

    /**
     * Returns the next batch of elements from this queue. May be empty in case no
     * elements have arrived in the maximum waiting time.
     *
     * @throws InterruptedException
     *             if this thread has been interrupted while waiting for more
     *             elements to arrive
     */
    @Override
    public List<T> poll() throws InterruptedException {
        LoggingContext.PreviousContext previousContext = changeEventQueueConfig.getLoggingContextSupplier().get();

        try {
            LOGGER.debug("polling records...");
            return doPoll();
        }
        finally {
            previousContext.restore();
        }
    }

    protected abstract void doEnqueue(T record) throws InterruptedException;

    public abstract List<T> doPoll() throws InterruptedException;

    @Override
    public void producerException(final RuntimeException producerException) {
        this.producerException = producerException;
    }

    @Override
    public boolean isBuffered() {
        return buffering;
    }

    /**
     * Applies a function to the event and the buffer and adds it to the queue. Buffer is emptied.
     *
     * @param recordModifier
     * @throws InterruptedException
     */
    public void flushBuffer(Function<T, T> recordModifier) throws InterruptedException {
        assert buffering : "Unsupported for queues with disabled buffering";
        T record = bufferedEvent.getAndSet(null);
        if (record != null) {
            doEnqueue(recordModifier.apply(record));
        }
    }

    /**
     * Disable buffering for the queue
     */
    @Override
    public void disableBuffering() {
        assert bufferedEvent.get() == null : "Buffer must be flushed";
        buffering = false;
    }

    /**
     * Enable buffering for the queue
     */
    @Override
    public void enableBuffering() {
        buffering = true;
    }

    protected void throwProducerExceptionIfPresent() {
        if (producerException != null) {
            throw producerException;
        }
    }

    @Override
    public int totalCapacity() {
        return -1;
    }

    @Override
    public int remainingCapacity() {
        return -1;
    }

    @Override
    public long maxQueueSizeInBytes() {
        return -1;
    }

    @Override
    public long currentQueueSizeInBytes() {
        return -1;
    }

}
