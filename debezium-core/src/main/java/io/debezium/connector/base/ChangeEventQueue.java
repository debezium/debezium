/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.SingleThreadAccess;
import io.debezium.annotation.ThreadSafe;
import io.debezium.config.ConfigurationDefaults;
import io.debezium.time.Temporals;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.LoggingContext.PreviousContext;
import io.debezium.util.ObjectSizeCalculator;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;

/**
 * A queue which serves as handover point between producer threads (e.g. MySQL's
 * binlog reader thread) and the Kafka Connect polling loop.
 * <p>
 * The queue is configurable in different aspects, e.g. its maximum size and the
 * time to sleep (block) between two subsequent poll calls. See the
 * {@link Builder} for the different options. The queue applies back-pressure
 * semantics, i.e. if it holds the maximum number of elements, subsequent calls
 * to {@link #enqueue(Object)} will block until elements have been removed from
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
 *            the type of events in this queue. Usually {@link SourceRecord} is
 *            used, but in cases where additional metadata must be passed from
 *            producers to the consumer, a custom type wrapping source records
 *            may be used.
 */
@ThreadSafe
public class ChangeEventQueue<T> implements ChangeEventQueueMetrics {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEventQueue.class);

    private final Duration pollInterval;
    private final int maxBatchSize;
    private final int maxQueueSize;
    private final long maxQueueSizeInBytes;
    private final Queue<T> queue;
    private final Supplier<PreviousContext> loggingContextSupplier;
    private final Queue<Long> sizeInBytesQueue;
    private long currentQueueSizeInBytes = 0;

    // Sometimes it is necessary to update the record before it is delivered depending on the content
    // of the following record. In that cases the easiest solution is to provide a single cell buffer
    // that will allow the modification of it during the explicit flush.
    // Typical example is MySQL connector when sometimes it is impossible to detect when the record
    // in process is the last one. In this case the snapshot flags are set during the explicit flush.
    @SingleThreadAccess("producer thread")
    private boolean buffering;

    @SingleThreadAccess("producer thread")
    private T bufferedEvent;

    private volatile RuntimeException producerException;

    private ChangeEventQueue(Duration pollInterval, int maxQueueSize, int maxBatchSize, Supplier<LoggingContext.PreviousContext> loggingContextSupplier,
                             long maxQueueSizeInBytes, boolean buffering) {
        this.pollInterval = pollInterval;
        this.maxBatchSize = maxBatchSize;
        this.maxQueueSize = maxQueueSize;
        this.queue = new ArrayDeque<>(maxQueueSize);
        this.loggingContextSupplier = loggingContextSupplier;
        this.sizeInBytesQueue = new ArrayDeque<>(maxQueueSize);
        this.maxQueueSizeInBytes = maxQueueSizeInBytes;
        this.buffering = buffering;
    }

    public static class Builder<T> {

        private Duration pollInterval;
        private int maxQueueSize;
        private int maxBatchSize;
        private Supplier<LoggingContext.PreviousContext> loggingContextSupplier;
        private long maxQueueSizeInBytes;
        private boolean buffering;

        public Builder<T> pollInterval(Duration pollInterval) {
            this.pollInterval = pollInterval;
            return this;
        }

        public Builder<T> maxQueueSize(int maxQueueSize) {
            this.maxQueueSize = maxQueueSize;
            return this;
        }

        public Builder<T> maxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public Builder<T> loggingContextSupplier(Supplier<LoggingContext.PreviousContext> loggingContextSupplier) {
            this.loggingContextSupplier = loggingContextSupplier;
            return this;
        }

        public Builder<T> maxQueueSizeInBytes(long maxQueueSizeInBytes) {
            this.maxQueueSizeInBytes = maxQueueSizeInBytes;
            return this;
        }

        public Builder<T> buffering() {
            this.buffering = true;
            return this;
        }

        public ChangeEventQueue<T> build() {
            return new ChangeEventQueue<T>(pollInterval, maxQueueSize, maxBatchSize, loggingContextSupplier, maxQueueSizeInBytes, buffering);
        }
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
    public void enqueue(T record) throws InterruptedException {
        if (record == null) {
            return;
        }

        // The calling thread has been interrupted, let's abort
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }

        if (buffering) {
            final T newEvent = record;
            record = bufferedEvent;
            bufferedEvent = newEvent;
            if (record == null) {
                // Can happen only for the first coming event
                return;
            }
        }

        doEnqueue(record);
    }

    /**
     * Applies a function to the event and the buffer and adds it to the queue. Buffer is emptied.
     *
     * @param recordModifier
     * @throws InterruptedException
     */
    public void flushBuffer(Function<T, T> recordModifier) throws InterruptedException {
        assert buffering : "Unsuported for queues with disabled buffering";
        if (bufferedEvent != null) {
            doEnqueue(recordModifier.apply(bufferedEvent));
            bufferedEvent = null;
        }
    }

    /**
     * Disable buffering for the queue
     */
    public void disableBuffering() {
        assert bufferedEvent == null : "Buffer must be flushed";
        buffering = false;
    }

    protected void doEnqueue(T record) throws InterruptedException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Enqueuing source record '{}'", record);
        }

        synchronized (this) {
            while (queue.size() >= maxQueueSize || (maxQueueSizeInBytes > 0 && currentQueueSizeInBytes >= maxQueueSizeInBytes)) {
                // notify poll() to drain queue
                this.notify();
                // queue size or queue sizeInBytes threshold reached, so wait a bit
                this.wait(pollInterval.toMillis());
            }

            queue.add(record);
            // If we pass a positiveLong max.queue.size.in.bytes to enable handling queue size in bytes feature
            if (maxQueueSizeInBytes > 0) {
                long messageSize = ObjectSizeCalculator.getObjectSize(record);
                sizeInBytesQueue.add(messageSize);
                currentQueueSizeInBytes += messageSize;
            }

            if (queue.size() >= maxBatchSize || (maxQueueSizeInBytes > 0 && currentQueueSizeInBytes >= maxQueueSizeInBytes)) {
                // notify poll() to start draining queue and do not wait
                this.notify();
            }
        }
    }

    /**
     * Returns the next batch of elements from this queue. May be empty in case no
     * elements have arrived in the maximum waiting time.
     *
     * @throws InterruptedException
     *             if this thread has been interrupted while waiting for more
     *             elements to arrive
     */
    public List<T> poll() throws InterruptedException {
        LoggingContext.PreviousContext previousContext = loggingContextSupplier.get();

        try {
            LOGGER.debug("polling records...");
            final Timer timeout = Threads.timer(Clock.SYSTEM, Temporals.min(pollInterval, ConfigurationDefaults.RETURN_CONTROL_INTERVAL));
            synchronized (this) {
                List<T> records = new ArrayList<>(Math.min(maxBatchSize, queue.size()));
                while (drainRecords(records, maxBatchSize - records.size()) < maxBatchSize
                        && (maxQueueSizeInBytes == 0 || currentQueueSizeInBytes < maxQueueSizeInBytes)
                        && !timeout.expired()) {
                    throwProducerExceptionIfPresent();

                    LOGGER.debug("no records available yet, sleeping a bit...");
                    long remainingTimeoutMills = timeout.remaining().toMillis();
                    if (remainingTimeoutMills > 0) {
                        // notify doEnqueue() to resume processing (if anything is on wait())
                        this.notify();
                        // no records yet, so wait a bit
                        this.wait(remainingTimeoutMills);
                    }
                    LOGGER.debug("checking for more records...");
                }
                // notify doEnqueue() to resume processing
                this.notify();
                return records;
            }
        }
        finally {
            previousContext.restore();
        }
    }

    private long drainRecords(List<T> records, int maxElements) {
        int queueSize = queue.size();
        if (queueSize == 0) {
            return records.size();
        }
        int recordsToDrain = Math.min(queueSize, maxElements);
        T[] drainedRecords = (T[]) new Object[recordsToDrain];
        for (int i = 0; i < recordsToDrain; i++) {
            T record = queue.poll();
            drainedRecords[i] = record;
        }
        if (maxQueueSizeInBytes > 0) {
            for (int i = 0; i < recordsToDrain; i++) {
                long objectSize = sizeInBytesQueue.poll();
                currentQueueSizeInBytes -= objectSize;
            }
        }
        records.addAll(Arrays.asList(drainedRecords));
        return records.size();
    }

    public void producerException(final RuntimeException producerException) {
        this.producerException = producerException;
    }

    private void throwProducerExceptionIfPresent() {
        if (producerException != null) {
            throw producerException;
        }
    }

    @Override
    public int totalCapacity() {
        return maxQueueSize;
    }

    @Override
    public int remainingCapacity() {
        return maxQueueSize - queue.size();
    }

    @Override
    public long maxQueueSizeInBytes() {
        return maxQueueSizeInBytes;
    }

    @Override
    public long currentQueueSizeInBytes() {
        return currentQueueSizeInBytes;
    }
}
