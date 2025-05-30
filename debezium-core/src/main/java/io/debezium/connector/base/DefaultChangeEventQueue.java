/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

import static io.debezium.util.Loggings.maybeRedactSensitiveData;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.ConfigurationDefaults;
import io.debezium.pipeline.Sizeable;
import io.debezium.time.Temporals;
import io.debezium.util.Clock;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;

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
public class DefaultChangeEventQueue<T extends Sizeable> extends AbstractChangeEventQueue<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultChangeEventQueue.class);

    private final Duration pollInterval;
    private final int maxBatchSize;
    private final int maxQueueSize;
    private final long maxQueueSizeInBytes;

    private final Lock lock;
    private final Condition isFull;
    private final Condition isNotFull;

    private final Queue<T> queue;
    private final Queue<Long> sizeInBytesQueue;
    private long currentQueueSizeInBytes = 0;

    public DefaultChangeEventQueue(ChangeEventQueueContext context) {
        super(context);
        this.pollInterval = context.getPollInterval();
        this.maxBatchSize = context.getMaxBatchSize();
        this.maxQueueSize = context.getMaxQueueSize();
        this.maxQueueSizeInBytes = context.getMaxQueueSizeInBytes();

        this.lock = new ReentrantLock();
        this.isFull = lock.newCondition();
        this.isNotFull = lock.newCondition();

        this.queue = new ArrayDeque<>(maxQueueSize);
        this.sizeInBytesQueue = new ArrayDeque<>(maxQueueSize);

    }

    @Override
    protected void doEnqueue(T record) throws InterruptedException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Enqueuing source record '{}'", maybeRedactSensitiveData(record));
        }

        try {
            this.lock.lock();

            while (queue.size() >= maxQueueSize || (maxQueueSizeInBytes > 0 && currentQueueSizeInBytes >= maxQueueSizeInBytes)) {
                // signal poll() to drain queue
                this.isFull.signalAll();
                // queue size or queue sizeInBytes threshold reached, so wait a bit
                this.isNotFull.await(pollInterval.toMillis(), TimeUnit.MILLISECONDS);
            }

            queue.add(record);
            // If we pass a positiveLong max.queue.size.in.bytes to enable handling queue size in bytes feature
            if (maxQueueSizeInBytes > 0) {
                long messageSize = record.objectSize();
                sizeInBytesQueue.add(messageSize);
                currentQueueSizeInBytes += messageSize;
            }

            // batch size or queue sizeInBytes threshold reached
            if (queue.size() >= maxBatchSize || (maxQueueSizeInBytes > 0 && currentQueueSizeInBytes >= maxQueueSizeInBytes)) {
                // signal poll() to start draining queue and do not wait
                this.isFull.signalAll();
            }
        }
        finally {
            this.lock.unlock();
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
    public List<T> doPoll() throws InterruptedException {
        final Timer timeout = Threads.timer(Clock.SYSTEM, Temporals.min(pollInterval, ConfigurationDefaults.RETURN_CONTROL_INTERVAL));
        try {
            this.lock.lock();
            List<T> records = new ArrayList<>(Math.min(maxBatchSize, queue.size()));
            throwProducerExceptionIfPresent();
            while (drainRecords(records, maxBatchSize - records.size()) < maxBatchSize
                    && (maxQueueSizeInBytes == 0 || currentQueueSizeInBytes < maxQueueSizeInBytes)
                    && !timeout.expired()) {
                throwProducerExceptionIfPresent();

                LOGGER.debug("no records available or batch size not reached yet, sleeping a bit...");
                long remainingTimeoutMills = timeout.remaining().toMillis();
                if (remainingTimeoutMills > 0) {
                    // signal doEnqueue() to add more records
                    this.isNotFull.signalAll();
                    // no records available or batch size not reached yet, so wait a bit
                    this.isFull.await(remainingTimeoutMills, TimeUnit.MILLISECONDS);
                }
                LOGGER.debug("checking for more records...");
            }
            // signal doEnqueue() to add more records
            this.isNotFull.signalAll();
            return records;
        }
        finally {
            this.lock.unlock();
        }
    }

    private long drainRecords(List<T> records, int maxElements) {
        int queueSize = queue.size();
        if (queueSize == 0) {
            return records.size();
        }
        int recordsToDrain = Math.min(queueSize, maxElements);
        T[] drainedRecords = (T[]) new Sizeable[recordsToDrain];
        for (int i = 0; i < recordsToDrain; i++) {
            T record = queue.poll();
            drainedRecords[i] = record;
        }
        if (maxQueueSizeInBytes > 0) {
            for (int i = 0; i < recordsToDrain; i++) {
                Long objectSize = sizeInBytesQueue.poll();
                currentQueueSizeInBytes -= (objectSize == null ? 0L : objectSize);
            }
        }
        records.addAll(Arrays.asList(drainedRecords));
        return records.size();
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
