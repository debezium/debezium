/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Supplier;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.ConfigurationDefaults;
import io.debezium.time.Temporals;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.LoggingContext.PreviousContext;
import io.debezium.util.Metronome;
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
 * exception known by calling {@link #producerFailure} before stopping its
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
public class ChangeEventQueue<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEventQueue.class);

    private final Duration pollInterval;
    private final int maxBatchSize;
    private final BlockingQueue<T> queue;
    private final Metronome metronome;
    private final Supplier<PreviousContext> loggingContextSupplier;

    private volatile Throwable producerFailure;

    private ChangeEventQueue(Duration pollInterval, int maxQueueSize, int maxBatchSize, Supplier<LoggingContext.PreviousContext> loggingContextSupplier) {
        this.pollInterval = pollInterval;
        this.maxBatchSize = maxBatchSize;
        this.queue = new LinkedBlockingDeque<>(maxQueueSize);
        this.metronome = Metronome.sleeper(pollInterval, Clock.SYSTEM);
        this.loggingContextSupplier = loggingContextSupplier;
    }

    public static class Builder<T> {

        private Duration pollInterval;
        private int maxQueueSize;
        private int maxBatchSize;
        private Supplier<LoggingContext.PreviousContext> loggingContextSupplier;

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

        public ChangeEventQueue<T> build() {
            return new ChangeEventQueue<T>(pollInterval, maxQueueSize, maxBatchSize, loggingContextSupplier);
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

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Enqueuing source record '{}'", record);
        }

        // this will also raise an InterruptedException if the thread is interrupted while waiting for space in the queue
        queue.put(record);
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
            List<T> records = new ArrayList<>();
            final Timer timeout = Threads.timer(Clock.SYSTEM, Temporals.max(pollInterval, ConfigurationDefaults.RETURN_CONTROL_INTERVAL));
            while (!timeout.expired() && queue.drainTo(records, maxBatchSize) == 0) {
                throwProducerFailureIfPresent();

                LOGGER.debug("no records available yet, sleeping a bit...");
                // no records yet, so wait a bit
                metronome.pause();
                LOGGER.debug("checking for more records...");
            }
            return records;
        } finally {
            previousContext.restore();
        }
    }

    public void producerFailure(final Throwable producerFailure) {
        this.producerFailure = producerFailure;
    }

    private void throwProducerFailureIfPresent() {
        if (producerFailure != null) {
            throw new ConnectException("An exception ocurred in the change event producer. This connector will be stopped.", producerFailure);
        }
    }
}
