/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import io.debezium.connector.cassandra.exceptions.CassandraConnectorTaskException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * This queue stores the change events sent from the readers and gets processed by {@link QueueProcessor}
 * where the events will get emitted to kafka.
 */
public class BlockingEventQueue<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BlockingEventQueue.class);

    private static final int SLEEP_MS = 100;
    private final Duration pollInterval;
    private final int maxBatchSize;
    private final java.util.concurrent.BlockingQueue queue;

    public BlockingEventQueue(Duration pollInterval, int maxQueueSize, int maxBatchSize) {
        this.pollInterval = pollInterval;
        this.maxBatchSize = maxBatchSize;
        this.queue = new LinkedBlockingDeque<>(maxQueueSize);
    }

    public void enqueue(T event) {
        try {
            queue.put(event);
        }
        catch (InterruptedException e) {
            LOGGER.error("Interruption while enqueuing event {}", event);
            throw new CassandraConnectorTaskException("Enqueuing has been interrupted: ", e);
        }
    }

    public List<T> poll() throws InterruptedException {
        LOGGER.debug("Begin polling events...");
        List<T> events = new ArrayList<>();
        long then = System.currentTimeMillis();
        while (queue.drainTo(events, maxBatchSize) == 0) {
            // Add a bit of buffer between polls so it isn't too busy looping
            Thread.sleep(SLEEP_MS);
            long now = System.currentTimeMillis();
            if (now - then >= pollInterval.toMillis()) {
                LOGGER.debug("Polling interval exceeded, returning empty-handed...");
                break;
            }
        }
        LOGGER.debug("Polled {} events", events.size());
        return events;
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public int size() {
        return queue.size();
    }
}
