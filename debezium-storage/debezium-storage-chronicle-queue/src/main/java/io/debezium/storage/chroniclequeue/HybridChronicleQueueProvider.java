/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.chroniclequeue;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.base.QueueProvider;
import io.debezium.pipeline.DataChangeEvent;

/**
 * A {@link QueueProvider} implementation that uses an in-memory {@link ArrayDeque} as
 * the primary hot buffer and spills to Chronicle Queue on disk when the deque is full.
 *
 * <p>The deque always holds the most recent N events. When the deque reaches capacity,
 * the oldest event is evicted to Chronicle Queue before the new event is added.
 * Polling drains from Chronicle Queue first (oldest evicted events), then from the deque,
 * preserving strict FIFO ordering.
 *
 * <p>In low-traffic scenarios where the deque never fills, no serialization or disk I/O
 * occurs, giving pure in-memory performance. Under sustained high traffic, the deque
 * remains active as the write buffer while Chronicle Queue absorbs overflow.
 *
 * <p>Configure via the {@link #QUEUE_PATH_PROPERTY} property to specify a directory for
 * queue data files. If not set, a temporary directory is created and cleaned up on close.
 *
 * @author Chris Cranford
 */
public class HybridChronicleQueueProvider implements QueueProvider<DataChangeEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HybridChronicleQueueProvider.class);

    public static final String QUEUE_PATH_PROPERTY = "chronicle.queue.path";

    private final AtomicInteger size = new AtomicInteger();
    private ChronicleQueueHelper chronicleQueueHelper;
    private ArrayDeque<DataChangeEvent> queue;
    private int maxQueueSize;
    private int chronicleQueueSize;

    @Override
    public String name() {
        return "hybrid_chronicle";
    }

    @Override
    public void configure(Map<String, ?> properties) {
        final Configuration config = Configuration.from(properties);
        this.maxQueueSize = config.getInteger(CommonConnectorConfig.MAX_QUEUE_SIZE);
        this.queue = new ArrayDeque<>(maxQueueSize);

        final String path = config.getString(QUEUE_PATH_PROPERTY);
        this.chronicleQueueHelper = new ChronicleQueueHelper(path, "debezium-hybrid-cq-");
        this.size.set(0);
        this.chronicleQueueSize = 0;

        LOGGER.info("Hybrid Chronicle Queue provider initialized at path: {} (in-memory capacity: {})",
                chronicleQueueHelper.getQueuePath(), maxQueueSize);
        if (chronicleQueueHelper.isTemporaryPath()) {
            LOGGER.warn("The queue path is temporary and will be removed on shutdown.");
        }
    }

    @Override
    public void enqueue(DataChangeEvent event) throws InterruptedException {
        if (queue.size() >= maxQueueSize) {
            final DataChangeEvent evicted = queue.pollFirst();
            chronicleQueueHelper.write(evicted);
            chronicleQueueSize++;
        }
        queue.addLast(event);
        size.incrementAndGet();
    }

    @Override
    public DataChangeEvent poll() throws InterruptedException {
        if (chronicleQueueSize > 0) {
            final DataChangeEvent event = chronicleQueueHelper.read();
            if (event != null) {
                chronicleQueueSize--;
                size.decrementAndGet();
                return event;
            }
        }

        final DataChangeEvent event = queue.pollFirst();
        if (event != null) {
            size.decrementAndGet();
        }
        return event;
    }

    @Override
    public int size() {
        return size.get();
    }

    @Override
    public void close() {
        LOGGER.info("Closing Hybrid Chronicle Queue provider at path: {}", chronicleQueueHelper.getQueuePath());
        if (chronicleQueueHelper != null) {
            chronicleQueueHelper.close();
        }
    }
}