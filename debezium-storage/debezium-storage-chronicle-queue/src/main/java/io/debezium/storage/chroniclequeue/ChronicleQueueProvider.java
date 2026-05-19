/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.chroniclequeue;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.base.QueueProvider;
import io.debezium.pipeline.DataChangeEvent;

/**
 * A {@link QueueProvider} implementation backed by Chronicle Queue that spills change events
 * to memory-mapped files on disk, reducing heap pressure during high-throughput periods.
 *
 * <p>Configure via the {@link #QUEUE_PATH_PROPERTY} property to specify a directory for
 * queue data files. If not set, a temporary directory is created and cleaned up on close.
 *
 * @author Chris Cranford
 */
public class ChronicleQueueProvider implements QueueProvider<DataChangeEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChronicleQueueProvider.class);

    public static final String QUEUE_PATH_PROPERTY = "chronicle.queue.path";

    private final AtomicInteger size = new AtomicInteger();
    private ChronicleQueueHelper chronicleQueueHelper;

    @Override
    public String name() {
        return "chronicle";
    }

    @Override
    public void configure(Map<String, ?> properties) {
        final Configuration config = Configuration.from(properties);
        final String path = config.getString(QUEUE_PATH_PROPERTY);

        this.chronicleQueueHelper = new ChronicleQueueHelper(path, "debezium-cq-");
        this.size.set(0);

        LOGGER.info("Chronicle Queue provider initialized at path: {}", chronicleQueueHelper.getQueuePath());
        if (chronicleQueueHelper.isTemporaryPath()) {
            LOGGER.warn("The queue path is temporary and will be removed on shutdown.");
        }
    }

    @Override
    public void enqueue(DataChangeEvent event) throws InterruptedException {
        chronicleQueueHelper.write(event);
        size.incrementAndGet();
    }

    @Override
    public DataChangeEvent poll() throws InterruptedException {
        final DataChangeEvent event = chronicleQueueHelper.read();
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
        LOGGER.info("Closing Chronicle Queue provider at path: {}", chronicleQueueHelper.getQueuePath());
        if (chronicleQueueHelper != null) {
            chronicleQueueHelper.close();
        }
    }
}