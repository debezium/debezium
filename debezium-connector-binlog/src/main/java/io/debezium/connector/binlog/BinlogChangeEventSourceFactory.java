/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.util.function.Function;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;

/**
 * Abstract base class for binlog-based ChangeEventSourceFactory implementations.
 * This class provides common functionality for MySQL and MariaDB connectors,
 * including thread-safe snapshot cleanup that prevents memory leaks.
 *
 * @author Yashi Srivastava
 */
public abstract class BinlogChangeEventSourceFactory<P extends Partition, O extends OffsetContext>
        implements ChangeEventSourceFactory<P, O> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogChangeEventSourceFactory.class);

    protected final ChangeEventQueue<DataChangeEvent> queue;

    public BinlogChangeEventSourceFactory(ChangeEventQueue<DataChangeEvent> queue) {
        this.queue = queue;
    }

    protected void preSnapshot() {
        queue.enableBuffering();
    }

    protected void modifyAndFlushLastRecord(Function<SourceRecord, SourceRecord> modify) throws InterruptedException {
        // Check if the current thread has been interrupted before attempting to flush
        if (Thread.currentThread().isInterrupted()) {
            LOGGER.info("Thread has been interrupted, skipping flush of buffered record");
            queue.disableBuffering();
            throw new InterruptedException("Thread interrupted during snapshot cleanup");
        }

        try {
            // Attempt to flush the buffered record
            // If queue is shut down, this will throw InterruptedException and we'll handle it gracefully
            queue.flushBuffer(dataChange -> new DataChangeEvent(modify.apply(dataChange.getRecord())));
            LOGGER.debug("Successfully flushed buffered record during snapshot cleanup");
        }
        catch (InterruptedException e) {
            // Queue was shut down or thread was interrupted - this is expected during task shutdown
            LOGGER.info("Buffered record flush interrupted during snapshot cleanup, likely due to task shutdown");
            throw e;
        }
        finally {
            // Always disable buffering to prevent memory leaks
            // Use the safe version that handles non-empty buffers gracefully during shutdown
            queue.disableBufferingSafely();
        }
    }
}
