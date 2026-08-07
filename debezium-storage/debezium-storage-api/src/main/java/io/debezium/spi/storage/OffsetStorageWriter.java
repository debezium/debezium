/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi.storage;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import io.debezium.common.annotation.Incubating;

/**
 * Interface for writing offset data to storage.
 * This interface provides write access to connector offsets with flush control.
 *
 * @author Debezium Authors
 */
@Incubating
public interface OffsetStorageWriter {

    /**
     * Record an offset to be written.
     * <p>
     * The offset is not immediately persisted; it will be written when {@link #doFlush(OffsetStore.Callback)}
     * is called.
     *
     * @param partition the partition map (typically contains table/collection identifiers)
     * @param offset the offset map (typically contains position information like LSN, binlog position, etc.)
     */
    void offset(Map<String, ?> partition, Map<String, ?> offset);

    /**
     * Begin the flush process with a timeout.
     * <p>
     * This prepares pending offsets for flushing but does not actually persist them yet.
     * Call {@link #doFlush(OffsetStore.Callback)} to actually write the offsets.
     *
     * @param timeout the maximum time to wait
     * @param timeUnit the time unit of the timeout
     * @return true if there are offsets to flush, false if there's nothing to flush
     */
    boolean beginFlush(long timeout, TimeUnit timeUnit);

    /**
     * Cancel an in-progress flush.
     * <p>
     * This should be called if an error occurs during the flush process.
     */
    void cancelFlush();

    /**
     * Actually flush the pending offsets to storage.
     * <p>
     * Must be called after {@link #beginFlush(long, TimeUnit)}.
     *
     * @param callback optional callback to invoke when flush completes (may be null)
     * @return a Future that completes when the flush is done
     */
    Future<Void> doFlush(OffsetStore.Callback<Void> callback);
}
