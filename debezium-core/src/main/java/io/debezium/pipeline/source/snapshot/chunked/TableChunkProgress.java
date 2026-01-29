/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.chunked;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.debezium.relational.TableId;

/**
 * Thread-safe progress tracking for chunked table snapshots across all table chunks.
 *
 * @author Chris Cranford
 */
public class TableChunkProgress {

    private final TableId tableId;
    private final int totalChunks;
    private final AtomicInteger completedChunks;
    private final AtomicLong totalRowsScanned;

    // Coordination primitives for emission ordering
    private final CountDownLatch firstRecordLatch;
    private final CountDownLatch chunksBeforeLastLatch;

    public TableChunkProgress(TableId tableId, int totalChunks) {
        this.tableId = tableId;
        this.totalChunks = totalChunks;
        this.completedChunks = new AtomicInteger(0);
        this.totalRowsScanned = new AtomicLong(0);

        // Coordination latches
        this.firstRecordLatch = new CountDownLatch(1);
        // All chunks except the last one must signal completion before LAST_IN_DATA_COLLECTION can be emitted
        this.chunksBeforeLastLatch = new CountDownLatch(Math.max(0, totalChunks - 1));
    }

    public void markChunkComplete(long rowsScanned) {
        totalRowsScanned.addAndGet(rowsScanned);
        completedChunks.incrementAndGet();
    }

    public boolean isTableComplete() {
        return completedChunks.get() == totalChunks;
    }

    public long getTotalRowsScanned() {
        return totalRowsScanned.get();
    }

    public int getCompletedChunks() {
        return completedChunks.get();
    }

    public int getTotalChunks() {
        return totalChunks;
    }

    public TableId getTableId() {
        return tableId;
    }

    // Coordination methods for emission ordering

    /**
     * Waits for the first record of this table (FIRST_IN_DATA_COLLECTION) to be emitted.
     * Called by all records except the first record before emission.
     *
     * @throws InterruptedException if the wait is interrupted
     */
    public void waitForFirstRecord() throws InterruptedException {
        firstRecordLatch.await();
    }

    /**
     * Waits for the first record with a timeout.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit
     * @return true if the latch was signaled, false if timeout elapsed
     * @throws InterruptedException if the wait is interrupted
     */
    public boolean waitForFirstRecord(long timeout, TimeUnit unit) throws InterruptedException {
        return firstRecordLatch.await(timeout, unit);
    }

    /**
     * Signals that the first record of this table has been emitted.
     * Called by the chunk processing the first record (chunk 0).
     */
    public void signalFirstRecordEmitted() {
        firstRecordLatch.countDown();
    }

    /**
     * Waits for all chunks except the last one to complete.
     * Called by the LAST_IN_DATA_COLLECTION record before emission.
     *
     * @throws InterruptedException if the wait is interrupted
     */
    public void waitForOtherChunks() throws InterruptedException {
        chunksBeforeLastLatch.await();
    }

    /**
     * Waits for all chunks except the last one with a timeout.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit
     * @return true if all chunks completed, false if timeout elapsed
     * @throws InterruptedException if the wait is interrupted
     */
    public boolean waitForOtherChunks(long timeout, TimeUnit unit) throws InterruptedException {
        return chunksBeforeLastLatch.await(timeout, unit);
    }

    /**
     * Signals that a non-last chunk has completed all its records.
     * Called by chunks 0 through N-2 after processing all their records.
     */
    public void signalChunkComplete() {
        chunksBeforeLastLatch.countDown();
    }
}
