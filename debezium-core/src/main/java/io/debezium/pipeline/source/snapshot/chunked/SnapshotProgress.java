/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.chunked;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Thread-safe coordination for global snapshot progress across all tables.
 * Ensures correct emission ordering of FIRST and LAST snapshot markers.
 *
 * @author Chris Cranford
 */
public class SnapshotProgress {

    private final int tableCount;
    private final CountDownLatch firstRecordLatch;
    private final CountDownLatch tablesBeforeLastLatch;

    /**
     * Creates a new SnapshotProgress instance.
     *
     * @param tableCount the total number of tables in the snapshot
     */
    public SnapshotProgress(int tableCount) {
        this.tableCount = tableCount;
        this.firstRecordLatch = new CountDownLatch(1);
        // All tables except the last one must signal completion before LAST can be emitted
        this.tablesBeforeLastLatch = new CountDownLatch(Math.max(0, tableCount - 1));
    }

    /**
     * Waits for the global FIRST record to be emitted.
     * Called by all records except the FIRST record before emission.
     *
     * @throws InterruptedException if the wait is interrupted
     */
    public void waitForFirstRecord() throws InterruptedException {
        firstRecordLatch.await();
    }

    /**
     * Waits for the global FIRST record with a timeout.
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
     * Signals that the global FIRST record has been emitted.
     * Called by the chunk processing the first record of the first table.
     */
    public void signalFirstRecordEmitted() {
        firstRecordLatch.countDown();
    }

    /**
     * Waits for all tables except the last one to complete.
     * Called by the LAST record before emission.
     *
     * @throws InterruptedException if the wait is interrupted
     */
    public void waitForOtherTables() throws InterruptedException {
        tablesBeforeLastLatch.await();
    }

    /**
     * Waits for all tables except the last one with a timeout.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit
     * @return true if all tables completed, false if timeout elapsed
     * @throws InterruptedException if the wait is interrupted
     */
    public boolean waitForOtherTables(long timeout, TimeUnit unit) throws InterruptedException {
        return tablesBeforeLastLatch.await(timeout, unit);
    }

    /**
     * Signals that a table has completed all its records (including LAST_IN_DATA_COLLECTION).
     * Called by non-last tables after emitting their final record.
     */
    public void signalTableComplete() {
        tablesBeforeLastLatch.countDown();
    }

    /**
     * @return the total number of tables in the snapshot
     */
    public int getTableCount() {
        return tableCount;
    }
}