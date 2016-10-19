/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import io.debezium.util.Clock;

/**
 * @author Randall Hauch
 *
 */
class SnapshotReaderMetrics extends Metrics implements SnapshotReaderMetricsMXBean {

    private final AtomicLong tableCount = new AtomicLong();
    private final AtomicLong remainingTableCount = new AtomicLong();
    private final AtomicBoolean holdingGlobalLock = new AtomicBoolean();
    private final AtomicBoolean snapshotRunning = new AtomicBoolean();
    private final AtomicBoolean snapshotCompleted = new AtomicBoolean();
    private final AtomicBoolean snapshotAborted = new AtomicBoolean();
    private final AtomicLong startTime = new AtomicLong();
    private final AtomicLong stopTime = new AtomicLong();
    
    private final Clock clock;
    
    public SnapshotReaderMetrics(Clock clock) {
        super("snapshot");
        this.clock= clock;
    }
    
    @Override
    public int getTotalTableCount() {
        return this.tableCount.intValue();
    }

    @Override
    public int getRemainingTableCount() {
        return this.remainingTableCount.intValue();
    }
    
    @Override
    public boolean getSnapshotRunning() {
        return this.snapshotRunning.get();
    }
    
    @Override
    public boolean getSnapshotCompleted() {
        return this.snapshotCompleted.get();
    }
    
    @Override
    public boolean getSnapshotAborted() {
        return this.snapshotAborted.get();
    }
    
    @Override
    public boolean getHoldingGlobalLock() {
        return holdingGlobalLock.get();
    }
    
    @Override
    public long getSnapshotDurationInSeconds() {
        long startMillis = startTime.get();
        if ( startMillis <= 0L) {
            return 0;
        }
        long stopMillis = stopTime.get();
        if ( stopMillis == 0L ) stopMillis = clock.currentTimeInMillis();
        return (stopMillis - startMillis)/1000L;
    }
    
    public void globalLockAcquired() {
        holdingGlobalLock.set(true);
    }
    
    public void globalLockReleased() {
        holdingGlobalLock.set(false);
    }
    
    public void setTableCount(int tableCount) {
        this.tableCount.set(tableCount);
        this.remainingTableCount.set(tableCount);
    }
    
    public void completeTable() {
        remainingTableCount.decrementAndGet();
    }
    
    public void startSnapshot() {
        this.snapshotRunning.set(true);
        this.snapshotCompleted.set(false);
        this.snapshotAborted.set(false);
        this.startTime.set(clock.currentTimeInMillis());
        this.stopTime.set(0L);
    }
    
    public void completeSnapshot() {
        this.snapshotCompleted.set(true);
        this.snapshotAborted.set(false);
        this.snapshotRunning.set(false);
        this.stopTime.set(clock.currentTimeInMillis());
    }
    
    public void abortSnapshot() {
        this.snapshotCompleted.set(false);
        this.snapshotAborted.set(true);
        this.snapshotRunning.set(false);
        this.stopTime.set(clock.currentTimeInMillis());
    }
}
