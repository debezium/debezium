/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;

/**
 * @author Randall Hauch
 *
 */
class SnapshotReaderMetrics extends SnapshotChangeEventSourceMetrics implements SnapshotReaderMetricsMXBean {

    private final AtomicLong remainingTableCount = new AtomicLong();
    private final AtomicBoolean holdingGlobalLock = new AtomicBoolean();

    private final MySqlSchema schema;

    public SnapshotReaderMetrics(MySqlTaskContext taskContext, MySqlSchema schema) {
        super(taskContext);
        this.schema = schema;
    }

    @Override
    public boolean getHoldingGlobalLock() {
        return holdingGlobalLock.get();
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

    @Override
    public String[] getMonitoredTables() {
        return schema.monitoredTablesAsStringArray();
    }
}
