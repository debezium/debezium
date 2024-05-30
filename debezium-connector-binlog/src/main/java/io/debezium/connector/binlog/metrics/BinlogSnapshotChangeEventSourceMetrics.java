/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.metrics;

import java.util.concurrent.atomic.AtomicBoolean;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.binlog.BinlogDatabaseSchema;
import io.debezium.connector.binlog.BinlogPartition;
import io.debezium.connector.binlog.BinlogTaskContext;
import io.debezium.pipeline.metrics.DefaultSnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * Tracks the snapshot metrics for binlog-based connectors.
 *
 * @author Chris Cranford
 */
public class BinlogSnapshotChangeEventSourceMetrics<P extends BinlogPartition>
        extends DefaultSnapshotChangeEventSourceMetrics<P>
        implements BinlogSnapshotChangeEventSourceMetricsMXBean {

    private final AtomicBoolean holdingGlobalLock = new AtomicBoolean();

    public <S extends BinlogDatabaseSchema> BinlogSnapshotChangeEventSourceMetrics(
                                                                                   BinlogTaskContext<S> taskContext,
                                                                                   ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                   EventMetadataProvider metadataProvider) {
        super(taskContext, changeEventQueueMetrics, metadataProvider);
    }

    @Override
    public boolean getHoldingGlobalLock() {
        return holdingGlobalLock.get();
    }

    @Override
    public long getTotalNumberOfEventsSeen() {
        return getRowsScanned().values().stream().mapToLong(x -> x).sum();
    }

    /**
     * Sets that the global database lock has been acquired.
     */
    public void setGlobalLockAcquired() {
        holdingGlobalLock.set(true);
    }

    /**
     * Sets that the global database lock has been released.
     */
    public void setGlobalLockReleased() {
        holdingGlobalLock.set(false);
    }
}
