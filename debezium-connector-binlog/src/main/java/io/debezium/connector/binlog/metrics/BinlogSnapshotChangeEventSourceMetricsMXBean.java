/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.metrics;

import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetricsMXBean;

/**
 * Binlog-based connector snapshot metrics.
 *
 * @author Chris Cranford
 */
public interface BinlogSnapshotChangeEventSourceMetricsMXBean extends SnapshotChangeEventSourceMetricsMXBean {
    /**
     * Get whether the connector is currently holding a global database block.
     *
     * @return true if there is a global database lock, false otherwise
     */
    boolean getHoldingGlobalLock();
}
