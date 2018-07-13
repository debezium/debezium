/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.Map;

/**
 * @author Randall Hauch
 */
public interface SnapshotReaderMetricsMXBean extends ReaderMetricsMXBean {

    int getTotalTableCount();
    int getRemainingTableCount();
    boolean getHoldingGlobalLock();
    boolean getSnapshotRunning();
    boolean getSnapshotAborted();
    boolean getSnapshotCompleted();
    long getSnapshotDurationInSeconds();
    Map<String, Long> getRowsScanned();
}
