/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

/**
 * @author Randall Hauch
 */
public interface SnapshotReaderMetricsMXBean {

    int getTotalTableCount();
    int getRemainingTableCount();
    boolean getHoldingGlobalLock();
    boolean getSnapshotRunning();
    boolean getSnapshotAborted();
    boolean getSnapshotCompleted();
    long getSnapshotDurationInSeconds();
}
