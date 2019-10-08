/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import java.util.Map;

/**
 * @author Randall Hauch, Jiri Pechanec
 */
public interface SnapshotChangeEventSourceMetricsMXBean extends ChangeEventSourceMetricsMXBean {

    int getTotalTableCount();

    int getRemainingTableCount();

    boolean getSnapshotRunning();

    boolean getSnapshotAborted();

    boolean getSnapshotCompleted();

    long getSnapshotDurationInSeconds();

    Map<String, Long> getRowsScanned();
}
