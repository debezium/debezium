/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.pipeline.metrics;

import java.util.concurrent.atomic.AtomicLong;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.metrics.Metrics;

/**
 * Metrics for task-level state that is shared across different connector phases
 * (snapshot, streaming, schema recovery, etc.).
 */
@ThreadSafe
public class TaskStateMetrics extends Metrics implements TaskStateMetricsMXBean {

    private final AtomicLong connectTaskDnd = new AtomicLong();

    public TaskStateMetrics(CdcSourceTaskContext taskContext) {
        super(taskContext, "task");
    }

    @Override
    public long getConnectTaskDnd() {
        return connectTaskDnd.get();
    }

    /**
     * Sets the do-not-disturb status.
     *
     * @param dnd 1 if the task should not be disturbed, 0 otherwise
     */
    public void setConnectTaskDnd(long dnd) {
        connectTaskDnd.set(dnd);
    }

    public void reset() {
        connectTaskDnd.set(0);
    }
}
