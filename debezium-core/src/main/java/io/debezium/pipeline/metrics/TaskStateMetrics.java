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

    private final AtomicLong connectTaskRebalanceExempt = new AtomicLong();

    public TaskStateMetrics(CdcSourceTaskContext taskContext) {
        super(taskContext, "task");
    }

    @Override
    public long getConnectTaskRebalanceExempt() {
        return connectTaskRebalanceExempt.get();
    }

    /**
     * Sets the rebalance exemption status.
     *
     * @param exempt 1 if the task should be exempt from rebalancing, 0 otherwise
     */
    public void setConnectTaskRebalanceExempt(long exempt) {
        connectTaskRebalanceExempt.set(exempt);
    }

    public void reset() {
        connectTaskRebalanceExempt.set(0);
    }
}
