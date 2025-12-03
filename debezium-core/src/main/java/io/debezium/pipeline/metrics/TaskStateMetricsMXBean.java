/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.pipeline.metrics;

/**
 * Exposes task-level state metrics that are shared across different connector phases
 * (snapshot, streaming, schema recovery, etc.).
 */
public interface TaskStateMetricsMXBean {

    /**
     * Gets the current rebalance exemption status.
     *
     * @return 1 if the task is exempt from rebalancing, 0 otherwise
     */
    long getConnectTaskRebalanceExempt();
}
