/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.metrics;

import io.debezium.pipeline.metrics.traits.QueueMetricsMXBean;

/**
 * Metrics scoped to a connector task that are common for both snapshot and streaming change event sources
 */
public interface SqlServerTaskMetricsMXBean extends QueueMetricsMXBean {

    void reset();
}
