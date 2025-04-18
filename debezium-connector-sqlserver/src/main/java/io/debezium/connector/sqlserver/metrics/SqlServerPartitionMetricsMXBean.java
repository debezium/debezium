/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.metrics;

import io.debezium.metrics.activity.ActivityMonitoringMXBean;
import io.debezium.pipeline.metrics.traits.CommonEventMetricsMXBean;
import io.debezium.pipeline.metrics.traits.SchemaMetricsMXBean;

/**
 * Metrics scoped to a source partition that are common for both snapshot and streaming change event sources
 */
public interface SqlServerPartitionMetricsMXBean extends CommonEventMetricsMXBean, SchemaMetricsMXBean, ActivityMonitoringMXBean {

    void reset();
}
