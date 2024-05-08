/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.metrics;

import io.debezium.pipeline.metrics.traits.SnapshotMetricsMXBean;

public interface SqlServerSnapshotPartitionMetricsMXBean extends SnapshotMetricsMXBean,
        SqlServerPartitionMetricsMXBean {
}
