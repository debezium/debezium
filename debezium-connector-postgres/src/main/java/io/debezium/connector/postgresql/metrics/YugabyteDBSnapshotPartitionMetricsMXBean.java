package io.debezium.connector.postgresql.metrics;

import io.debezium.pipeline.metrics.traits.SnapshotMetricsMXBean;

public interface YugabyteDBSnapshotPartitionMetricsMXBean extends SnapshotMetricsMXBean,
        YugabyteDBPartitionMetricsMXBean {
}
