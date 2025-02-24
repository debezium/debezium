package io.debezium.connector.postgresql.metrics;

import io.debezium.pipeline.metrics.traits.StreamingMetricsMXBean;

public interface YugabyteDBStreamingPartitionMetricsMXBean extends StreamingMetricsMXBean,
    YugabyteDBPartitionMetricsMXBean {
}
