package io.debezium.connector.postgresql.metrics;

import io.debezium.pipeline.metrics.traits.ConnectionMetricsMXBean;

public interface YugabyteDBStreamingTaskMetricsMXBean extends ConnectionMetricsMXBean, YugabyteDBTaskMetricsMXBean {
}
