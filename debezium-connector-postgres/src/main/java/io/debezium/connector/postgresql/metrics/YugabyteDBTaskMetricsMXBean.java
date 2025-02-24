package io.debezium.connector.postgresql.metrics;

import io.debezium.pipeline.metrics.traits.QueueMetricsMXBean;

public interface YugabyteDBTaskMetricsMXBean extends QueueMetricsMXBean {
    void reset();
}
