package io.debezium.connector.postgresql.metrics;

import io.debezium.pipeline.metrics.traits.CommonEventMetricsMXBean;
import io.debezium.pipeline.metrics.traits.SchemaMetricsMXBean;

public interface YugabyteDBPartitionMetricsMXBean extends CommonEventMetricsMXBean, SchemaMetricsMXBean  {
    void reset();
}
