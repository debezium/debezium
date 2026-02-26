/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import io.debezium.pipeline.metrics.traits.CommonEventMetricsMXBean;
import io.debezium.pipeline.metrics.traits.QueueMetricsMXBean;
import io.debezium.pipeline.metrics.traits.SchemaMetricsMXBean;

/**
 * Metrics that are common for both snapshot and streaming change event sources
 *
 * @author Jiri Pechanec
 */
public interface ChangeEventSourceMetricsMXBean extends CommonEventMetricsMXBean, QueueMetricsMXBean,
        SchemaMetricsMXBean {

    void reset();
}
