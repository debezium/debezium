/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import io.debezium.pipeline.metrics.traits.ConnectionMetricsMXBean;
import io.debezium.pipeline.metrics.traits.StreamingMetricsMXBean;

/**
 * Metrics specific to streaming change event sources
 *
 * @author Randall Hauch, Jiri Pechanec
 */
public interface StreamingChangeEventSourceMetricsMXBean extends ChangeEventSourceMetricsMXBean,
        ConnectionMetricsMXBean, StreamingMetricsMXBean {
}
