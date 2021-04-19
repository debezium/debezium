/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

/**
 * Metrics specific to streaming change event sources scoped to a connector task
 */
public interface StreamingChangeEventSourceTaskMetricsMXBean extends ChangeEventSourceTaskMetricsMXBean {

    boolean isConnected();
}
