/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

/**
 * Metrics that are common for both snapshot and streaming change event sources
 * and source partition and task scopes.
 *
 * @author Jiri Pechanec
 */
public interface ChangeEventSourceMetricsMXBean {

    void reset();
}
