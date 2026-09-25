/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.metrics;

import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean;

/**
 * Extended metrics exposed by the PostgreSQL connector during streaming.
 */
public interface PostgresStreamingChangeEventSourceMetricsMXBean extends StreamingChangeEventSourceMetricsMXBean {

    long getTotalNumberOfLogicalMessageEventsSeen();
}
