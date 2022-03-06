/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics.traits;

import java.util.Map;

/**
 * Exposes streaming metrics.
 */
public interface StreamingMetricsMXBean extends SchemaMetricsMXBean {

    Map<String, String> getSourceEventPosition();

    long getMilliSecondsBehindSource();

    long getNumberOfCommittedTransactions();

    String getLastTransactionId();
}
