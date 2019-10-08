/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import java.util.Map;

/**
 * Metrics specific to streaming change event sources
 *
 * @author Randall Hauch, Jiri Pechanec
 */
public interface StreamingChangeEventSourceMetricsMXBean extends ChangeEventSourceMetricsMXBean {

    boolean isConnected();

    long getMilliSecondsBehindSource();

    long getNumberOfCommittedTransactions();

    Map<String, String> getSourceEventPosition();

    String getLastTransactionId();
}
