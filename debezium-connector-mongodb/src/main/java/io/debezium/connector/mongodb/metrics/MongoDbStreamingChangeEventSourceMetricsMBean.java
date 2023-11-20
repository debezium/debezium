/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.metrics;

import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean;

/**
 * Extended metrics exposed by the MongoDB connector during streaming.
 *
 * @author Chris Cranford
 */
public interface MongoDbStreamingChangeEventSourceMetricsMBean extends StreamingChangeEventSourceMetricsMXBean {

    long getNumberOfDisconnects();

    long getNumberOfPrimaryElections();

    long getLastSourceEventPollTime();

    long getLastEmptyPollTime();

    long getNumberOfSourceEvents();

    long getNumberOfEmptyPolls();
}
