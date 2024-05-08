/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.metrics;

import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetricsMXBean;

/**
 * Extended metrics exposed by the MongoDB connector during snapshot.
 *
 * @author Chris Cranford
 */
public interface MongoDbSnapshotChangeEventSourceMetricsMBean extends SnapshotChangeEventSourceMetricsMXBean {
    long getNumberOfDisconnects();
}
