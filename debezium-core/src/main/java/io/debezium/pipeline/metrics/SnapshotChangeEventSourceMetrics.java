/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import io.debezium.pipeline.source.spi.SnapshotProgressListener;

/**
 * Metrics related to the snapshot phase of a connector.
 */
public interface SnapshotChangeEventSourceMetrics extends ChangeEventSourceMetrics, SnapshotProgressListener {
}
