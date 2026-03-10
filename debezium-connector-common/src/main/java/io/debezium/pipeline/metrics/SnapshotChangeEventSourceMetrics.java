/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.Partition;

/**
 * Metrics related to the snapshot phase of a connector.
 */
public interface SnapshotChangeEventSourceMetrics<P extends Partition> extends ChangeEventSourceMetrics<P>,
        SnapshotProgressListener<P> {
}
