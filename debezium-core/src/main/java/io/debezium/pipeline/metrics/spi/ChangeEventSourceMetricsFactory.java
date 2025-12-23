/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics.spi;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.Partition;

/**
 * A factory for creating {@link SnapshotChangeEventSourceMetrics} and {@link StreamingChangeEventSourceMetrics}.
 *
 * @author Chris Cranford
 */
public interface ChangeEventSourceMetricsFactory<P extends Partition> {

    /**
     * Returns the snapshot change event source metrics.
     *
     * @param taskContext
     *          The task context
     * @param changeEventQueueMetrics
     *          The change event queue metrics
     * @param eventMetadataProvider
     *          The event metadata provider implementation
     *
     * @return a snapshot change event source metrics
     */
    <T extends CdcSourceTaskContext> SnapshotChangeEventSourceMetrics<P> getSnapshotMetrics(T taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                            EventMetadataProvider eventMetadataProvider);

    /**
     * Returns the streaming change event source metrics.
     *
     * @param taskContext
     *          The task context
     * @param changeEventQueueMetrics
     *          The change event queue metrics
     * @param eventMetadataProvider
     *          The event metadata provider implementation
     *
     * @return a streaming change event source metrics
     */
    <T extends CdcSourceTaskContext> StreamingChangeEventSourceMetrics<P> getStreamingMetrics(T taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                              EventMetadataProvider eventMetadataProvider);

    default boolean connectionMetricHandledByCoordinator() {
        return true;
    }
}
