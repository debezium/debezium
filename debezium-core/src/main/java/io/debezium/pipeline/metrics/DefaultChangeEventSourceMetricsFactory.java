/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.Partition;

/**
 * @author Chris Cranford
 */
public class DefaultChangeEventSourceMetricsFactory<P extends Partition> implements ChangeEventSourceMetricsFactory<P> {

    /**
     * Returns the snapshot change event source metrics with shared task state metrics.
     */
    @Override
    public <T extends CdcSourceTaskContext> SnapshotChangeEventSourceMetrics<P> getSnapshotMetrics(T taskContext,
                                                                                                   ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                   EventMetadataProvider eventMetadataProvider,
                                                                                                   TaskStateMetrics taskStateMetrics) {
        return new DefaultSnapshotChangeEventSourceMetrics<>(taskContext, changeEventQueueMetrics,
                eventMetadataProvider, taskStateMetrics);
    }

    /**
     * Returns the streaming change event source metrics.
     */
    @Override
    public <T extends CdcSourceTaskContext> StreamingChangeEventSourceMetrics<P> getStreamingMetrics(T taskContext,
                                                                                                     ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                     EventMetadataProvider eventMetadataProvider) {
        return new DefaultStreamingChangeEventSourceMetrics<>(taskContext, changeEventQueueMetrics, eventMetadataProvider);
    }
}
