/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import java.util.Collection;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.TaskPartition;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * @author Chris Cranford
 */
public class DefaultChangeEventSourceMetricsFactory<P extends TaskPartition> implements ChangeEventSourceMetricsFactory<P> {
    @Override
    public <T extends CdcSourceTaskContext> SnapshotChangeEventSourceMetrics<P> getSnapshotMetrics(T taskContext,
                                                                                                   ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                   EventMetadataProvider eventMetadataProvider,
                                                                                                   Collection<P> partitions) {
        return new SnapshotChangeEventSourceMetrics<>(taskContext, changeEventQueueMetrics, eventMetadataProvider, partitions);
    }

    @Override
    public <T extends CdcSourceTaskContext> StreamingChangeEventSourceMetrics<P> getStreamingMetrics(T taskContext,
                                                                                                     ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                     EventMetadataProvider eventMetadataProvider,
                                                                                                     Collection<P> partitions) {
        return new StreamingChangeEventSourceMetrics<>(taskContext, changeEventQueueMetrics, eventMetadataProvider, partitions);
    }
}
