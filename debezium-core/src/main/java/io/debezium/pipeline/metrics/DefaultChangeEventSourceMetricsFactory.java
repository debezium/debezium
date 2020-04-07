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

/**
 * @author Chris Cranford
 */
public class DefaultChangeEventSourceMetricsFactory implements ChangeEventSourceMetricsFactory {
    @Override
    public <T extends CdcSourceTaskContext> SnapshotChangeEventSourceMetrics getSnapshotMetrics(T taskContext,
                                                                                                ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                EventMetadataProvider eventMetadataProvider) {
        return new SnapshotChangeEventSourceMetrics(taskContext, changeEventQueueMetrics, eventMetadataProvider);
    }

    @Override
    public <T extends CdcSourceTaskContext> StreamingChangeEventSourceMetrics getStreamingMetrics(T taskContext,
                                                                                                  ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                  EventMetadataProvider eventMetadataProvider) {
        return new StreamingChangeEventSourceMetrics(taskContext, changeEventQueueMetrics, eventMetadataProvider);
    }
}
