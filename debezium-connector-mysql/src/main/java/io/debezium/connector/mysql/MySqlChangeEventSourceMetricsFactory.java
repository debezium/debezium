/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * @author Jiri Pechanec
 */
public class MySqlChangeEventSourceMetricsFactory extends DefaultChangeEventSourceMetricsFactory<MySqlPartition> {

    final MySqlStreamingChangeEventSourceMetrics streamingMetrics;

    public MySqlChangeEventSourceMetricsFactory(MySqlStreamingChangeEventSourceMetrics streamingMetrics) {
        this.streamingMetrics = streamingMetrics;
    }

    @Override
    public <T extends CdcSourceTaskContext> SnapshotChangeEventSourceMetrics<MySqlPartition> getSnapshotMetrics(T taskContext,
                                                                                                                ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                                EventMetadataProvider eventMetadataProvider) {
        return new MySqlSnapshotChangeEventSourceMetrics((MySqlTaskContext) taskContext, changeEventQueueMetrics, eventMetadataProvider);
    }

    @Override
    public <T extends CdcSourceTaskContext> StreamingChangeEventSourceMetrics<MySqlPartition> getStreamingMetrics(T taskContext,
                                                                                                                  ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                                  EventMetadataProvider eventMetadataProvider,
                                                                                                                  CapturedTablesSupplier capturedTablesSupplier) {
        return streamingMetrics;
    }

    @Override
    public boolean connectionMetricHandledByCoordinator() {
        return false;
    }
}
