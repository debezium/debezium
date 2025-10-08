/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.metrics;

import java.util.Collection;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.sqlserver.SqlServerPartition;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

public class SqlServerMetricsFactory implements ChangeEventSourceMetricsFactory<SqlServerPartition> {

    private final Collection<SqlServerPartition> partitions;

    public SqlServerMetricsFactory(Collection<SqlServerPartition> partitions) {
        this.partitions = partitions;
    }

    @Override
    public <T extends CdcSourceTaskContext> SnapshotChangeEventSourceMetrics<SqlServerPartition> getSnapshotMetrics(T taskContext,
                                                                                                                    ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                                    EventMetadataProvider eventMetadataProvider) {
        return new SqlServerSnapshotTaskMetrics(taskContext, changeEventQueueMetrics,
                eventMetadataProvider, partitions);
    }

    @Override
    public <T extends CdcSourceTaskContext> StreamingChangeEventSourceMetrics<SqlServerPartition> getStreamingMetrics(T taskContext,
                                                                                                                      ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                                      EventMetadataProvider eventMetadataProvider,
                                                                                                                      CapturedTablesSupplier capturedTablesSupplier) {
        return new SqlServerStreamingTaskMetrics(taskContext, changeEventQueueMetrics,
                eventMetadataProvider, partitions, capturedTablesSupplier);
    }
}
