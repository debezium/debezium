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
import io.debezium.pipeline.meters.ConnectionMeter;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.util.Collect;

class SqlServerStreamingTaskMetrics extends AbstractSqlServerTaskMetrics<SqlServerStreamingPartitionMetrics>
        implements StreamingChangeEventSourceMetrics<SqlServerPartition>, SqlServerStreamingTaskMetricsMXBean {

    private final ConnectionMeter connectionMeter;

    SqlServerStreamingTaskMetrics(CdcSourceTaskContext taskContext,
                                  ChangeEventQueueMetrics changeEventQueueMetrics,
                                  EventMetadataProvider metadataProvider,
                                  Collection<SqlServerPartition> partitions) {
        super(taskContext, "streaming", changeEventQueueMetrics, partitions,
                (SqlServerPartition partition) -> new SqlServerStreamingPartitionMetrics(taskContext,
                        Collect.linkMapOf(
                                "server", taskContext.getConnectorName(),
                                "task", taskContext.getTaskId(),
                                "context", "streaming",
                                "database", partition.getDatabaseName()),
                        metadataProvider));
        connectionMeter = new ConnectionMeter();
    }

    @Override
    public boolean isConnected() {
        return connectionMeter.isConnected();
    }

    @Override
    public void connected(boolean connected) {
        connectionMeter.connected(connected);
    }
}
