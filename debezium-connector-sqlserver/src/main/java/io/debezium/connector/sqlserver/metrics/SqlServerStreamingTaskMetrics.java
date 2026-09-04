/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.metrics;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.sqlserver.SqlServerPartition;
import io.debezium.pipeline.meters.ConnectionMeter;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;

class SqlServerStreamingTaskMetrics extends AbstractSqlServerTaskMetrics<SqlServerStreamingPartitionMetrics>
        implements StreamingChangeEventSourceMetrics<SqlServerPartition>, SqlServerStreamingTaskMetricsMXBean {

    private final ConnectionMeter connectionMeter;

    SqlServerStreamingTaskMetrics(CdcSourceTaskContext taskContext,
                                  ChangeEventQueueMetrics changeEventQueueMetrics,
                                  EventMetadataProvider metadataProvider,
                                  Collection<SqlServerPartition> partitions,
                                  CapturedTablesSupplier capturedTablesSupplier) {
        super(taskContext, "streaming", changeEventQueueMetrics, partitions,
                (SqlServerPartition partition) -> new SqlServerStreamingPartitionMetrics(taskContext,
                        Collect.linkMapOf(
                                "server", taskContext.getConnectorLogicalName(),
                                "task", taskContext.getTaskId(),
                                "context", "streaming",
                                "database", partition.getDatabaseName()),
                        metadataProvider,
                        scopedTo(capturedTablesSupplier, partition)));
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

    @Override
    public void onUnchangedEventSkipped(SqlServerPartition partition) {
        onPartitionEvent(partition, SqlServerStreamingPartitionMetrics::onUnchangedEventSkipped);
    }

    static CapturedTablesSupplier scopedTo(CapturedTablesSupplier supplier, SqlServerPartition partition) {
        if (supplier == null) {
            return Collections::emptyList;
        }
        return () -> supplier.getCapturedTables().stream()
                .filter(id -> isTableInPartition(id, partition))
                .collect(Collectors.toList());
    }

    private static boolean isTableInPartition(DataCollectionId dataCollectionId, SqlServerPartition partition) {
        if (dataCollectionId instanceof TableId tableId) {
            return partition.getDatabaseName() != null && partition.getDatabaseName().equalsIgnoreCase(tableId.catalog());
        }
        return false;
    }
}
