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
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;

class SqlServerSnapshotTaskMetrics extends AbstractSqlServerTaskMetrics<SqlServerSnapshotPartitionMetrics>
        implements SnapshotChangeEventSourceMetrics<SqlServerPartition> {

    SqlServerSnapshotTaskMetrics(CdcSourceTaskContext taskContext,
                                 ChangeEventQueueMetrics changeEventQueueMetrics,
                                 EventMetadataProvider metadataProvider,
                                 Collection<SqlServerPartition> partitions) {
        super(taskContext, "snapshot", changeEventQueueMetrics, partitions,
                (SqlServerPartition partition) -> new SqlServerSnapshotPartitionMetrics(taskContext,
                        Collect.linkMapOf(
                                "server", taskContext.getConnectorLogicalName(),
                                "task", taskContext.getTaskId(),
                                "context", "snapshot",
                                "database", partition.getDatabaseName()),
                        metadataProvider));
    }

    @Override
    public void snapshotStarted(SqlServerPartition partition) {
        onPartitionEvent(partition, SqlServerSnapshotPartitionMetrics::snapshotStarted);
    }

    @Override
    public void snapshotPaused(SqlServerPartition partition) {
        onPartitionEvent(partition, SqlServerSnapshotPartitionMetrics::snapshotPaused);
    }

    @Override
    public void snapshotResumed(SqlServerPartition partition) {
        onPartitionEvent(partition, SqlServerSnapshotPartitionMetrics::snapshotResumed);
    }

    @Override
    public void monitoredDataCollectionsDetermined(SqlServerPartition partition, Iterable<? extends DataCollectionId> dataCollectionIds) {
        onPartitionEvent(partition, bean -> bean.monitoredDataCollectionsDetermined(dataCollectionIds));
    }

    @Override
    public void snapshotCompleted(SqlServerPartition partition) {
        onPartitionEvent(partition, SqlServerSnapshotPartitionMetrics::snapshotCompleted);
    }

    @Override
    public void snapshotAborted(SqlServerPartition partition) {
        onPartitionEvent(partition, SqlServerSnapshotPartitionMetrics::snapshotAborted);
    }

    @Override
    public void snapshotSkipped(SqlServerPartition partition) {
        onPartitionEvent(partition, SqlServerSnapshotPartitionMetrics::snapshotSkipped);
    }

    @Override
    public void dataCollectionSnapshotCompleted(SqlServerPartition partition, DataCollectionId dataCollectionId, long numRows) {
        onPartitionEvent(partition, bean -> bean.dataCollectionSnapshotCompleted(dataCollectionId, numRows));
    }

    @Override
    public void rowsScanned(SqlServerPartition partition, TableId tableId, long numRows) {
        onPartitionEvent(partition, bean -> bean.rowsScanned(tableId, numRows));
    }

    @Override
    public void currentChunk(SqlServerPartition partition, String chunkId, Object[] chunkFrom, Object[] chunkTo) {
        onPartitionEvent(partition, bean -> bean.currentChunk(chunkId, chunkFrom, chunkTo));
    }

    @Override
    public void currentChunk(SqlServerPartition partition, String chunkId, Object[] chunkFrom, Object[] chunkTo, Object[] tableTo) {
        onPartitionEvent(partition, bean -> bean.currentChunk(chunkId, chunkFrom, chunkTo, tableTo));
    }
}
