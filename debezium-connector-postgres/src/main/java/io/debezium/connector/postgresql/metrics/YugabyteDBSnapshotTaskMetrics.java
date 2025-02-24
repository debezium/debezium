package io.debezium.connector.postgresql.metrics;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.PostgresTaskContext;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;

import java.util.Collection;

class YugabyteDBSnapshotTaskMetrics extends AbstractYugabyteDBTaskMetrics<YugabyteDBSnapshotPartitionMetrics>
    implements SnapshotChangeEventSourceMetrics<PostgresPartition> {

    YugabyteDBSnapshotTaskMetrics(CdcSourceTaskContext taskContext,
                                  ChangeEventQueueMetrics changeEventQueueMetrics,
                                  EventMetadataProvider metadataProvider,
                                  Collection<PostgresPartition> partitions) {
        super(taskContext, "snapshot", changeEventQueueMetrics, partitions,
                (PostgresPartition partition) -> new YugabyteDBSnapshotPartitionMetrics(taskContext,
                        Collect.linkMapOf(
                                "server", taskContext.getConnectorName(),
                                "task", taskContext.getTaskId(),
                                "context", "snapshot",
                                "partition", partition.getPartitionIdentificationKey()),
                        metadataProvider));
    }

    @Override
    public void snapshotStarted(PostgresPartition partition) {
        onPartitionEvent(partition, YugabyteDBSnapshotPartitionMetrics::snapshotStarted);
    }

    @Override
    public void snapshotPaused(PostgresPartition partition) {
        onPartitionEvent(partition, YugabyteDBSnapshotPartitionMetrics::snapshotPaused);
    }

    @Override
    public void snapshotResumed(PostgresPartition partition) {
        onPartitionEvent(partition, YugabyteDBSnapshotPartitionMetrics::snapshotResumed);
    }

    @Override
    public void monitoredDataCollectionsDetermined(PostgresPartition partition, Iterable<? extends DataCollectionId> dataCollectionIds) {
        onPartitionEvent(partition, bean -> bean.monitoredDataCollectionsDetermined(dataCollectionIds));
    }

    @Override
    public void snapshotCompleted(PostgresPartition partition) {
        onPartitionEvent(partition, YugabyteDBSnapshotPartitionMetrics::snapshotCompleted);
    }

    @Override
    public void snapshotAborted(PostgresPartition partition) {
        onPartitionEvent(partition, YugabyteDBSnapshotPartitionMetrics::snapshotAborted);
    }

    @Override
    public void dataCollectionSnapshotCompleted(PostgresPartition partition, DataCollectionId dataCollectionId, long numRows) {
        onPartitionEvent(partition, bean -> bean.dataCollectionSnapshotCompleted(dataCollectionId, numRows));
    }

    @Override
    public void rowsScanned(PostgresPartition partition, TableId tableId, long numRows) {
        onPartitionEvent(partition, bean -> bean.rowsScanned(tableId, numRows));
    }

    @Override
    public void currentChunk(PostgresPartition partition, String chunkId, Object[] chunkFrom, Object[] chunkTo) {
        onPartitionEvent(partition, bean -> bean.currentChunk(chunkId, chunkFrom, chunkTo));
    }

    @Override
    public void currentChunk(PostgresPartition partition, String chunkId, Object[] chunkFrom, Object[] chunkTo, Object[] tableTo) {
        onPartitionEvent(partition, bean -> bean.currentChunk(chunkId, chunkFrom, chunkTo, tableTo));
    }
}
