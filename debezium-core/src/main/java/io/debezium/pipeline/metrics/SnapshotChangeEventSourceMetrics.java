/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import java.util.Collection;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.TaskPartition;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;

/**
 * Metrics related to the initial snapshot of a connector.
 *
 * @author Randall Hauch, Jiri Pechanec
 */
@ThreadSafe
public class SnapshotChangeEventSourceMetrics<P extends TaskPartition>
        extends PipelineMetrics<P, SnapshotChangeEventSourcePartitionMetrics>
        implements ChangeEventSourceTaskMetricsMXBean, SnapshotProgressListener<P> {

    public <T extends CdcSourceTaskContext> SnapshotChangeEventSourceMetrics(T taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                             EventMetadataProvider metadataProvider,
                                                                             Collection<P> partitions) {
        super(taskContext, "snapshot", changeEventQueueMetrics, partitions,
                (P partition) -> new SnapshotChangeEventSourcePartitionMetrics(taskContext, "snapshot",
                        partition, metadataProvider));
    }

    @Override
    public void monitoredDataCollectionsDetermined(P partition, Iterable<? extends DataCollectionId> dataCollectionIds) {
        onPartitionEvent(partition, bean -> bean.monitoredDataCollectionsDetermined(dataCollectionIds));
    }

    @Override
    public void dataCollectionSnapshotCompleted(P partition, DataCollectionId dataCollectionId, long numRows) {
        onPartitionEvent(partition, bean -> bean.dataCollectionSnapshotCompleted(dataCollectionId, numRows));
    }

    @Override
    public void snapshotStarted(P partition) {
        onPartitionEvent(partition, SnapshotChangeEventSourcePartitionMetrics::snapshotStarted);
    }

    @Override
    public void snapshotCompleted(P partition) {
        onPartitionEvent(partition, SnapshotChangeEventSourcePartitionMetrics::snapshotCompleted);
    }

    @Override
    public void snapshotAborted(P partition) {
        onPartitionEvent(partition, SnapshotChangeEventSourcePartitionMetrics::snapshotAborted);
    }

    @Override
    public void rowsScanned(P partition, TableId tableId, long numRows) {
        onPartitionEvent(partition, bean -> bean.rowsScanned(tableId, numRows));
    }
}
