package io.debezium.connector.postgresql.metrics;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

import java.util.Collection;

public class YugabyteDBMetricsFactory implements ChangeEventSourceMetricsFactory<PostgresPartition> {

    private final Collection<PostgresPartition> partitions;

    public YugabyteDBMetricsFactory(Collection<PostgresPartition> partitions) {
        this.partitions = partitions;
    }

    @Override
    public <T extends CdcSourceTaskContext> SnapshotChangeEventSourceMetrics<PostgresPartition> getSnapshotMetrics(T taskContext,
                                                                                                                   ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                                   EventMetadataProvider eventMetadataProvider) {
        return new YugabyteDBSnapshotTaskMetrics(taskContext, changeEventQueueMetrics, eventMetadataProvider, partitions);
    }

    @Override
    public <T extends CdcSourceTaskContext> StreamingChangeEventSourceMetrics<PostgresPartition> getStreamingMetrics(T taskContext,
                                                                                                                     ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                                     EventMetadataProvider eventMetadataProvider) {
        return new YugabyteDBStreamingTaskMetrics(taskContext, changeEventQueueMetrics, eventMetadataProvider, partitions);
    }
}
