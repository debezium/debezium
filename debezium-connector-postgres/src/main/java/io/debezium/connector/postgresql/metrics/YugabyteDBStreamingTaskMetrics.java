package io.debezium.connector.postgresql.metrics;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.PostgresTaskContext;
import io.debezium.pipeline.meters.ConnectionMeter;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.util.Collect;

import java.util.Collection;

public class YugabyteDBStreamingTaskMetrics extends AbstractYugabyteDBTaskMetrics<YugabyteDBStreamingPartitionMetrics>
    implements StreamingChangeEventSourceMetrics<PostgresPartition>, YugabyteDBStreamingTaskMetricsMXBean {

    private final ConnectionMeter connectionMeter;

    YugabyteDBStreamingTaskMetrics(CdcSourceTaskContext taskContext,
                                   ChangeEventQueueMetrics changeEventQueueMetrics,
                                   EventMetadataProvider metadataProvider,
                                   Collection<PostgresPartition> partitions) {
        super(taskContext, "streaming", changeEventQueueMetrics, partitions,
                (PostgresPartition partition) -> new YugabyteDBStreamingPartitionMetrics(taskContext,
                        Collect.linkMapOf(
                                "server", taskContext.getConnectorName(),
                                "task", taskContext.getTaskId(),
                                "context", "streaming",
                                "partition", partition.getPartitionIdentificationKey()),
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
