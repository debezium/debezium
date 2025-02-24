package io.debezium.connector.postgresql.metrics;

import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.postgresql.PostgresTaskContext;
import io.debezium.data.Envelope;
import io.debezium.pipeline.meters.StreamingMeter;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;
import org.apache.kafka.connect.data.Struct;

import java.util.Map;

class YugabyteDBStreamingPartitionMetrics extends AbstractYugabyteDBPartitionMetrics
    implements YugabyteDBStreamingPartitionMetricsMXBean {

    private final StreamingMeter streamingMeter;

    YugabyteDBStreamingPartitionMetrics(CdcSourceTaskContext taskContext,
                                        Map<String, String> tags,
                                        EventMetadataProvider metadataProvider) {
        super(taskContext, tags, metadataProvider);
        streamingMeter = new StreamingMeter(taskContext, metadataProvider);
    }

    @Override
    void onEvent(DataCollectionId source, OffsetContext offset, Object key, Struct value, Envelope.Operation operation) {
        super.onEvent(source, offset, key, value, operation);
        streamingMeter.onEvent(source, offset, key, value);
    }

    @Override
    public String[] getCapturedTables() {
        return streamingMeter.getCapturedTables();
    }

    @Override
    public long getMilliSecondsBehindSource() {
        return streamingMeter.getMilliSecondsBehindSource();
    }

    @Override
    public long getNumberOfCommittedTransactions() {
        return streamingMeter.getNumberOfCommittedTransactions();
    }

    @Override
    public Map<String, String> getSourceEventPosition() {
        return streamingMeter.getSourceEventPosition();
    }

    @Override
    public String getLastTransactionId() {
        return streamingMeter.getLastTransactionId();
    }

    @Override
    public void reset() {
        super.reset();
        streamingMeter.reset();
    }
}
