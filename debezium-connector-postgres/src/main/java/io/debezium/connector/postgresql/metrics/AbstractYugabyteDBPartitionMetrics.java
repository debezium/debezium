package io.debezium.connector.postgresql.metrics;

import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.postgresql.PostgresTaskContext;
import io.debezium.data.Envelope;
import io.debezium.metrics.Metrics;
import io.debezium.pipeline.ConnectorEvent;
import io.debezium.pipeline.meters.CommonEventMeter;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;
import org.apache.kafka.connect.data.Struct;

import java.util.Map;

abstract class AbstractYugabyteDBPartitionMetrics extends YugabyteDBMetrics implements YugabyteDBPartitionMetricsMXBean {
    private final CommonEventMeter commonEventMeter;

    AbstractYugabyteDBPartitionMetrics(CdcSourceTaskContext taskContext, Map<String, String> tags,
                                       EventMetadataProvider metadataProvider) {
        super(taskContext, tags);
        this.commonEventMeter = new CommonEventMeter(taskContext.getClock(), metadataProvider);
    }

    @Override
    public String getLastEvent() {
        return commonEventMeter.getLastEvent();
    }

    @Override
    public long getMilliSecondsSinceLastEvent() {
        return commonEventMeter.getMilliSecondsSinceLastEvent();
    }

    @Override
    public long getTotalNumberOfEventsSeen() {
        return commonEventMeter.getTotalNumberOfEventsSeen();
    }

    @Override
    public long getTotalNumberOfCreateEventsSeen() {
        return commonEventMeter.getTotalNumberOfCreateEventsSeen();
    }

    @Override
    public long getTotalNumberOfUpdateEventsSeen() {
        return commonEventMeter.getTotalNumberOfUpdateEventsSeen();
    }

    @Override
    public long getTotalNumberOfDeleteEventsSeen() {
        return commonEventMeter.getTotalNumberOfDeleteEventsSeen();
    }

    @Override
    public long getNumberOfEventsFiltered() {
        return commonEventMeter.getNumberOfEventsFiltered();
    }

    @Override
    public long getNumberOfErroneousEvents() {
        return commonEventMeter.getNumberOfErroneousEvents();
    }

    /**
     * Invoked if an event is processed for a captured table.
     */
    void onEvent(DataCollectionId source, OffsetContext offset, Object key, Struct value, Envelope.Operation operation) {
        commonEventMeter.onEvent(source, offset, key, value, operation);
    }

    /**
     * Invoked for events pertaining to non-captured tables.
     */
    void onFilteredEvent(String event) {
        commonEventMeter.onFilteredEvent();
    }

    /**
     * Invoked for events pertaining to non-captured tables.
     */
    void onFilteredEvent(String event, Envelope.Operation operation) {
        commonEventMeter.onFilteredEvent(operation);
    }

    /**
     * Invoked for events that cannot be processed.
     */
    void onErroneousEvent(String event) {
        commonEventMeter.onErroneousEvent();
    }

    /**
     * Invoked for events that cannot be processed.
     */
    void onErroneousEvent(String event, Envelope.Operation operation) {
        commonEventMeter.onErroneousEvent(operation);
    }

    /**
     * Invoked for events that represent a connector event.
     */
    void onConnectorEvent(ConnectorEvent event) {
    }

    @Override
    public void reset() {
        commonEventMeter.reset();
    }
}
