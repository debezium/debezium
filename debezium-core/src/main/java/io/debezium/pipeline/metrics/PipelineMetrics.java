/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.data.Envelope.Operation;
import io.debezium.metrics.Metrics;
import io.debezium.pipeline.ConnectorEvent;
import io.debezium.pipeline.meters.CommonEventMeter;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Base for metrics implementations.
 *
 * @author Randall Hauch, Jiri Pechanec
 */
@ThreadSafe
public abstract class PipelineMetrics<P extends Partition> extends Metrics
        implements DataChangeEventListener<P>, ChangeEventSourceMetricsMXBean {

    protected final EventMetadataProvider metadataProvider;

    private final ChangeEventQueueMetrics changeEventQueueMetrics;
    protected final CdcSourceTaskContext taskContext;
    private final CommonEventMeter commonEventMeter;

    protected <T extends CdcSourceTaskContext> PipelineMetrics(T taskContext, String contextName, ChangeEventQueueMetrics changeEventQueueMetrics,
                                                               EventMetadataProvider metadataProvider) {
        super(taskContext, contextName);
        this.taskContext = taskContext;
        this.changeEventQueueMetrics = changeEventQueueMetrics;
        this.metadataProvider = metadataProvider;
        this.commonEventMeter = new CommonEventMeter(taskContext.getClock(), metadataProvider);
    }

    protected <T extends CdcSourceTaskContext> PipelineMetrics(T taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                                               EventMetadataProvider metadataProvider, Map<String, String> tags) {
        super(taskContext, tags);
        this.taskContext = taskContext;
        this.changeEventQueueMetrics = changeEventQueueMetrics;
        this.metadataProvider = metadataProvider;
        this.commonEventMeter = new CommonEventMeter(taskContext.getClock(), metadataProvider);
    }

    @Override
    public void onEvent(P partition, DataCollectionId source, OffsetContext offset, Object key, Struct value,
                        Operation operation) {
        commonEventMeter.onEvent(source, offset, key, value, operation);
    }

    @Override
    public void onFilteredEvent(P partition, String event) {
        commonEventMeter.onFilteredEvent();
    }

    @Override
    public void onFilteredEvent(P partition, String event, Operation operation) {
        commonEventMeter.onFilteredEvent(operation);
    }

    @Override
    public void onErroneousEvent(P partition, String event) {
        commonEventMeter.onErroneousEvent();
    }

    @Override
    public void onErroneousEvent(P partition, String event, Operation operation) {
        commonEventMeter.onErroneousEvent(operation);
    }

    @Override
    public void onConnectorEvent(P partition, ConnectorEvent event) {
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

    @Override
    public void reset() {
        commonEventMeter.reset();
    }

    @Override
    public int getQueueTotalCapacity() {
        return changeEventQueueMetrics.totalCapacity();
    }

    @Override
    public int getQueueRemainingCapacity() {
        return changeEventQueueMetrics.remainingCapacity();
    }

    @Override
    public long getMaxQueueSizeInBytes() {
        return changeEventQueueMetrics.maxQueueSizeInBytes();
    }

    @Override
    public long getCurrentQueueSizeInBytes() {
        return changeEventQueueMetrics.currentQueueSizeInBytes();
    }

}
