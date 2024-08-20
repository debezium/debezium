/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.metrics;

import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.data.Envelope.Operation;
import io.debezium.metrics.Metrics;
import io.debezium.metrics.activity.ActivityMonitoringMeter;
import io.debezium.pipeline.ConnectorEvent;
import io.debezium.pipeline.meters.CommonEventMeter;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Base implementation of partition-scoped multi-partition SQL Server connector metrics.
 */
abstract public class AbstractSqlServerPartitionMetrics extends Metrics implements SqlServerPartitionMetricsMXBean {

    private final CommonEventMeter commonEventMeter;
    private final ActivityMonitoringMeter activityMonitoringMeter;
    protected final CdcSourceTaskContext taskContext;

    AbstractSqlServerPartitionMetrics(CdcSourceTaskContext taskContext, Map<String, String> tags,
                                      EventMetadataProvider metadataProvider) {
        super(taskContext, tags);
        this.taskContext = taskContext;
        this.commonEventMeter = new CommonEventMeter(taskContext.getClock(), metadataProvider);
        this.activityMonitoringMeter = new ActivityMonitoringMeter();
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
    public Map<String, Long> getNumberOfCreateEventsSeen() {
        return activityMonitoringMeter.getNumberOfCreateEventsSeen();
    }

    @Override
    public Map<String, Long> getNumberOfDeleteEventsSeen() {
        return activityMonitoringMeter.getNumberOfDeleteEventsSeen();
    }

    @Override
    public Map<String, Long> getNumberOfUpdateEventsSeen() {
        return activityMonitoringMeter.getNumberOfUpdateEventsSeen();
    }

    @Override
    public Map<String, Long> getNumberOfTruncateEventsSeen() {
        return activityMonitoringMeter.getNumberOfTruncateEventsSeen();
    }

    @Override
    public void pause() {
        activityMonitoringMeter.pause();
    }

    @Override
    public void resume() {
        activityMonitoringMeter.resume();
    }

    /**
     * Invoked if an event is processed for a captured table.
     */
    void onEvent(DataCollectionId source, OffsetContext offset, Object key, Struct value, Operation operation) {
        commonEventMeter.onEvent(source, offset, key, value, operation);
        if (taskContext.getConfig().isAdvancedMetricsEnabled()) {
            activityMonitoringMeter.onEvent(source, offset, key, value, operation);
        }
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
    void onFilteredEvent(String event, Operation operation) {
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
    void onErroneousEvent(String event, Operation operation) {
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
        activityMonitoringMeter.reset();
    }
}
