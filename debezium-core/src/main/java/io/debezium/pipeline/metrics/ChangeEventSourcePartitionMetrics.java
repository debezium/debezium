/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.TaskPartition;
import io.debezium.metrics.Metrics;
import io.debezium.pipeline.ConnectorEvent;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Clock;

/**
 * Metrics scoped to a source partition that are common for both snapshot and streaming change event sources
 *
 * @author Jiri Pechanec
 */
abstract class ChangeEventSourcePartitionMetrics extends Metrics implements ChangeEventSourcePartitionMetricsMXBean {

    protected final EventMetadataProvider metadataProvider;
    protected final AtomicLong totalNumberOfEventsSeen = new AtomicLong();
    private final AtomicLong numberOfEventsFiltered = new AtomicLong();
    protected final AtomicLong numberOfErroneousEvents = new AtomicLong();
    protected final AtomicLong lastEventTimestamp = new AtomicLong(-1);
    private volatile String lastEvent;

    protected final Clock clock;
    protected final CdcSourceTaskContext taskContext;

    protected <T extends CdcSourceTaskContext> ChangeEventSourcePartitionMetrics(T taskContext, String contextName,
                                                                                 TaskPartition partition,
                                                                                 EventMetadataProvider metadataProvider) {
        super(taskContext, contextName, partition);

        this.taskContext = taskContext;
        this.clock = taskContext.getClock();
        this.metadataProvider = metadataProvider;
    }

    public void onEvent(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        updateCommonEventMetrics();
        lastEvent = metadataProvider.toSummaryString(source, offset, key, value);
    }

    private void updateCommonEventMetrics() {
        totalNumberOfEventsSeen.incrementAndGet();
        lastEventTimestamp.set(clock.currentTimeInMillis());
    }

    public void onFilteredEvent(String event) {
        numberOfEventsFiltered.incrementAndGet();
        updateCommonEventMetrics();
    }

    public void onErroneousEvent(String event) {
        numberOfErroneousEvents.incrementAndGet();
        updateCommonEventMetrics();
    }

    public void onConnectorEvent(ConnectorEvent event) {
    }

    public String getLastEvent() {
        return lastEvent;
    }

    public long getMilliSecondsSinceLastEvent() {
        return (lastEventTimestamp.get() == -1) ? -1 : (clock.currentTimeInMillis() - lastEventTimestamp.get());
    }

    public long getTotalNumberOfEventsSeen() {
        return totalNumberOfEventsSeen.get();
    }

    public long getNumberOfEventsFiltered() {
        return numberOfEventsFiltered.get();
    }

    public long getNumberOfErroneousEvents() {
        return numberOfErroneousEvents.get();
    }
}
