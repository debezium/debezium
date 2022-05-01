/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.meters;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.connect.data.Struct;

import io.debezium.annotation.ThreadSafe;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.metrics.traits.CommonEventMetricsMXBean;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;

/**
 * Carries common event metrics.
 */
@ThreadSafe
public class CommonEventMeter implements CommonEventMetricsMXBean {

    protected final AtomicLong totalNumberOfEventsSeen = new AtomicLong();
    protected final AtomicLong totalNumberOfCreateEventsSeen = new AtomicLong();
    protected final AtomicLong totalNumberOfUpdateEventsSeen = new AtomicLong();
    protected final AtomicLong totalNumberOfDeleteEventsSeen = new AtomicLong();
    private final AtomicLong numberOfEventsFiltered = new AtomicLong();
    protected final AtomicLong numberOfErroneousEvents = new AtomicLong();
    protected final AtomicLong lastEventTimestamp = new AtomicLong(-1);
    private volatile String lastEvent;

    private final Clock clock;
    private final EventMetadataProvider metadataProvider;

    public CommonEventMeter(Clock clock, EventMetadataProvider metadataProvider) {
        this.clock = clock;
        this.metadataProvider = metadataProvider;
    }

    public void onEvent(DataCollectionId source, OffsetContext offset, Object key, Struct value, Operation operation) {
        updateCommonEventMetrics(operation);
        lastEvent = metadataProvider.toSummaryString(source, offset, key, value);
    }

    private void updateCommonEventMetrics() {
        updateCommonEventMetrics(null);
    }

    private void updateCommonEventMetrics(Operation operation) {
        totalNumberOfEventsSeen.incrementAndGet();
        lastEventTimestamp.set(clock.currentTimeInMillis());

        if (operation != null) {
            switch (operation) {
                case CREATE:
                    totalNumberOfCreateEventsSeen.incrementAndGet();
                    break;
                case UPDATE:
                    totalNumberOfUpdateEventsSeen.incrementAndGet();
                    break;
                case DELETE:
                    totalNumberOfDeleteEventsSeen.incrementAndGet();
                    break;
                default:
                    break;
            }
        }
    }

    public void onFilteredEvent() {
        numberOfEventsFiltered.incrementAndGet();
        updateCommonEventMetrics();
    }

    public void onFilteredEvent(Operation operation) {
        numberOfEventsFiltered.incrementAndGet();
        updateCommonEventMetrics(operation);
    }

    public void onErroneousEvent() {
        numberOfErroneousEvents.incrementAndGet();
        updateCommonEventMetrics();
    }

    public void onErroneousEvent(Operation operation) {
        numberOfErroneousEvents.incrementAndGet();
        updateCommonEventMetrics(operation);
    }

    @Override
    public String getLastEvent() {
        return lastEvent;
    }

    @Override
    public long getMilliSecondsSinceLastEvent() {
        return (lastEventTimestamp.get() == -1) ? -1 : (clock.currentTimeInMillis() - lastEventTimestamp.get());
    }

    @Override
    public long getTotalNumberOfEventsSeen() {
        return totalNumberOfEventsSeen.get();
    }

    @Override
    public long getTotalNumberOfCreateEventsSeen() {
        return totalNumberOfCreateEventsSeen.get();
    }

    @Override
    public long getTotalNumberOfUpdateEventsSeen() {
        return totalNumberOfUpdateEventsSeen.get();
    }

    @Override
    public long getTotalNumberOfDeleteEventsSeen() {
        return totalNumberOfDeleteEventsSeen.get();
    }

    @Override
    public long getNumberOfEventsFiltered() {
        return numberOfEventsFiltered.get();
    }

    @Override
    public long getNumberOfErroneousEvents() {
        return numberOfErroneousEvents.get();
    }

    public void reset() {
        totalNumberOfEventsSeen.set(0);
        totalNumberOfCreateEventsSeen.set(0);
        totalNumberOfUpdateEventsSeen.set(0);
        totalNumberOfDeleteEventsSeen.set(0);
        lastEventTimestamp.set(-1);
        numberOfEventsFiltered.set(0);
        numberOfErroneousEvents.set(0);
        lastEvent = null;
    }
}
