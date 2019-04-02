/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

/**
 * Metrics that are common for both snapshot and streaming change event sources
 *
 * @author Jiri Pechanec
 */
public interface ChangeEventSourceMetricsMXBean {

    String getLastEvent();
    long getMilliSecondsSinceLastEvent();
    long getTotalNumberOfEventsSeen();
    long getNumberOfEventsFiltered();
    String[] getMonitoredTables();
    int getQueueTotalCapacity();
    int getQueueRemainingCapacity();
    void reset();

    /**
     * @deprecated Renamed to getNumberOfEventsFiltered(). To be removed in next major release version.
     * See DBZ-1206 and DBZ-1209 for more details.
     */
    @Deprecated
    long getNumberOfEventsSkipped();
}
