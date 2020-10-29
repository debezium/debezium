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

    long getNumberOfErroneousEvents();

    String[] getMonitoredTables();

    int getQueueTotalCapacity();

    int getQueueRemainingCapacity();

    long getMaxQueueSizeInBytes();

    long getCurrentQueueSizeInBytes();

    void reset();
}
