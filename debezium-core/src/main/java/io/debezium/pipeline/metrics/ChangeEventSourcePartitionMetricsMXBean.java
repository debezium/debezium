/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

/**
 * Metrics scoped to a source partition that are common for both snapshot and streaming change event sources
 *
 * @author Jiri Pechanec
 */
public interface ChangeEventSourcePartitionMetricsMXBean extends ChangeEventSourceMetricsMXBean {

    String getLastEvent();

    long getMilliSecondsSinceLastEvent();

    long getTotalNumberOfEventsSeen();

    long getNumberOfEventsFiltered();

    long getNumberOfErroneousEvents();

    String[] getMonitoredTables();
}
