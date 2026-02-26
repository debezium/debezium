/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics.traits;

/**
 * Exposes common event metrics.
 */
public interface CommonEventMetricsMXBean {

    String getLastEvent();

    long getMilliSecondsSinceLastEvent();

    long getTotalNumberOfEventsSeen();

    long getTotalNumberOfCreateEventsSeen();

    long getTotalNumberOfUpdateEventsSeen();

    long getTotalNumberOfDeleteEventsSeen();

    long getNumberOfEventsFiltered();

    long getNumberOfErroneousEvents();
}
