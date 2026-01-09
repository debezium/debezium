/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metrics.stats;

import io.debezium.metrics.event.LagBehindSourceEvent;

/**
 * Measurement of the lag between the time when a record was inserted into the source database and when it was processed by Debezium.
 *
 * @author vjuranek
 */
public class LagBehindSourceMeasurement extends SingleValueLongMeasurementWithStats<LagBehindSourceEvent> {
    public LagBehindSourceMeasurement(MeasurementStatistics<LagBehindSourceEvent, Long> statistics) {
        super(statistics);
    }
}
