/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metrics.stats;

import io.debezium.metrics.event.LagBehindSourceEvent;

/**
 * // TODO: Document this
 * @author vjuranek
 * @since 4.0
 */
public class LagBehindSourceMeasurement extends SingleValueLongMeasurementWithStats<LagBehindSourceEvent> {
    public LagBehindSourceMeasurement(LongDDSketchStatistics<LagBehindSourceEvent> statistics) {
        super(statistics);
    }
}
