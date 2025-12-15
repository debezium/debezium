/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metrics.event;

/**
 * Carries over one single measurement of {@link io.debezium.metrics.stats.LagBehindSourceMeasurement}.
 *
 * @author vjuranek
 */
public class LagBehindSourceEvent extends SingleValueEvent<Long> {

    public LagBehindSourceEvent(long value) {
        super(value);
    }
}
