/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metrics.stats;

import io.debezium.metrics.event.SingleValueEvent;

/**
 * {@link SingleValueMeasurement} of {@link Long} type, which provides support of more advanced statistics like quantiles.
 *
 * @author vjuranek
 */
public abstract class SingleValueLongMeasurementWithStats<T extends SingleValueEvent<Long>> extends SingleValueLongMeasurement<T>
        implements MeasurementStatistics<T, Long> {

    private final MeasurementStatistics<T, Long> statistics;

    public SingleValueLongMeasurementWithStats(MeasurementStatistics<T, Long> statistics) {
        this.statistics = statistics;
    }

    @Override
    public Double getValueAtQuantile(double quantile) {
        return statistics.getValueAtQuantile(quantile);
    }

    @Override
    public void accept(T event) {
        super.accept(event);
        statistics.accept(event);
    }
}
