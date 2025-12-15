/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metrics.stats;

import java.util.function.Consumer;

import io.debezium.metrics.event.SingleValueEvent;

/**
 * Statistics of the {@link SingleValueMeasurement}s over some period of time.
 * Currently, only quantiles are supported.
 *
 * @author vjuranek
 */
public interface MeasurementStatistics<T extends SingleValueEvent<V>, V> extends Consumer<T> {

    void reset();

    Double getValueAtQuantile(double quantile);

    default Double getValueAtP50() {
        return getValueAtQuantile(0.50);
    }

    default Double getValueAtP95() {
        return getValueAtQuantile(0.95);
    }

    default Double getValueAtP99() {
        return getValueAtQuantile(0.99);
    }
}