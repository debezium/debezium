/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metrics.stats;

import io.debezium.metrics.event.SingleValueEvent;

/**
 * // TODO: Document this
 * @author vjuranek
 * @since 4.0
 */
public interface SingleValueMeasurement<T extends SingleValueEvent<V>, V> extends Measurement<T> {

    void reset();

    Long getLastValue();

    Long getMinValue();

    Long getMaxValue();

    Long getAverageValue();
}
