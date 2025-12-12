/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metrics.stats;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.metrics.event.SingleValueEvent;

/**
 * // TODO: Document this
 * @author vjuranek
 * @since 4.0
 */
@NotThreadSafe
public abstract class SingleValueLongMeasurement<T extends SingleValueEvent<Long>> implements SingleValueMeasurement<T, Long> {

    private long minValue = Long.MAX_VALUE;
    private long maxValue = 0L;
    private long averageValue = 0L;
    private long lastValue = 0L;
    private long count = 0L;

    @Override
    public void accept(T event) {
        final long value = event.getValue();
        lastValue = value;
        count++;

        if (minValue > value) {
            minValue = value;
        }

        if (maxValue < value) {
            maxValue = value;
        }

        averageValue = (averageValue * (count - 1) + value) / count;
    }

    @Override
    public synchronized void reset() {
        minValue = Long.MAX_VALUE;
        maxValue = 0L;
        averageValue = 0L;
        lastValue = 0L;
        count = 0L;
    }

    @Override
    public Long getLastValue() {
        return count == 0L ? null : lastValue;
    }

    @Override
    public Long getMinValue() {
        return count == 0L ? null : minValue;
    }

    @Override
    public Long getMaxValue() {
        return count == 0L ? null : maxValue;
    }

    @Override
    public Long getAverageValue() {
        return count == 0L ? null : averageValue;
    }
}
