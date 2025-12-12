/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metrics.stats;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.DDSketches;

import io.debezium.annotation.ThreadSafe;
import io.debezium.metrics.event.SingleValueEvent;

/**
 * // TODO: Document this
 * @author vjuranek
 */
@ThreadSafe
public class LongDDSketchStatistics<T extends SingleValueEvent<Long>> implements MeasurementStatistics<T, Long> {

    // TODO make quantile accuracy configurable
    private final DDSketch sketch = DDSketches.unboundedDense(0.01);

    @Override
    public synchronized void accept(T event) {
        sketch.accept((double) event.getValue());
    }

    @Override
    public synchronized void reset() {
        sketch.clear();
    }

    @Override
    public synchronized Double getValueAtQuantile(double quantile) {
        return sketch.isEmpty() ? null : sketch.getValueAtQuantile(quantile);
    }
}
