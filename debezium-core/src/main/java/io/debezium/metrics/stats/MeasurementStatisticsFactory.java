/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metrics.stats;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.metrics.event.SingleValueEvent;

/**
 * // TODO: Document this
 *
 * @author vjuranek
 * @since 4.0
 */
public class MeasurementStatisticsFactory {

    public static <T extends SingleValueEvent<Long>> MeasurementStatistics<T, Long> createStatistics(CommonConnectorConfig config) {
        if (config.isStatisticsMetricsEnabled()) {
            return new LongDDSketchStatistics<T>();
        }

        return new NoopStatistics<T, Long>();
    }

    private static class NoopStatistics<T extends SingleValueEvent<V>, V> implements MeasurementStatistics<T, V> {
        @Override
        public Double getValueAtP99() {
            return null;
        }

        @Override
        public Double getValueAtP95() {
            return null;
        }

        @Override
        public Double getValueAtP50() {
            return null;
        }

        @Override
        public Double getValueAtQuantile(double quantile) {
            return null;
        }

        @Override
        public void reset() {
            // no-op
        }

        @Override
        public void accept(T t) {
            // no-op
        }
    }
}
