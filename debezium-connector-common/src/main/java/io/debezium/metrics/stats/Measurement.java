/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metrics.stats;

import java.util.function.Consumer;

import io.debezium.metrics.event.MeasurementEvent;

/**
 * // TODO: Document this
 * @author vjuranek
 * @since 4.0
 */
public interface Measurement<T extends MeasurementEvent> extends Consumer<T> {
    void accept(T event);
}
