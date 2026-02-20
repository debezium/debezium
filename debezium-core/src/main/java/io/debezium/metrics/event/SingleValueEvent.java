/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metrics.event;

/**
 * Measurement which tracks only a single variable.
 *
 * @author vjuranek
 */
public class SingleValueEvent<T> implements MeasurementEvent {
    private final T value;

    public SingleValueEvent(T value) {
        this.value = value;
    }

    public T getValue() {
        return value;
    }
}
