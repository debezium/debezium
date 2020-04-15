/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.format;

public class ChangeEvent<T> {

    private final T key;
    private final T value;
    private final Object backReference;

    public ChangeEvent(T key, T value, Object backReference) {
        this.key = key;
        this.value = value;
        this.backReference = backReference;
    }

    public T key() {
        return key;
    }

    public T value() {
        return value;
    }

    public Object reference() {
        return backReference;
    }

    @Override
    public String toString() {
        return "ChangeEvent [key=" + key + ", value=" + value + "]";
    }
}
