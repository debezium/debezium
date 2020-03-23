/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.format;

public class Change<T> {
    public final T key;
    public final T value;
    private final Object backReference;

    public Change(T key, T value, Object backReference) {
        this.key = key;
        this.value = value;
        this.backReference = backReference;
    }

    public Object reference() {
        return backReference;
    }

    @Override
    public String toString() {
        return "Change [key=" + key + ", value=" + value + "]";
    }
}
