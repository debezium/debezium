/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.format;

/**
 * Describes a change event output format comprising just of a single value.
 */
public interface ChangeEventFormat<V extends SerializationFormat<?>> {

    /**
     * Creates a change event format representing key and value in a single object.
     */
    static <V extends SerializationFormat<?>> ChangeEventFormat<V> of(Class<V> format) {
        return () -> format;
    }

    Class<V> getValueFormat();
}
