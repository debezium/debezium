/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.format;

import io.debezium.common.annotation.Incubating;

/**
 * Describes a change event output format comprising a key and a value.
 */
@Incubating
public interface KeyValueChangeEventFormat<K extends SerializationFormat<?>, V extends SerializationFormat<?>> {

    /**
     * Creates a change event format representing key and value using separate objects.
     */
    static <K extends SerializationFormat<?>, V extends SerializationFormat<?>> KeyValueChangeEventFormat<K, V> of(Class<K> keyFormat, Class<V> valueFormat) {
        return new KeyValueChangeEventFormat<K, V>() {

            @Override
            public Class<K> getKeyFormat() {
                return keyFormat;
            }

            @Override
            public Class<V> getValueFormat() {
                return valueFormat;
            }
        };
    }

    Class<V> getValueFormat();

    Class<K> getKeyFormat();
}
