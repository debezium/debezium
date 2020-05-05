/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.format;

import io.debezium.common.annotation.Incubating;
import io.debezium.engine.SerializationFormat;

/**
 * Describes a change event output format comprising just of a single value.
 */
@Incubating
public interface ChangeEventFormat<V extends SerializationFormat<?>> {

    static <V extends SerializationFormat<?>> ChangeEventFormat<V> formatOf(Class<V> format) {
        return () -> format;
    }

    static <K extends SerializationFormat<?>, V extends SerializationFormat<?>> KeyValueChangeEventFormat<K, V> keyValueFormatOf(Class<K> keyFormat,
                                                                                                                                 Class<V> valueFormat) {
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
}
