/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.format;

import io.debezium.common.annotation.Incubating;

/**
 * Describes a change event output format comprising a key, value, and a header.
 */
@Incubating
public interface KeyValueHeaderChangeEventFormat<K extends SerializationFormat<?>, V extends SerializationFormat<?>, H extends SerializationFormat<?>>
        extends KeyValueChangeEventFormat<K, V> {
    static <K extends SerializationFormat<?>, V extends SerializationFormat<?>, H extends SerializationFormat<?>> KeyValueHeaderChangeEventFormat<K, V, H> of(
                                                                                                                                                              Class<K> keyFormat,
                                                                                                                                                              Class<V> valueFormat,
                                                                                                                                                              Class<H> headerFormat) {
        return new KeyValueHeaderChangeEventFormat<>() {

            @Override
            public Class<K> getKeyFormat() {
                return keyFormat;
            }

            @Override
            public Class<V> getValueFormat() {
                return valueFormat;
            }

            @Override
            public Class<H> getHeaderFormat() {
                return headerFormat;
            }
        };
    }

    Class<H> getHeaderFormat();
}
