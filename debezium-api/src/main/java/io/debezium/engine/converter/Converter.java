/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.converter;

import java.util.Map;

import io.debezium.common.annotation.Incubating;

/**
 * Converter for translating record keys and values between Debezium data format and byte[].
 * <p>
 * Implementations handle serialization of record keys and values for the Debezium engine.
 *
 * @author Debezium Authors
 */
@Incubating
public interface Converter {

    /**
     * Configure this converter.
     *
     * @param configs the configuration
     * @param isKey whether this converter is used for keys or values
     */
    void configure(Map<String, ?> configs, boolean isKey);

    /**
     * Convert a Debezium data value and its schema into a byte array.
     *
     * @param topic the name of the topic for the data
     * @param headers the headers for the record; may be null
     * @param schema the schema for the value; may be null
     * @param value the value to convert; may be null
     * @return the byte array form of the value; may be null if the value is null
     */
    byte[] fromDebeziumData(String topic, Object headers, Object schema, Object value);
}
