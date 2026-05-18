/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.converter;

import java.io.Closeable;
import java.util.Map;

import io.debezium.common.annotation.Incubating;

/**
 * Converter for translating header values between runtime data format and byte[].
 * <p>
 * Implementations handle serialization of record headers for the embedded engine.
 *
 * @author Debezium Authors
 */
@Incubating
public interface HeaderConverter extends Closeable {

    /**
     * Configure this header converter.
     *
     * @param configs the configuration
     */
    void configure(Map<String, ?> configs);

    /**
     * Convert a header value and its schema into a byte array.
     *
     * @param topic the name of the topic for the record containing the header
     * @param headerKey the header's key; may not be null
     * @param schema the schema for the header's value; may be null
     * @param value the header's value to convert; may be null
     * @return the byte array form of the header's value; may be null if the value is null
     */
    byte[] fromHeader(String topic, String headerKey, Object schema, Object value);
}
