/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Utility helper that serializes a Kafka Connect {@link Struct} to a JSON string, so the value can
 * be stored in the dialect's JSON or string column type. Nested structs, collections, and maps are
 * converted recursively; scalar values keep Jackson's default representation (binary data as
 * Base64, decimals as numbers, dates as epoch milliseconds).
 */
public class StructToJsonConverter {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private StructToJsonConverter() {
    }

    /**
     * Serializes the given struct to a JSON string.
     *
     * @param struct the struct to serialize; may not be null
     * @return the JSON representation of the struct
     */
    public static String structToJsonString(Struct struct) {
        try {
            return MAPPER.writeValueAsString(toJsonValue(struct));
        }
        catch (JsonProcessingException e) {
            throw new ConnectException("Failed to serialize STRUCT data to JSON", e);
        }
    }

    private static Object toJsonValue(Object value) {
        if (value instanceof Struct struct) {
            final Map<String, Object> result = new LinkedHashMap<>();
            for (Field field : struct.schema().fields()) {
                result.put(field.name(), toJsonValue(struct.get(field)));
            }
            return result;
        }
        if (value instanceof List<?> list) {
            final List<Object> result = new ArrayList<>(list.size());
            for (Object element : list) {
                result.add(toJsonValue(element));
            }
            return result;
        }
        if (value instanceof Map<?, ?> map) {
            final Map<Object, Object> result = new LinkedHashMap<>();
            map.forEach((key, mapValue) -> result.put(key, toJsonValue(mapValue)));
            return result;
        }
        if (value instanceof ByteBuffer buffer) {
            // Unwrap to byte[], which Jackson serializes as Base64 like any other binary value.
            final ByteBuffer slice = buffer.slice();
            final byte[] bytes = new byte[slice.remaining()];
            slice.get(bytes);
            return bytes;
        }
        return value;
    }
}
