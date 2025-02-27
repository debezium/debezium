/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data.vector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Vectors {
    private static final String SPARSE_VECTOR_ERROR = "Cannot convert sparse vector {}, expected format is {i1:x,i2:y,i3:z,...}/dimensions";
    private static final Logger LOGGER = LoggerFactory.getLogger(Vectors.class);

    static <T> List<T> fromVectorString(Schema schema, String value, Function<String, T> elementMapper) {
        Objects.requireNonNull(value, "value may not be null");

        value = value.trim();
        if (!value.startsWith("[") || !value.endsWith("]")) {
            LOGGER.warn("Cannot convert vector {}, expected format is [x,y,z,...]", value);
            return null;
        }

        value = value.substring(1, value.length() - 1);
        final var strValues = value.split(",");
        final List<T> result = new ArrayList<>(strValues.length);
        for (String element : strValues) {
            result.add(elementMapper.apply(element.trim()));
        }
        return result;
    }

    static <T> Struct fromSparseVectorString(Schema schema, String value, Function<String, T> elementMapper) {
        Objects.requireNonNull(value, "value may not be null");

        value = value.trim();
        var parts = value.split("/");
        if (parts.length != 2) {
            LOGGER.warn(SPARSE_VECTOR_ERROR, value);
            return null;
        }

        var strVector = parts[0].trim();
        final var dimensions = Short.parseShort(parts[1].trim());

        if (!strVector.startsWith("{") || !strVector.endsWith("}")) {
            LOGGER.warn(SPARSE_VECTOR_ERROR, value);
            return null;
        }

        strVector = strVector.substring(1, strVector.length() - 1);
        final var strValues = strVector.split(",");
        final Map<Short, T> vector = new HashMap<>(strValues.length);

        for (String element : strValues) {
            parts = element.split(":");
            if (parts.length != 2) {
                LOGGER.warn(SPARSE_VECTOR_ERROR, value);
                return null;
            }
            vector.put(Short.parseShort(parts[0].trim()), elementMapper.apply(parts[1].trim()));
        }

        final var result = new Struct(schema);
        result.put(SparseDoubleVector.DIMENSIONS_FIELD, dimensions);
        result.put(SparseDoubleVector.VECTOR_FIELD, vector);
        return result;
    }
}
