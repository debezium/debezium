/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data.vector;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Vectors {
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
}
