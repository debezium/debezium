/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data.vector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.schema.SchemaFactory;

/**
 * A semantic type for a vector type with 4-byte elemnents.
 *
 * @author Jiri Pechanec
 */
public class FloatVector {
    private static final Logger LOGGER = LoggerFactory.getLogger(FloatVector.class);

    public static final String LOGICAL_NAME = "io.debezium.data.FloatVector";
    public static int SCHEMA_VERSION = 1;

    /**
     * Returns a {@link SchemaBuilder} for a 4-byte vector field. You can use the resulting SchemaBuilder
     * to set additional schema settings such as required/optional, default value, and documentation.
     *
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return SchemaFactory.get().datatypeFloatVectorSchema();
    }

    /**
     * Returns a {@link SchemaBuilder} for a 4-byte vector field, with all other default Schema settings.
     *
     * @return the schema
     * @see #builder()
     */
    public static Schema schema() {
        return builder().build();
    }

    /**
     * Converts a value from its logical format - {@link String} of {@code [x,y,z,...]}
     * to its encoded format - a Connect array represented by list of numbers.
     *
     * @param schema of the encoded value
     * @param value the value of the vector
     *
     * @return the encoded value
     */
    public static List<Float> fromLogical(Schema schema, String value) {
        return Vectors.fromVectorString(schema, value, Float::parseFloat);
    }

    /**
     * Converts a value from its raw array of floats format
     * to its encoded format - a Connect array represented by list of numbers.
     *
     * @param schema of the encoded value
     * @param value the value of the vector
     *
     * @return the encoded value
     */
    public static List<Float> fromLogical(Schema schema, float[] value) {
        final List<Float> ret = new ArrayList<>(value.length);
        for (float v : value) {
            ret.add(v);
        }
        return ret;
    }

    /**
     * Converts a value from its octet stream of 4-byte values
     * to its encoded format - a Connect array represented by list of numbers.
     *
     * @param schema of the encoded value
     * @param value the value of the vector
     *
     * @return the encoded value
     */
    public static List<Float> fromLogical(Field fieldDfn, byte[] value) {
        if (value.length % Float.BYTES != 0) {
            LOGGER.warn("Cannot convert field '{}', the octet stream is not multiply of {}", fieldDfn.name(), Float.BYTES);
            return Collections.emptyList();
        }

        final List<Float> ret = new ArrayList<>(value.length);

        for (int i = 0; i < value.length;) {
            final var intValue = (value[i++] & 0xff)
                    | ((value[i++] & 0xff) << 8)
                    | ((value[i++] & 0xff) << 16)
                    | ((value[i++] & 0xff) << 24);
            ret.add(Float.intBitsToFloat(intValue));
        }
        return ret;
    }
}
