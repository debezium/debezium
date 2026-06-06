/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data.vector;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.schema.SchemaFactory;

/**
 * A semantic type for a vector type with 8-byte elements.
 *
 * @author Jiri Pechanec
 */
public class DoubleVector {

    public static final String LOGICAL_NAME = "io.debezium.data.DoubleVector";
    public static int SCHEMA_VERSION = 1;

    /**
     * Returns a {@link SchemaBuilder} for a 8-byte vector field. You can use the resulting SchemaBuilder
     * to set additional schema settings such as required/optional, default value, and documentation.
     *
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return SchemaFactory.get().datatypeDoubleVectorSchema();
    }

    /**
     * Returns a {@link SchemaBuilder} for a 8-byte ector field, with all other default Schema settings.
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
    public static List<Double> fromLogical(Schema schema, String value) {
        return Vectors.fromVectorString(schema, value, Double::parseDouble);
    }
}
