/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data.vector;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.debezium.schema.SchemaFactory;

/**
 * A semantic type for a PgVector sparsevec type.
 *
 * @author Mincong Huang
 */
public class SparseVector {

    public static final String LOGICAL_NAME = "io.debezium.data.SparseVector";
    public static final String DIMENSIONS_FIELD = "dimensions";
    public static final String VECTOR_FIELD = "vector";
    public static int SCHEMA_VERSION = 1;

    /**
     * Returns a {@link SchemaBuilder} for a {@code sparsevec} field. You can use the resulting SchemaBuilder
     * to set additional schema settings such as required/optional, default value, and documentation.
     *
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return SchemaFactory.get().dataTypeSparseVectorSchema();
    }

    /**
     * Returns a {@link SchemaBuilder} for a {@code sparsevec} field, with all other default Schema settings.
     *
     * @return the schema
     * @see #builder()
     */
    public static Schema schema() {
        return builder().build();
    }

    /**
     * Converts a value from its logical format - {@link String} of {@code {i1: v1, i2: v2, ...}/dimensions}
     * to its encoded format - a {@link Struct} with a number of dimensions and a map of index to value
     *
     * @param schema of the encoded value
     * @param value the value of the vector
     *
     * @return the encoded value
     */
    public static Struct fromLogical(Schema schema, String value) {
        return Vectors.fromSparseVectorString(schema, value, Double::parseDouble);
    }
}
