/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.data.vector;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.connector.postgresql.PostgresSchemaFactory;

/**
 * A semantic type for a PgVector halfvec type.
 *
 * @author Jiri Pechanec
 */
public class HalfVector {

    public static final String LOGICAL_NAME = "io.debezium.data.HalfVector";
    public static int SCHEMA_VERSION = 1;

    /**
     * Returns a {@link SchemaBuilder} for a halfvec field. You can use the resulting SchemaBuilder
     * to set additional schema settings such as required/optional, default value, and documentation.
     *
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return PostgresSchemaFactory.get().datatypeHalfVectorSchema();
    }

    /**
     * Returns a {@link SchemaBuilder} for a halfvec field, with all other default Schema settings.
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
}
