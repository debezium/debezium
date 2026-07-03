/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.data.geometry;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * A semantic type for a PostgreSQL {@code line}, an infinite line described by the linear equation
 * {@code Ax + By + C = 0} and stored as the coefficients {@code {a, b, c}}.
 * <p>
 * This type does not extend {@link Geometry}: an infinite line has no OGC Well-Known Binary
 * representation (WKB models only finite geometries), so it carries the three coefficients
 * losslessly instead and lets the sink dispatch on this logical name.
 *
 * @author Debezium Authors
 */
public class Line {

    public static final String LOGICAL_NAME = "io.debezium.data.geometry.Line";
    public static final String A_FIELD = "a";
    public static final String B_FIELD = "b";
    public static final String C_FIELD = "c";

    /**
     * Returns a {@link SchemaBuilder} for a Line field. You can use the resulting SchemaBuilder
     * to set additional schema settings such as required/optional, default value, and documentation.
     *
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.struct()
                .name(LOGICAL_NAME)
                .version(1)
                .doc("Geometry (LINE)")
                .optional()
                .field(A_FIELD, Schema.FLOAT64_SCHEMA)
                .field(B_FIELD, Schema.FLOAT64_SCHEMA)
                .field(C_FIELD, Schema.FLOAT64_SCHEMA);
    }

    /**
     * Returns a {@link Schema} for a Line field, with all other default Schema settings.
     *
     * @return the schema
     * @see #builder()
     */
    public static Schema schema() {
        return builder().build();
    }

    /**
     * Creates a value for this schema using the line's coefficients.
     *
     * @param lineSchema a {@link Schema} instance which represents a line; may not be null
     * @param a the {@code A} coefficient
     * @param b the {@code B} coefficient
     * @param c the {@code C} coefficient
     * @return a {@link Struct} which represents a Connect value for this schema; never null
     */
    public static Struct createValue(Schema lineSchema, double a, double b, double c) {
        Struct result = new Struct(lineSchema);
        result.put(A_FIELD, a);
        result.put(B_FIELD, b);
        result.put(C_FIELD, c);
        return result;
    }
}
