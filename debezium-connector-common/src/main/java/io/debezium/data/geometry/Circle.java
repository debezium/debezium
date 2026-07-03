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
 * A semantic type for a PostgreSQL {@code circle}, defined by a center {@link Point} and a radius.
 * <p>
 * Unlike {@link Point}, this type does not extend {@link Geometry}: a circle has no faithful OGC
 * Well-Known Binary representation (WKB has no curve primitive), so it carries its true center and
 * radius losslessly instead. The lossy, target-specific approximation (for sinks without a native
 * circle type) is left to the sink, which can dispatch on this logical name.
 *
 * @author Debezium Authors
 */
public class Circle {

    public static final String LOGICAL_NAME = "io.debezium.data.geometry.Circle";
    public static final String CENTER_FIELD = "center";
    public static final String RADIUS_FIELD = "radius";

    /**
     * Returns a {@link SchemaBuilder} for a Circle field. You can use the resulting SchemaBuilder
     * to set additional schema settings such as required/optional, default value, and documentation.
     *
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.struct()
                .name(LOGICAL_NAME)
                .version(1)
                .doc("Geometry (CIRCLE)")
                .optional()
                .field(CENTER_FIELD, Point.builder().build())
                .field(RADIUS_FIELD, Schema.FLOAT64_SCHEMA);
    }

    /**
     * Returns a {@link Schema} for a Circle field, with all other default Schema settings.
     *
     * @return the schema
     * @see #builder()
     */
    public static Schema schema() {
        return builder().build();
    }

    /**
     * Creates a value for this schema using the circle's center coordinates and radius.
     *
     * @param circleSchema a {@link Schema} instance which represents a circle; may not be null
     * @param centerX the X coordinate of the center point
     * @param centerY the Y coordinate of the center point
     * @param radius the radius of the circle
     * @return a {@link Struct} which represents a Connect value for this schema; never null
     */
    public static Struct createValue(Schema circleSchema, double centerX, double centerY, double radius) {
        Struct result = new Struct(circleSchema);
        Schema centerSchema = circleSchema.field(CENTER_FIELD).schema();
        result.put(CENTER_FIELD, Point.createValue(centerSchema, centerX, centerY));
        result.put(RADIUS_FIELD, radius);
        return result;
    }
}
