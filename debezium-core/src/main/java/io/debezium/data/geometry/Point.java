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
 * A semantic type for a geometric Point, defined as a set of (x,y) coordinates.
 * 
 * @author Horia Chiorean
 * @author Omar Al-Safi
 */
public class Point {

    public static final String LOGICAL_NAME = "io.debezium.data.geometry.Point";
    public static final String X_FIELD = "x";
    public static final String Y_FIELD = "y";
    public static final String WKB_FIELD = "wkb"; //Please see DBZ-208

    /**
     * Returns a {@link SchemaBuilder} for a Uuid field. You can use the resulting SchemaBuilder
     * to set additional schema settings such as required/optional, default value, and documentation.
     * 
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.struct()
                            .name(LOGICAL_NAME)
                            .version(1)
                            .doc("A geometric point")
                            .field(X_FIELD, Schema.FLOAT64_SCHEMA)
                            .field(Y_FIELD, Schema.FLOAT64_SCHEMA)
                            .field(WKB_FIELD, Schema.OPTIONAL_BYTES_SCHEMA);
    }

    /**
     * Returns a {@link SchemaBuilder} for a Point, with all other default Schema settings.
     * 
     * @return the schema
     * @see #builder()
     */
    public static Schema schema() {
        return builder().build();
    }

    /**
     * Creates a value for this schema using 2 given coordinates.
     * 
     * @param pointSchema a {@link Schema} instance which represents a point; may not be null
     * @param x the X coordinate of the point; may not be null
     * @param y the Y coordinate of the point; may not be null
     * @return a {@link Struct} which represents a Connect value for this schema; never null
     */
    public static Struct createValue(Schema pointSchema, double x, double y) {
        Struct result = new Struct(pointSchema);
        return result.put(X_FIELD, x).put(Y_FIELD, y);
    }

    /**
     * Create a value for this schema using 2 given coordinates and WKB as the original representation of the coordinate (Ref: DBZ-208)
     * @param pointSchema a {@link Schema} instance which represents a point; may not be null
     * @param x the X coordinate of the point; may not be null
     * @param y the Y coordinate of the point; may not be null
     * @param wkb the original Well-Known binary representation of the coordinate
     * @return a {@link Struct} which represents a Connect value for this schema; never null
     */
    public static Struct createValue(Schema pointSchema, double x, double y, byte[] wkb){
        Struct results = createValue(pointSchema, x, y);
        return results.put(WKB_FIELD, wkb);
    }
}
