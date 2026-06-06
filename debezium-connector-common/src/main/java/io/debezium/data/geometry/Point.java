/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.data.geometry;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * A semantic type for a geometric Point, defined as a set of (x,y) coordinates.
 *
 * This historically used to be a useful class, but is now mostly obsolete due
 * to the generic {@link Geometry} type. TODO for 1.0: Decide whether to remove
 * it or keep it.
 *
 * @author Horia Chiorean
 * @author Omar Al-Safi
 */
public class Point extends Geometry {

    public static final String LOGICAL_NAME = "io.debezium.data.geometry.Point";
    public static final String X_FIELD = "x";
    public static final String Y_FIELD = "y";

    private static final int WKB_POINT = 1; // type constant
    private static final int WKB_POINT_SIZE = (1 + 4 + 8 + 8); // fixed size

    /**
     * Returns a {@link SchemaBuilder} for a Point field.
     * A Geometry with extra X & Y fields
     *
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.struct()
                .name(LOGICAL_NAME)
                .version(1)
                .doc("Geometry (POINT)")
                .field(X_FIELD, Schema.FLOAT64_SCHEMA)
                .field(Y_FIELD, Schema.FLOAT64_SCHEMA)
                .field(WKB_FIELD, Schema.OPTIONAL_BYTES_SCHEMA)
                .field(SRID_FIELD, Schema.OPTIONAL_INT32_SCHEMA);
    }

    /**
     * Creates WKB for a 2D {x,y} point.
     * @param x coordinate
     * @param y coordinate
     * @return OGC WKB byte array
     */
    private static byte[] buildWKBPoint(double x, double y) {
        ByteBuffer wkb = ByteBuffer.allocate(WKB_POINT_SIZE);
        wkb.put((byte) 1); // BOM
        wkb.order(ByteOrder.LITTLE_ENDIAN);

        wkb.putInt(WKB_POINT);
        wkb.putDouble(x);
        wkb.putDouble(y);
        return wkb.array();
    }

    /**
     * Parses a 2D WKB Point into a {x,y} coordinate array.
     * Returns null for any non-point or points with Z/M/etc modifiers.
     * @param wkb OGC WKB geometry
     * @return x,y coordinate array
     */
    public static double[] parseWKBPoint(byte[] wkb) throws IllegalArgumentException {
        if (wkb.length != WKB_POINT_SIZE) {
            throw new IllegalArgumentException(String.format("Invalid WKB for Point (length %d < %d)", wkb.length, WKB_POINT_SIZE));
        }

        final ByteBuffer reader = ByteBuffer.wrap(wkb);

        // Read the BOM
        reader.order((reader.get() != 0) ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);

        int geomType = reader.getInt();
        if (geomType != WKB_POINT) {
            // we only parse 2D points
            throw new IllegalArgumentException(String.format("Invalid WKB for 2D Point (wrong type %d)", geomType));
        }

        double x = reader.getDouble();
        double y = reader.getDouble();
        return new double[]{ x, y };
    }

    /**
     * Creates a value for this schema using 2 given coordinates.
     *
     * @param geomSchema a {@link Schema} instance which represents a point; may not be null
     * @param x the X coordinate of the point; may not be null
     * @param y the Y coordinate of the point; may not be null
     * @return a {@link Struct} which represents a Connect value for this schema; never null
     */
    public static Struct createValue(Schema geomSchema, double x, double y) {
        // turn the specified points
        byte[] wkb = buildWKBPoint(x, y);
        Struct result = Geometry.createValue(geomSchema, wkb, null);
        result.put(X_FIELD, x);
        result.put(Y_FIELD, y);
        return result;
    }

    /**
     * Create a value for this schema using WKB
     * @param geomSchema a {@link Schema} instance which represents a point; may not be null
     * @param wkb the original Well-Known binary representation of the coordinate; may not be null
     * @param srid the coordinate reference system identifier; null if unset/unknown
     * @return a {@link Struct} which represents a Connect value for this schema; never null
     */
    public static Struct createValue(Schema geomSchema, byte[] wkb, Integer srid) throws IllegalArgumentException {
        Struct result = Geometry.createValue(geomSchema, wkb, srid);
        double[] pt = parseWKBPoint(wkb);
        result.put(X_FIELD, pt[0]);
        result.put(Y_FIELD, pt[1]);
        return result;
    }
}
