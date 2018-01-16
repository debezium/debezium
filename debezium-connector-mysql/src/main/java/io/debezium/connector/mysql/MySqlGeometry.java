/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A parser API for MySQL Geometry types
 *
 * @author Omar Al-Safi
 * @author Robert Coup
 */
public class MySqlGeometry {
    protected static final Logger LOGGER = LoggerFactory.getLogger(MySqlGeometry.class);

    /**
     * Open Geospatial Consortium Well-Known-Binary representation of the Geometry.
     * http://www.opengeospatial.org/standards/sfa
     */
    private final byte[] wkb;
    /**
     * Coordinate reference system identifier. While it's technically user-defined,
     * the standard/common values in use are the EPSG code list http://www.epsg.org/
     */
    private final int srid;
    /**
     * {X,Y} coordinate pair if the geometry is a 2D point
     */
    private final double[] coords;

    // WKB constants from http://www.opengeospatial.org/standards/sfa
    private static final int WKB_POINT = 1;  // type constant
    private static final int WKB_POINT_SIZE = (1 + 4 + 8 + 8);  // fixed size
    // WKB for a GEOMETRYCOLLECTION EMPTY object
    private static final byte[] WKB_EMPTY_GEOMETRYCOLLECTION = {0x01, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};  // 0x010700000000000000

    /**
     * Create a MySqlGeometry using the supplied wkb, note this should be the cleaned wkb for MySQL
     *
     * @param wkb the Well-Known binary representation of the coordinate in the standard format
     */
    private MySqlGeometry(byte[] wkb, int srid) {
        this.wkb = wkb;
        this.srid = srid;

        // this will be either a {X,Y} coordinate pair if the geometry is a 2D point, null otherwise
        this.coords = parseWKBPoint(wkb);
    }

    /**
     * Create a MySqlGeometry from the original byte array from MySQL binlog event
     *
     * @param mysqlBytes he original byte array from MySQL binlog event
     *
     * @return a {@link MySqlGeometry} which represents a MySqlGeometry API
     */
    public static MySqlGeometry fromBytes(final byte[] mysqlBytes) {
        ByteBuffer buf = ByteBuffer.wrap(mysqlBytes);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        // first 4 bytes are SRID
        int srid = buf.getInt();
        // remainder is WKB
        byte[] wkb = new byte[buf.remaining()];
        buf.get(wkb);
        return new MySqlGeometry(wkb, srid);
    }

    /**
     * Returns the standard well-known binary representation of the MySQL byte
     *
     * @return {@link byte[]} which represents the standard well-known binary
     */
    public byte[] getWkb() {
        return wkb;
    }

    /**
     * Returns the coordinate reference system identifier (SRID)
     * @return srid
     */
    public int getSrid() {
        return srid;
    }

    /**
     * Returns whether this geometry is a 2D POINT type.
     * @return true if the geometry is a 2D Point.
     */
    public boolean isPoint() {
        return (coords != null);
    }

    /**
     * Returns the {x,y} coordinates of this geometry if it is a 2D Point.
     * @return {x,y} coordinate array, or null if the geometry is not a 2D Point.
     */
    public double[] getPointCoords() {
        return coords;
    }

    /**
     * Parses a 2D WKB Point into a {x,y} coordinate array.
     * Returns null for any non-point or points with Z/M/etc modifiers.
     * @param wkb OGC WKB geometry
     * @return {x,y} coordinate array, or null if the geometry is not a 2D Point
     */
    protected double[] parseWKBPoint(byte[] wkb) {
        if (wkb == null) {
            return null;
        } else if (wkb.length != WKB_POINT_SIZE) {
            return null;
        }

        final ByteBuffer reader = ByteBuffer.wrap(wkb);

        // Read the BOM
        reader.order((reader.get() != 0) ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);

        int geomType = reader.getInt();
        if (geomType != WKB_POINT) {
            // we only parse 2D points
            return null;
        }

        double x = reader.getDouble();
        double y = reader.getDouble();
        return new double[] {x, y};
    }

    /**
     * Create a GEOMETRYCOLLECTION EMPTY MySqlGeometry
     *
     * @return a {@link MySqlGeometry} which represents a MySqlGeometry API
     */
    public static MySqlGeometry createEmpty() {
        return new MySqlGeometry(WKB_EMPTY_GEOMETRYCOLLECTION, 0);
    }

}
