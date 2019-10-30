/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A parser API for MySQL Geometry types
 *
 * @author Omar Al-Safi
 * @author Robert Coup
 */
public class MySqlGeometry {

    // WKB constants from http://www.opengeospatial.org/standards/sfa
    private static final int WKB_POINT_SIZE = (1 + 4 + 8 + 8); // fixed size
    // WKB for a GEOMETRYCOLLECTION EMPTY object
    private static final byte[] WKB_EMPTY_GEOMETRYCOLLECTION = { 0x01, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }; // 0x010700000000000000

    /**
     * Open Geospatial Consortium Well-Known-Binary representation of the Geometry.
     * http://www.opengeospatial.org/standards/sfa
     */
    private final byte[] wkb;
    /**
     * Coordinate reference system identifier. While it's technically user-defined,
     * the standard/common values in use are the EPSG code list http://www.epsg.org/
     * null if unset/unknown
     */
    private final Integer srid;

    /**
     * Create a MySqlGeometry using the supplied wkb, note this should be the cleaned wkb for MySQL
     *
     * @param wkb the Well-Known binary representation of the coordinate in the standard format
     */
    private MySqlGeometry(byte[] wkb, Integer srid) {
        this.wkb = wkb;
        this.srid = srid;
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
        Integer srid = buf.getInt();
        if (srid == 0) {
            // Debezium uses null for an unset/unknown SRID
            srid = null;
        }

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
    public Integer getSrid() {
        return srid;
    }

    /**
     * Returns whether this geometry is a 2D POINT type.
     * @return true if the geometry is a 2D Point.
     */
    public boolean isPoint() {
        return wkb.length == WKB_POINT_SIZE;
    }

    /**
     * Create a GEOMETRYCOLLECTION EMPTY MySqlGeometry
     *
     * @return a {@link MySqlGeometry} which represents a MySqlGeometry API
     */
    public static MySqlGeometry createEmpty() {
        return new MySqlGeometry(WKB_EMPTY_GEOMETRYCOLLECTION, null);
    }
}
