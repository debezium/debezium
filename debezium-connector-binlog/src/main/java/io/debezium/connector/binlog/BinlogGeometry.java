/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A simple parser API for binlog-based connector geometry data types.
 *
 * @author Omar Al-Safi
 * @author Robert Coup
 * @author Chris Cranford
 */
public class BinlogGeometry {

    // WKB constants from http://www.opengeospatial.org/standards/sfa
    private static final int WKB_POINT_SIZE = (1 + 4 + 8 + 8); // fixed size

    // WKB for a GEOMETRYCOLLECTION EMPTY object
    // 0x010700000000000000
    private static final byte[] WKB_EMPTY_GEOMETRYCOLLECTION = { 0x01, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };

    /**
     * Open Geospatial Consortium Well-Known-Binary representation of the Geometry.
     * http://www.opengeospatial.org/standards/sfa
     */
    private final byte[] wkb;

    /**
     * Coordinate reference system identifier. While it's technically user-defined, the standard/common values
     * in use are the EPSG code list http://www.epsg.org/ null if unset/unknown
     */
    private final Integer srid;

    /**
     * Create a BinlogGeometry using the supplied wkb, note this should be the cleaned wkb
     *
     * @param wkb the Well-Known binary representation of the coordinate in the standard format
     */
    private BinlogGeometry(byte[] wkb, Integer srid) {
        this.wkb = wkb;
        this.srid = srid;
    }

    /**
     * Create a BinlogGeometry from the original byte array from binlog event
     *
     * @param mysqlBytes he original byte array from binlog event
     *
     * @return a {@link BinlogGeometry} which represents a BinlogGeometry API
     */
    public static BinlogGeometry fromBytes(final byte[] mysqlBytes) {
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
        return new BinlogGeometry(wkb, srid);
    }

    /**
     * Returns the standard well-known binary representation.
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
     * Create a GEOMETRYCOLLECTION EMPTY BinlogGeometry
     *
     * @return a {@link BinlogGeometry} which represents a BinlogGeometry API
     */
    public static BinlogGeometry createEmpty() {
        return new BinlogGeometry(WKB_EMPTY_GEOMETRYCOLLECTION, null);
    }
}
