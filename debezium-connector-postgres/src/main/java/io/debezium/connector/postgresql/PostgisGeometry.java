/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import io.debezium.util.HexConverter;

/**
 * A parser API for Postgres Geometry types
 *
 * @author Robert Coup
 */
public class PostgisGeometry {

    /**
     * Static Hex EKWB for a GEOMETRYCOLLECTION EMPTY.
     */
    private static final String HEXEWKB_EMPTY_GEOMETRYCOLLECTION = "010700000000000000";

    /**
     * PostGIS Extended-Well-Known-Binary (EWKB) geometry representation. An extension of the
     * Open Geospatial Consortium Well-Known-Binary format. Since EWKB is a superset of
     * WKB, we use EWKB here.
     * https://postgis.net/docs/using_postgis_dbmanagement.html#EWKB_EWKT
     * http://www.opengeospatial.org/standards/sfa
     */
    private final byte[] wkb;

    /**
     * Coordinate reference system identifier. While it's technically user-defined,
     * the standard/common values in use are the EPSG code list http://www.epsg.org/
     * null if unset/unspecified
     */
    private final Integer srid;

    /**
     * Create a PostgisGeometry using the supplied PostGIS Hex EWKB string.
     * SRID is extracted from the EWKB
     */
    public static PostgisGeometry fromHexEwkb(String hexEwkb) {
        byte[] ewkb = HexConverter.convertFromHex(hexEwkb);
        return fromEwkb(ewkb);
    }

    /**
     * Create a PostgisGeometry using the supplied PostGIS EWKB.
     * SRID is extracted from the EWKB
     */
    public static PostgisGeometry fromEwkb(byte[] ewkb) {
        return new PostgisGeometry(ewkb, parseSrid(ewkb));
    }

    /**
     * Create a GEOMETRYCOLLECTION EMPTY PostgisGeometry
     *
     * @return a {@link PostgisGeometry} which represents a PostgisGeometry API
     */
    public static PostgisGeometry createEmpty() {
        return PostgisGeometry.fromHexEwkb(HEXEWKB_EMPTY_GEOMETRYCOLLECTION);
    }

    /**
     * Create a PostgisGeometry using the supplied EWKB and SRID.
     *
     * @param ewkb the Extended Well-Known binary representation of the coordinate in the standard format
     * @param srid the coordinate system identifier (SRID); null if unset/unknown
     */
    private PostgisGeometry(byte[] ewkb, Integer srid) {
        this.wkb = ewkb;
        this.srid = srid;
    }

    /**
     * Returns the standard well-known binary representation
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
     * Parses an EWKB Geometry and extracts the SRID (if any)
     *
     * https://trac.osgeo.org/postgis/browser/trunk/doc/ZMSgeoms.txt
     *
     * @param ewkb PostGIS EWKB geometry
     * @return Geometry SRID or null if there is no SRID.
     */
    private static Integer parseSrid(byte[] ewkb) {
        if (ewkb.length < 9) {
            throw new IllegalArgumentException("Invalid EWKB length");
        }

        final ByteBuffer reader = ByteBuffer.wrap(ewkb);

        // Read the BOM
        reader.order((reader.get() != 0) ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);

        int geomType = reader.getInt();
        final int EWKB_SRID = 0x20000000;
        // SRID flag in the type integer
        if ((geomType & EWKB_SRID) != 0) {
            // SRID is set
            // value is encoded as a 4 byte integer right after the type integer.
            return reader.getInt();
        }
        return null;
    }
}
