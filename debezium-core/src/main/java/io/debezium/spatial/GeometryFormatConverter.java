/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spatial;

import java.nio.ByteBuffer;

/**
 * Utility class for converting a {@link GeometryBytes} between WKB and EWKB formats.
 *
 * @author Chris Cranford
 */
class GeometryFormatConverter {

    /**
     * Converts geometry to use the Extended Well-Known Binary (EWKB) format.
     *
     * @param geometry geometry, should not be {@code null}
     * @return the geometry instance using EWKB format
     */
    public static GeometryBytes toExtendedWkb(GeometryBytes geometry) {
        if (geometry.isExtended()) {
            return geometry;
        }

        final byte[] extendedWkb = convertToExtendedWkb(geometry.getBytes(), geometry.getSrid());
        return new GeometryBytes(extendedWkb, geometry.getSrid());
    }

    /**
     * Converts geometry to use the Well-Known Binary (WKB) format.
     *
     * @param geometry geometry, should not be {@code null}
     * @return the geometry instance using WKB format
     */
    public static GeometryBytes toWkb(GeometryBytes geometry) {
        if (!geometry.isExtended()) {
            return geometry;
        }

        final byte[] wkb = convertToWkb(geometry.getBytes());
        return new GeometryBytes(wkb, geometry.getSrid());
    }

    private static byte[] convertToExtendedWkb(byte[] wkb, Integer srid) {
        final ByteBuffer buffer = ByteBuffer.wrap(wkb);
        final byte byteOrder = buffer.get();
        buffer.order(GeometryUtil.getByteOrder(byteOrder));

        int wkbType = buffer.getInt();
        wkbType = wkbType | 0x20000000; // Add SRID flag

        final ByteBuffer output = ByteBuffer.allocate(wkb.length + 4);
        output.put(byteOrder);
        output.order(buffer.order());
        output.putInt(wkbType);
        output.putInt(srid);
        output.put(buffer);

        return output.array();
    }

    private static byte[] convertToWkb(byte[] extendedWkb) {
        final ByteBuffer buffer = ByteBuffer.wrap(extendedWkb);
        final byte byteOrder = buffer.get();
        buffer.order(GeometryUtil.getByteOrder(byteOrder));

        int wkbType = buffer.getInt();
        wkbType = wkbType & ~0x20000000; // Remove SRID flag
        buffer.getInt(); // Skip SRID

        final ByteBuffer output = ByteBuffer.allocate(extendedWkb.length - 4);
        output.put(byteOrder);
        output.order(buffer.order());
        output.putInt(wkbType);
        output.put(buffer);

        return output.array();
    }
}
