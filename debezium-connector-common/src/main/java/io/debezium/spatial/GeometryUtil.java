/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spatial;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Simple geometry utility class.
 *
 * @author Chris Cranford
 */
public class GeometryUtil {

    /**
     * Check whether the WKB byte array represents a EWKB (Extended Well-Known Binary) format.
     *
     * @param wkb the byte array
     * @return {@code true} if the array is an EWKB, {@code false} if it is a WKB
     */
    public static boolean isExtended(byte[] wkb) {
        if (wkb == null || wkb.length < 5) {
            return false;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(wkb);
        buffer.order(getByteOrder(buffer.get()));

        final int type = buffer.getInt();
        return (type & GeometryConstants.EWKB_SRID_FLAG) != 0;
    }

    /**
     * Reads the spatial reference identifier from a potential EWKB byte array.
     *
     * @param ewkb the byte array
     * @return the SRID if the byte array is an EWKB and has an encoded SRID, otherwise {@code null}
     */
    public static Integer extractSrid(byte[] ewkb) {
        if (ewkb == null || ewkb.length < 5) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(ewkb);
        final byte byteOrder = buffer.get();
        buffer.order(getByteOrder(byteOrder));

        final int wkbType = buffer.getInt();
        if (hasSrid(wkbType)) {
            return buffer.getInt();
        }

        return null;
    }

    /**
     * Checks whether the Well-Known Binary type has the SRID flag encoded.
     *
     * @param wkbType the WKB type
     * @return {@code true} if the SRID is encoded, {@code false} if the SRID is not encoded
     */
    public static boolean hasSrid(int wkbType) {
        return (wkbType & GeometryConstants.EWKB_SRID_FLAG) != 0;
    }

    /**
     * Given the WKB byte (0x00 or 0x01) byte order, returns the Java byte order.
     *
     * @param byteOrder the WKB byte order byte
     * @return the Java byte order
     */
    public static ByteOrder getByteOrder(byte byteOrder) {
        return byteOrder == GeometryConstants.LITTLE_BYTE_ORDER ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
    }

    /**
     * Given a Java byte order, returns the mapped WKB type (0x00 or 0x01).
     *
     * @param byteOrder the Java byte order
     * @return the WKB byte order
     */
    public static byte getByteOrderByte(ByteOrder byteOrder) {
        return (byte) (byteOrder == ByteOrder.LITTLE_ENDIAN
                ? GeometryConstants.LITTLE_BYTE_ORDER
                : GeometryConstants.BIG_BYTE_ORDER);
    }

    private GeometryUtil() {
    }
}
