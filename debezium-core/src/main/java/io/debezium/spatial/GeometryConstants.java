/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spatial;

/**
 * Constants used by the geometry spatial classes.
 *
 * @author Chris Cranford
 */
final class GeometryConstants {

    // Extended WKB format SRID byte mask/flags
    static final int EWKB_SRID_Z_MASK = 0x80000000;
    static final int EWKB_SRID_M_MASK = 0x40000000;
    static final int EWKB_SRID_FLAG = 0x20000000;

    static final int WKB_TYPE_MASK = 0xFF;

    // Byte orders
    static final int BIG_BYTE_ORDER = 0x00;
    static final int LITTLE_BYTE_ORDER = 0x01;

    // Geometry types
    static final int POINT = 1;
    static final int LINE_STRING = 2;
    static final int POLYGON = 3;
    static final int MULTI_POINT = 4;
    static final int MULTI_LINE_STRING = 5;
    static final int MULTI_POLYGON = 6;
    static final int GEOMETRY_COLLECTION = 7;

    private GeometryConstants() {
    }
}
