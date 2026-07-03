/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spatial;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

/**
 * Builds OGC Well-Known Binary (WKB) byte arrays from raw {@code (x, y)} coordinates.
 * <p>
 * The rest of {@code io.debezium.spatial} reads or rewrites existing WKB/EWKB; this class is the
 * complement that encodes 2D geometries from scratch, mirroring the byte layout emitted by
 * {@code GeometryCoordinateSwapper.SwapVisitor} and {@code io.debezium.data.geometry.Point#buildWKBPoint}.
 * All output uses little-endian byte order and carries no SRID (these are planar, unreferenced
 * geometries), so it is strict OGC WKB.
 *
 * @author Debezium Authors
 */
public final class WkbWriter {

    private static final int HEADER_SIZE = 1 + 4; // byte order + geometry type
    private static final int COUNT_SIZE = 4; // a 4-byte element count (points or rings)
    private static final int COORDINATE_SIZE = 8 + 8; // x + y as doubles

    /**
     * Builds a WKB {@code LineString} from the given points.
     *
     * @param points the ordered vertices, each a {@code double[]{x, y}}; must not be {@code null}
     * @return the little-endian WKB byte array
     */
    public static byte[] buildLineString(List<double[]> points) {
        final ByteBuffer buffer = allocate(HEADER_SIZE + COUNT_SIZE + points.size() * COORDINATE_SIZE);
        writeHeader(buffer, GeometryConstants.LINE_STRING);
        writePoints(buffer, points);
        return buffer.array();
    }

    /**
     * Builds a WKB {@code Polygon} from the given rings. Each ring should already be closed
     * (first vertex equal to last).
     *
     * @param rings the rings, outer ring first; each ring a list of {@code double[]{x, y}}; must not be {@code null}
     * @return the little-endian WKB byte array
     */
    public static byte[] buildPolygon(List<List<double[]>> rings) {
        int capacity = HEADER_SIZE + COUNT_SIZE;
        for (List<double[]> ring : rings) {
            capacity += COUNT_SIZE + ring.size() * COORDINATE_SIZE;
        }

        final ByteBuffer buffer = allocate(capacity);
        writeHeader(buffer, GeometryConstants.POLYGON);
        buffer.putInt(rings.size());
        for (List<double[]> ring : rings) {
            writePoints(buffer, ring);
        }
        return buffer.array();
    }

    private static ByteBuffer allocate(int capacity) {
        final ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        return buffer;
    }

    private static void writeHeader(ByteBuffer buffer, int geometryType) {
        buffer.put(GeometryUtil.getByteOrderByte(ByteOrder.LITTLE_ENDIAN));
        buffer.putInt(geometryType);
    }

    private static void writePoints(ByteBuffer buffer, List<double[]> points) {
        buffer.putInt(points.size());
        for (double[] point : points) {
            buffer.putDouble(point[0]);
            buffer.putDouble(point[1]);
        }
    }

    private WkbWriter() {
    }
}
