/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spatial;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import io.debezium.DebeziumException;

/**
 * Reads the 2D coordinates back out of an OGC Well-Known Binary (WKB) byte array produced by
 * {@link WkbWriter}. This is the complement used on the sink side to reconstruct native PostgreSQL
 * geometric text (e.g. {@code box}, {@code lseg}, {@code path}, {@code polygon}) from the WKB carried
 * on the wire, without requiring PostGIS.
 * <p>
 * It reuses {@link GeometryTraverser} to walk the geometry, so it stays in lock-step with the writer
 * and the rest of {@code io.debezium.spatial}.
 *
 * @author Debezium Authors
 */
public final class WkbReader {

    /**
     * Reads the ordered {@code (x, y)} vertices of a WKB {@code LineString}.
     *
     * @param wkb the little-endian WKB byte array; must not be {@code null}
     * @return the vertices, each a {@code double[]{x, y}}
     */
    public static List<double[]> readLineString(byte[] wkb) {
        return collectCoordinates(wkb);
    }

    /**
     * Reads the ordered {@code (x, y)} vertices of the single ring of a WKB {@code Polygon}. The
     * geometries Debezium emits for PostgreSQL {@code box}/{@code polygon} always carry exactly one
     * ring; a polygon with holes (multiple rings) cannot be reconstructed as a native PostgreSQL value
     * and is rejected rather than silently flattening the inner rings into the outer one.
     *
     * @param wkb the little-endian WKB byte array; must not be {@code null}
     * @return the ring vertices, each a {@code double[]{x, y}}
     * @throws DebeziumException if the polygon carries more than one ring
     */
    public static List<double[]> readPolygonRing(byte[] wkb) {
        final CoordinateCollector collector = new CoordinateCollector();
        traverse(wkb, collector);
        if (collector.ringCount > 1) {
            throw new DebeziumException("Cannot reconstruct a native PostgreSQL polygon from a multi-ring "
                    + "(polygon-with-holes) geometry; found " + collector.ringCount + " rings");
        }
        return collector.coordinates;
    }

    private static List<double[]> collectCoordinates(byte[] wkb) {
        final CoordinateCollector collector = new CoordinateCollector();
        traverse(wkb, collector);
        return collector.coordinates;
    }

    private static void traverse(byte[] wkb, GeometryVisitor visitor) {
        final ByteBuffer buffer = ByteBuffer.wrap(wkb);
        buffer.order(GeometryUtil.getByteOrder(buffer.get()));
        GeometryTraverser.traverse(buffer, visitor);
    }

    private static final class CoordinateCollector implements GeometryVisitor {

        private final List<double[]> coordinates = new ArrayList<>();
        private int ringCount;

        @Override
        public void startPolygon(int rings) {
            ringCount = rings;
        }

        @Override
        public void visitCoordinate(double[] ordinates) {
            coordinates.add(new double[]{ ordinates[0], ordinates[1] });
        }
    }

    private WkbReader() {
    }
}
