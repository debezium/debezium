/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spatial;

import java.nio.ByteBuffer;
import java.util.Arrays;

import io.debezium.DebeziumException;

/**
 * Swaps X/Y coordinates in geometry data.
 *
 * @author Chris Cranford
 */
class GeometryCoordinateSwapper {

    static final int[] DEFAULT_SWAP_SRIDS = { 4326, 3857, 4269 };

    /**
     * Swap geometry coordinates using default spatial reference identifiers.
     *
     * @param geometry geometry, should not be {@code null}
     * @return a new geometry instance with coordinates swapped if SRID matched
     */
    public static GeometryBytes swap(GeometryBytes geometry) {
        return swap(geometry, DEFAULT_SWAP_SRIDS);
    }

    /**
     * Swap geometry coordinates using default spatial reference identifiers.
     *
     * @param geometry geometry, should not be {@code null}
     * @param srids list of SRIDs to filter whether to swap coordinates by
     * @return a new geometry instance with coordinates swapped if SRID matched
     */
    public static GeometryBytes swap(GeometryBytes geometry, int... srids) {
        final Integer srid = geometry.getSrid();
        if (srid == null || !needsSwapping(srid, srids)) {
            return geometry;
        }
        return swapNoCheck(geometry);
    }

    /**
     * Swap geometry coordinates, as long as an SRID is present.
     *
     * @param geometry geometry, should not be {@code null}
     * @return a new geometry instance with coordinates swapped or existing geometry if no SRID is set
     */
    public static GeometryBytes swapNoCheck(GeometryBytes geometry) {
        if (geometry.getSrid() == null) {
            return geometry;
        }

        final byte[] swapped = swapCoordinates(geometry.getBytes());
        return new GeometryBytes(swapped, geometry.getSrid());
    }

    private static boolean needsSwapping(int srid, int[] allowedSrids) {
        return Arrays.stream(allowedSrids).anyMatch(n -> n == srid);
    }

    private static byte[] swapCoordinates(byte[] wkb) {
        try {
            final ByteBuffer input = ByteBuffer.wrap(wkb);
            final byte byteOrder = input.get();
            input.order(GeometryUtil.getByteOrder(byteOrder));

            final ByteBuffer output = ByteBuffer.allocate(wkb.length);
            output.order(input.order());
            output.put(byteOrder);

            GeometryTraverser.traverse(input, new SwapVisitor(output));

            return Arrays.copyOf(output.array(), output.position());
        }
        catch (Exception e) {
            throw new DebeziumException("Failed to swap EWKB/WKB coordinates", e);
        }
    }

    /**
     * Visitor implementation that writes the contents of an existing geometry byte array swapping x and y.
     *
     * @param output the output byte buffer, must not be {@code null}
     */
    private record SwapVisitor(ByteBuffer output) implements GeometryVisitor {

        @Override
        public boolean enterGeometry(int wkbType, boolean hasZ, boolean hasM, int stride) {
            output.putInt(wkbType);
            return true;
        }

        @Override
        public boolean enterCollectionGeometry(byte byteOrder) {
            output.put(byteOrder);
            return true;
        }

        @Override
        public boolean enterSubGeometry(byte byteOrder, int wkbType) {
            output.put(byteOrder);
            output.putInt(wkbType);
            return true;
        }

        @Override
        public void visitSrid(int srid) {
            output.putInt(srid);
        }

        @Override
        public void visitCoordinate(double[] ordinates) {
            // Swap X/Y
            output.putDouble(ordinates[1]);
            output.putDouble(ordinates[0]);

            // Copy remaining ordinates
            for (int i = 2; i < ordinates.length; i++) {
                output.putDouble(ordinates[i]);
            }
        }

        @Override
        public void startLineString(int pointCount) {
            output.putInt(pointCount);
        }

        @Override
        public void startPolygon(int ringCount) {
            output.putInt(ringCount);
        }

        @Override
        public void startRing(int ringIndex, int pointCount) {
            output.putInt(pointCount);
        }

        @Override
        public void startMultiPoint(int pointCount) {
            output.putInt(pointCount);
        }

        @Override
        public void startMultiLineString(int lineStringCount) {
            output.putInt(lineStringCount);
        }

        @Override
        public void startMultiPolygon(int polygonCount) {
            output.putInt(polygonCount);
        }

        @Override
        public void startGeometryCollection(int geometryCount) {
            output.putInt(geometryCount);
        }
    }

}
