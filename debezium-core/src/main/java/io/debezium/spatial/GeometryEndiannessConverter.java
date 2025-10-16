/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spatial;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import io.debezium.DebeziumException;

/**
 * Converts a geometry's byte order (endianness).
 *
 * @author Chris Cranford
 */
class GeometryEndiannessConverter {

    /**
     * Creates a new {@link GeometryBytes} using little endian format.
     *
     * @param geometry the geometry to convert, must not be {@code null}
     * @return the converted geometry
     */
    public static GeometryBytes toLittleEndian(GeometryBytes geometry) {
        return convert(geometry, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Creates a new {@link GeometryBytes} using big endian format.
     *
     * @param geometry the geometry to convert, must not be {@code null}
     * @return the converted geometry
     */
    public static GeometryBytes toBigEndian(GeometryBytes geometry) {
        return convert(geometry, ByteOrder.BIG_ENDIAN);
    }

    private static GeometryBytes convert(GeometryBytes geometry, ByteOrder targetByteOrder) {
        try {
            final ByteBuffer input = ByteBuffer.wrap(geometry.getBytes());
            input.order(GeometryUtil.getByteOrder(input.get()));

            final ByteBuffer output = ByteBuffer.allocate(geometry.getBytes().length);
            output.put(GeometryUtil.getByteOrderByte(targetByteOrder));
            output.order(targetByteOrder);

            GeometryTraverser.traverse(input, new EndianConverterVisitor(output, targetByteOrder));

            return new GeometryBytes(output.array(), geometry.getSrid());
        }
        catch (Exception e) {
            throw new DebeziumException("Failed to convert geometry endianness", e);
        }
    }

    /**
     * Visitor implementation that writes to the output buffer the contents of an existing geometry
     * representation, but using the given target byte order.
     *
     * @param output the output byte buffer to write into, must not be {@code null}
     * @param targetByteOrder the target byte order that the output uses
     */
    private record EndianConverterVisitor(ByteBuffer output, ByteOrder targetByteOrder) implements GeometryVisitor {
        @Override
        public boolean enterGeometry(int wkbType, boolean hasZ, boolean hasM, int stride) {
            output.putInt(wkbType);
            return true;
        }

        @Override
        public boolean enterCollectionGeometry(byte byteOrder) {
            output.put(GeometryUtil.getByteOrderByte(targetByteOrder));
            return true;
        }

        @Override
        public boolean enterSubGeometry(byte byteOrder, int wkbType) {
            output.put(GeometryUtil.getByteOrderByte(targetByteOrder));
            output.putInt(wkbType);
            return true;
        }

        @Override
        public void visitSrid(int srid) {
            output.putInt(srid);
        }

        @Override
        public void visitCoordinate(double[] ordinates) {
            Arrays.stream(ordinates).forEach(output::putDouble);
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
