/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spatial;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

import org.apache.kafka.connect.errors.ConnectException;

/**
 * A simple geometry bytes helper class to handle flipping of coordinate points.
 *
 * @author Chris Cranford
 */
public class GeometryBytes {

    private final byte[] wkb;
    private final Integer srid;

    public GeometryBytes(byte[] wkb, Integer srid) {
        this.wkb = wkb;
        this.srid = srid;
    }

    public GeometryBytes(byte[] ewkb) {
        this.wkb = ewkb;
        this.srid = extractSridFromEwkb(ewkb);
    }

    public byte[] getWkb() {
        return wkb;
    }

    public Integer getSrid() {
        return srid;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final GeometryBytes geometry = (GeometryBytes) o;
        return Objects.deepEquals(wkb, geometry.wkb) && Objects.equals(srid, geometry.srid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(wkb), srid);
    }

    /**
     * Swaps the coordinates, limited to swapping only based on specific SRIDs.
     *
     * @return a geometry bytes instance with coordinates swapped
     */
    public GeometryBytes swapCoordinates() {
        if (srid == null || !needsSwapping(srid)) {
            return this;
        }

        final byte[] flipped = swapWkbCoordinates(wkb);
        return new GeometryBytes(flipped, srid);
    }

    /**
     * Swaps the coordinates regardless of SRID.
     *
     * @return a geometry bytes instance with coordinates swapped
     */
    public GeometryBytes swapCoordinatesNoCheck() {
        if (srid == null) {
            return this;
        }

        final byte[] flipped = swapWkbCoordinates(wkb);
        return new GeometryBytes(flipped, srid);
    }

    private boolean needsSwapping(int srid) {
        return srid == 4326 || srid == 4269;
    }

    private Integer extractSridFromEwkb(byte[] ewkb) {
        if (ewkb == null || ewkb.length < 5) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(wkb);
        final byte byteOrder = buffer.get();
        buffer.order(byteOrder == 0x01 ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);

        final int wkbType = buffer.getInt();
        if (hasSrid(wkbType)) {
            return buffer.getInt();
        }

        return null;
    }

    private byte[] swapWkbCoordinates(byte[] wkb) {
        try {
            final ByteBuffer input = ByteBuffer.wrap(wkb);
            final byte byteOrder = input.get();
            input.order(getByteOrder(byteOrder));

            final ByteBuffer output = ByteBuffer.allocate(wkb.length);
            output.order(input.order());
            output.put(byteOrder);

            swapGeometry(input, output);

            return Arrays.copyOf(output.array(), output.position());
        }
        catch (Exception e) {
            throw new ConnectException("Failed to flip EWKB/WKB coordinates", e);
        }
    }

    private void swapGeometry(ByteBuffer input, ByteBuffer output) {
        final int wkbType = input.getInt();
        output.putInt(wkbType);

        final boolean hasZ = (wkbType & 0x80000000) != 0;
        final boolean hasM = (wkbType & 0x40000000) != 0;

        if (hasSrid(wkbType)) {
            output.putInt(input.getInt()); // Copy SRID
        }

        final int baseType = wkbType & 0xFF;
        final int stride = 2 + (hasZ ? 1 : 0) + (hasM ? 1 : 0);

        switch (baseType) {
            case 1: // Point
                swapPoint(input, output, stride);
                break;
            case 2: // LineString
                swapLineString(input, output, stride);
                break;
            case 3: // Polygon
                swapPolygon(input, output, stride);
                break;
            case 4: // MultiPoint
                swapMultiPoint(input, output, stride);
                break;
            case 5: // MultiLineString
                swapMultiLineString(input, output, stride);
                break;
            case 6: // MultiPolygon
                swapMultiPolygon(input, output, stride);
                break;
            case 7: // GeometryCollection
                swapGeometryCollection(input, output);
                break;
            default:
                throw new ConnectException("An invalid geometry type detected: " + baseType);
        }
    }

    private void swapPoint(ByteBuffer input, ByteBuffer output, int stride) {
        final double x = input.getDouble();
        final double y = input.getDouble();

        output.putDouble(y);
        output.putDouble(x);

        for (int i = 2; i < stride; i++) {
            output.putDouble(input.getDouble());
        }
    }

    private void swapLineString(ByteBuffer input, ByteBuffer output, int stride) {
        final int numPoints = input.getInt();
        output.putInt(numPoints);

        for (int i = 0; i < numPoints; i++) {
            swapPoint(input, output, stride);
        }
    }

    private void swapPolygon(ByteBuffer input, ByteBuffer output, int stride) {
        final int numRings = input.getInt();
        output.putInt(numRings);

        for (int i = 0; i < numRings; i++) {
            swapLineString(input, output, stride);
        }
    }

    private void swapMultiPoint(ByteBuffer input, ByteBuffer output, int stride) {
        final int numGeometries = input.getInt();
        output.putInt(numGeometries);

        for (int i = 0; i < numGeometries; i++) {
            final byte byteOrder = input.get();
            output.put(byteOrder);

            final ByteOrder order = getByteOrder(byteOrder);
            try (ByteOrderScope inputScope = new ByteOrderScope(input, order);
                    ByteOrderScope outputScope = new ByteOrderScope(output, order)) {

                int wkbType = input.getInt();
                output.putInt(wkbType);

                swapPoint(input, output, stride);

            }
        }
    }

    private void swapMultiLineString(ByteBuffer input, ByteBuffer output, int stride) {
        final int numGeometries = input.getInt();
        output.putInt(numGeometries);

        for (int i = 0; i < numGeometries; i++) {
            final byte byteOrder = input.get();
            output.put(byteOrder);

            final ByteOrder order = getByteOrder(byteOrder);
            try (ByteOrderScope inputScope = new ByteOrderScope(input, order);
                    ByteOrderScope outputScope = new ByteOrderScope(output, order)) {

                int wkbType = input.getInt();
                output.putInt(wkbType);

                swapLineString(input, output, stride);

            }
        }
    }

    private void swapMultiPolygon(ByteBuffer input, ByteBuffer output, int stride) {
        final int numGeometries = input.getInt();
        output.putInt(numGeometries);

        for (int i = 0; i < numGeometries; i++) {
            final byte byteOrder = input.get();
            output.put(byteOrder);

            final ByteOrder order = getByteOrder(byteOrder);
            try (ByteOrderScope inputScope = new ByteOrderScope(input, order);
                    ByteOrderScope outputScope = new ByteOrderScope(output, order)) {

                int wkbType = input.getInt();
                output.putInt(wkbType);

                swapPolygon(input, output, stride);

            }
        }
    }

    private void swapGeometryCollection(ByteBuffer input, ByteBuffer output) {
        final int numGeometries = input.getInt();
        output.putInt(numGeometries);

        for (int i = 0; i < numGeometries; i++) {
            final byte byteOrder = input.get();
            output.put(byteOrder);

            final ByteOrder order = getByteOrder(byteOrder);
            try (ByteOrderScope inputScope = new ByteOrderScope(input, order);
                    ByteOrderScope outputScope = new ByteOrderScope(output, order)) {

                swapGeometry(input, output);
            }
        }
    }

    private static boolean hasSrid(int wkbType) {
        return (wkbType & 0x20000000) != 0;
    }

    private static ByteOrder getByteOrder(byte byteOrder) {
        return byteOrder == 0x01 ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
    }

    /**
     * Helper class that changes the {@link ByteOrder} of a {@link ByteBuffer} if the new order
     * differs, restoring the order after the scope closes automatically.
     */
    private static class ByteOrderScope implements AutoCloseable {

        private final ByteBuffer buffer;
        private final ByteOrder byteOrder;

        ByteOrderScope(ByteBuffer buffer, ByteOrder newByteOrder) {
            this.buffer = buffer;
            this.byteOrder = buffer.order();

            if (buffer.order() != newByteOrder) {
                buffer.order(newByteOrder);
            }
        }

        @Override
        public void close() {
            if (buffer.order() != byteOrder) {
                buffer.order(byteOrder);
            }
        }
    }
}
