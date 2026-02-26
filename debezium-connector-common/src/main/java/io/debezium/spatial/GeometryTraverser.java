/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spatial;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import io.debezium.DebeziumException;

/**
 * A traversal algorithm that uses the visitor pattern, {@link GeometryVisitor}, to perform
 * operations over an input byte buffer that represents a WKB or EWKB byte array.
 *
 * @author Chris Cranford
 */
class GeometryTraverser {

    /**
     * For a given byte buffer that contains a WKB/EWKB, applies the visitor to traverse the geometry.
     *
     * @param buffer the geometry byte buffer, should not be {@code null}
     * @param visitor the visit to apply, should not be {@code null}
     */
    public static void traverse(ByteBuffer buffer, GeometryVisitor visitor) {
        final int wkbType = buffer.getInt();

        final boolean hasZ = (wkbType & GeometryConstants.EWKB_SRID_Z_MASK) != 0;
        final boolean hasM = (wkbType & GeometryConstants.EWKB_SRID_M_MASK) != 0;
        final int stride = 2 + (hasZ ? 1 : 0) + (hasM ? 1 : 0);

        if (!visitor.enterGeometry(wkbType, hasZ, hasM, stride)) {
            return;
        }

        if (GeometryUtil.hasSrid(wkbType)) {
            visitor.visitSrid(buffer.getInt());
        }

        final int baseType = wkbType & GeometryConstants.WKB_TYPE_MASK;
        switch (baseType) {
            case GeometryConstants.POINT: // Point
                traversePoint(buffer, visitor, stride);
                break;
            case GeometryConstants.LINE_STRING: // Line String
                traverseLineString(buffer, visitor, stride);
                break;
            case GeometryConstants.POLYGON: // Polygon
                traversePolygon(buffer, visitor, stride);
                break;
            case GeometryConstants.MULTI_POINT: // Multi Point
                traverseMultiPoint(buffer, visitor, stride);
                break;
            case GeometryConstants.MULTI_LINE_STRING: // Multi Line String
                traverseMultiLineString(buffer, visitor, stride);
                break;
            case GeometryConstants.MULTI_POLYGON: // Multi Polygon
                traverseMultiPolygon(buffer, visitor, stride);
                break;
            case GeometryConstants.GEOMETRY_COLLECTION: // Geometry Collection
                traverseGeometryCollection(buffer, visitor);
                break;
            default:
                throw new DebeziumException("Invalid geometry type: " + baseType);
        }

        visitor.exitGeometry();
    }

    private static void traversePoint(ByteBuffer buffer, GeometryVisitor visitor, int stride) {
        final double[] ordinates = new double[stride];
        for (int i = 0; i < stride; i++) {
            ordinates[i] = buffer.getDouble();
        }
        visitor.visitCoordinate(ordinates);
    }

    private static void traverseLineString(ByteBuffer buffer, GeometryVisitor visitor, int stride) {
        final int numPoints = buffer.getInt();
        visitor.startLineString(numPoints);

        for (int i = 0; i < numPoints; i++) {
            traversePoint(buffer, visitor, stride);
        }

        visitor.endLineString();
    }

    private static void traversePolygon(ByteBuffer buffer, GeometryVisitor visitor, int stride) {
        final int numRings = buffer.getInt();
        visitor.startPolygon(numRings);

        for (int i = 0; i < numRings; i++) {
            final int numPoints = buffer.getInt();
            visitor.startRing(i, numPoints);

            for (int j = 0; j < numPoints; j++) {
                traversePoint(buffer, visitor, stride);
            }

            visitor.endRing();
        }

        visitor.endPolygon();
    }

    private static void traverseMultiPoint(ByteBuffer buffer, GeometryVisitor visitor, int stride) {
        final int numGeometries = buffer.getInt();
        visitor.startMultiPoint(numGeometries);

        for (int i = 0; i < numGeometries; i++) {
            final byte byteOrder = buffer.get();
            final ByteOrder order = GeometryUtil.getByteOrder(byteOrder);

            try (ByteOrderScope scope = new ByteOrderScope(buffer, order)) {
                final int wkbType = buffer.getInt();
                if (visitor.enterSubGeometry(byteOrder, wkbType)) {
                    traversePoint(buffer, visitor, stride);
                    visitor.exitSubGeometry();
                }
            }
        }

        visitor.endMultiPoint();
    }

    private static void traverseMultiLineString(ByteBuffer buffer, GeometryVisitor visitor, int stride) {
        final int numGeometries = buffer.getInt();
        visitor.startMultiLineString(numGeometries);

        for (int i = 0; i < numGeometries; i++) {
            final byte byteOrder = buffer.get();
            final ByteOrder order = GeometryUtil.getByteOrder(byteOrder);

            try (ByteOrderScope scope = new ByteOrderScope(buffer, order)) {
                final int wkbType = buffer.getInt();
                if (visitor.enterSubGeometry(byteOrder, wkbType)) {
                    traverseLineString(buffer, visitor, stride);
                    visitor.exitSubGeometry();
                }
            }
        }

        visitor.endMultiLineString();
    }

    private static void traverseMultiPolygon(ByteBuffer buffer, GeometryVisitor visitor, int stride) {
        final int numGeometries = buffer.getInt();
        visitor.startMultiPolygon(numGeometries);

        for (int i = 0; i < numGeometries; i++) {
            final byte byteOrder = buffer.get();
            final ByteOrder order = GeometryUtil.getByteOrder(byteOrder);

            try (ByteOrderScope scope = new ByteOrderScope(buffer, order)) {
                final int wkbType = buffer.getInt();
                if (visitor.enterSubGeometry(byteOrder, wkbType)) {
                    traversePolygon(buffer, visitor, stride);
                    visitor.exitSubGeometry();
                }
            }
        }

        visitor.endMultiPolygon();
    }

    private static void traverseGeometryCollection(ByteBuffer buffer, GeometryVisitor visitor) {
        final int numGeometries = buffer.getInt();
        visitor.startGeometryCollection(numGeometries);

        for (int i = 0; i < numGeometries; i++) {
            final byte byteOrder = buffer.get();
            final ByteOrder order = GeometryUtil.getByteOrder(byteOrder);

            try (ByteOrderScope scope = new ByteOrderScope(buffer, order)) {
                if (visitor.enterCollectionGeometry(byteOrder)) {
                    traverse(buffer, visitor);
                    visitor.exitCollectionGeometry();
                }
            }
        }

        visitor.endGeometryCollection();
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
