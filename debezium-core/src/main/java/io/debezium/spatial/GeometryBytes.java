/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spatial;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

import io.debezium.util.HexConverter;

/**
 * A simple geometry bytes helper class to handle operating on WKB (Well-Known Binary) and EWKB
 * (Extended Well-Known Binary) formats used to represent spatial geometry.
 *
 * @author Chris Cranford
 */
public class GeometryBytes {

    private final byte[] wkb;
    private final Integer srid;

    /**
     * Create a geometry with a given byte array and an optional spatial reference identifier.
     *
     * @param wkb byte array, must not be {@code null}
     * @param srid spatial reference identifier, can be {@code null}
     */
    public GeometryBytes(byte[] wkb, Integer srid) {
        this.wkb = wkb;
        this.srid = srid;
    }

    /**
     * Create a geometry with a given Extended Well-Known Binary (EWKB) format byte array.
     * The SRID is decoded from the provided byte array.
     *
     * @param ewkb byte array, must not be {@code null}
     */
    public GeometryBytes(byte[] ewkb) {
        this(ewkb, GeometryUtil.extractSrid(ewkb));
    }

    /**
     * Get the raw geometry bytes.
     *
     * @return geometry byte array, never {@code null}
     */
    public byte[] getBytes() {
        return wkb;
    }

    /**
     * Get the spatial reference identifier.
     *
     * @return spatial reference identifier, may be {@code null}
     */
    public Integer getSrid() {
        return srid;
    }

    /**
     * Returns whether the geometry bytes using the Extended Well-Known Binary (EWKB) format.
     */
    public boolean isExtended() {
        return GeometryUtil.isExtended(wkb);
    }

    /**
     * Returns whether the geometry represent a geometry collection that has no geometry elements.
     */
    public boolean isEmptyGeometryCollection() {
        if (wkb == null || wkb.length < 5) {
            return false;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(wkb);
        buffer.order(GeometryUtil.getByteOrder(buffer.get()));

        final EmptyGeometryCollectionVisitor visitor = new EmptyGeometryCollectionVisitor();
        GeometryTraverser.traverse(buffer, visitor);

        return visitor.isGeometryCollection && visitor.isEmpty;
    }

    /**
     * Swaps coordinates by limiting the swap to {@link GeometryCoordinateSwapper#DEFAULT_SWAP_SRIDS}.
     *
     * @return a new geometry instance that may or may not have its coordinated swapped.
     */
    public GeometryBytes swapCoordinates() {
        return GeometryCoordinateSwapper.swap(this);
    }

    /**
     * Swaps coordinates regardless of spatial reference identifiers.
     *
     * @return a geometry instance with the coordinates swapped
     */
    public GeometryBytes swapCoordinatesNoCheck() {
        return GeometryCoordinateSwapper.swapNoCheck(this);
    }

    /**
     * Provides a geometry instance that uses EWKB format. If the current instance is already in EWKB format,
     * the method returns the same instance.
     */
    public GeometryBytes asExtendedWkb() {
        return GeometryFormatConverter.toExtendedWkb(this);
    }

    /**
     * Provides a geometry instance that uses WKB format. If the current instance is already in WKB format,
     * the method returns the same instance.
     */
    public GeometryBytes asWkb() {
        return GeometryFormatConverter.toWkb(this);
    }

    /**
     * Returns a new geometry instance using little endian byte order.
     */
    public GeometryBytes asLittleEndian() {
        return GeometryEndiannessConverter.toLittleEndian(this);
    }

    /**
     * Returns a new geometry instance using big endian byte order.
     */
    public GeometryBytes asBigEndian() {
        return GeometryEndiannessConverter.toBigEndian(this);
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

    @Override
    public String toString() {
        return "GeometryBytes{" +
                "wkb=" + HexConverter.convertToHexString(wkb) +
                ", srid=" + srid +
                '}';
    }

    /**
     * Utility visitor that checks if the {@code GEOMETRYCOLLECTION} is empty.
     */
    static class EmptyGeometryCollectionVisitor implements GeometryVisitor {
        boolean isGeometryCollection = false;
        boolean isEmpty = true;

        @Override
        public void startGeometryCollection(int geometryCount) {
            isGeometryCollection = true;
            isEmpty = (geometryCount == 0);
        }
    }
}
