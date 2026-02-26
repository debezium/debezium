/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spatial;

/**
 * A visitor that is used by the {@link GeometryTraverser} to traverse a WKB/EKB byte buffer.
 *
 * @author Chris Cranford
 * @see GeometryCoordinateSwapper
 * @see GeometryEndiannessConverter
 */
interface GeometryVisitor {
    /**
     * Called when a geometry sequence has started.
     *
     * @param wkbType the WKB geometry type
     * @param hasZ whether the geometry has a Z-axis
     * @param hasM whether the geometry has an M-axis
     * @param stride the stride for given coordinates, 2D vs 3D
     * @return {@code true} if the visitor should continue visiting, {@code false} otherwise
     */
    default boolean enterGeometry(int wkbType, boolean hasZ, boolean hasM, int stride) {
        return true;
    }

    /**
     * Called when a a geometry sequence ends.
     */
    default void exitGeometry() {
    }

    /**
     * Called when a {@code GEOMETRYCOLLECTION} geometry type is observed.
     *
     * @param byteOrder the byte order
     * @return {@code true} if the visitor should continue visiting, {@code false} otherwise
     */
    default boolean enterCollectionGeometry(byte byteOrder) {
        return true;
    }

    /**
     * Called when leaving a {@code GEOMETRYCOLLECTION} geometry type.
     */
    default void exitCollectionGeometry() {
    }

    /**
     * Called just before visiting sub-geometry types inside a {@code MULTIPOINT}, {@code MULTILINESTRING},
     * or a {@code MULTIPOLYGON} geometry type.
     *
     * @param byteOrder the byte order
     * @param wkbType the sub geometry type
     * @return {@code true} if the visitor should continue visiting, {@code false} otherwise
     */
    default boolean enterSubGeometry(byte byteOrder, int wkbType) {
        return true;
    }

    /**
     * Called after leaving a sub-geometry type inside a {@code MULTIPOINT}, {@code MULTILINESTRING}, or a
     * {@code MULTIPOLYGON} geometry type.
     */
    default void exitSubGeometry() {
    }

    /**
     * Called after the geometry spatial reference identifier was read. If the Geometry is not an Extended
     * Well-Known Binary (EWKB) type that has the spatial reference identifier, this won't be called.
     *
     * @param srid the spatial reference identifier
     */
    default void visitSrid(int srid) {
    }

    /**
     * Called after reading the coordinates of a point.
     * The stride of the point can be determined by the number of values in the array.
     *
     * @param ordinates the coordinate values
     */
    default void visitCoordinate(double[] ordinates) {
    }

    /**
     * Called after reading the number of points inside a {@code LINESTRING} geometry type.
     *
     * @param pointCount the number of points
     */
    default void startLineString(int pointCount) {
    }

    /**
     * Called before leaving a {@code LINESTRING} geometry type.
     */
    default void endLineString() {
    }

    /**
     * Called after reading the number of rings inside a {@code POLYGON} geometry type.
     *
     * @param ringCount the number of rings
     */
    default void startPolygon(int ringCount) {
    }

    /**
     * Called before leaving a {@code POLYGON} geometry type.
     */
    default void endPolygon() {
    }

    /**
     * Called after reading the number of points present in a ring within a polygon.
     *
     * @param ringIndex the ring index, 0-based.
     * @param pointCount the number of points present within the ring
     */
    default void startRing(int ringIndex, int pointCount) {
    }

    /**
     * Called before leaving a ring within a polygon.
     */
    default void endRing() {
    }

    /**
     * Called after reading the number of points present in a {@code MULTIPOINT} geometry type.
     *
     * @param pointCount the number of points
     */
    default void startMultiPoint(int pointCount) {
    }

    /**
     * Called before leaving a {@code MULTIPOINT} geometry type.
     */
    default void endMultiPoint() {
    }

    /**
     * Called after reading the number of line strings present in a {@code MULTILINESTRING} geometry type.
     *
     * @param lineStringCount the number of line strings
     */
    default void startMultiLineString(int lineStringCount) {
    }

    /**
     * Called before leaving a {@code MULTILINESTRING} geometry type.
     */
    default void endMultiLineString() {
    }

    /**
     * Called after reading the number of polygons present in a {@code MULTIPOYLGON} geometry type.
     *
     * @param polygonCount the number of polygons
     */
    default void startMultiPolygon(int polygonCount) {
    }

    /**
     * Called before leaving a {@code MULTIPOLYGON} geometry type.
     */
    default void endMultiPolygon() {
    }

    /**
     * Called after reading the number of geometries present in a {@code GEOMETRYCOLLECTION} geometry type.
     *
     * @param geometryCount the number of geometries
     */
    default void startGeometryCollection(int geometryCount) {
    }

    /**
     * Called before leaving a {@code GEOMETRYCOLLECTION} geometry type.
     */
    default void endGeometryCollection() {
    }
}
