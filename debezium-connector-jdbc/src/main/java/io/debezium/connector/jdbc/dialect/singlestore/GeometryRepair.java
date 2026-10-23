/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.singlestore;

import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import org.apache.kafka.connect.errors.ConnectException;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateArrays;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFilter;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.util.GeometryFixer;

import mil.nga.wkb.geom.LineString;
import mil.nga.wkb.geom.Point;

/**
 * Repairs the complete polygonal geometry after rounding coordinates to eight decimal places.
 */
final class GeometryRepair {

    private static final int MAX_FIX_ATTEMPTS = 3;

    private GeometryRepair() {
    }

    static String polygon(List<List<LineString>> polygons) {
        final GeometryFactory factory = new GeometryFactory();
        final List<Polygon> components = new ArrayList<>(polygons.size());
        for (List<LineString> rings : polygons) {
            if (rings.isEmpty()) {
                throw new ConnectException("SingleStore polygon is empty.");
            }
            final LinearRing shell = ring(factory, rings.get(0));
            final LinearRing[] holes = new LinearRing[rings.size() - 1];
            for (int i = 1; i < rings.size(); i++) {
                holes[i - 1] = ring(factory, rings.get(i));
            }
            components.add(factory.createPolygon(shell, holes));
        }
        final Geometry input = components.size() == 1 ? components.get(0) : factory.createMultiPolygon(components.toArray(Polygon[]::new));
        Geometry result = input;
        if (result.isValid()) {
            return toSingleStoreWkt(result);
        }
        for (int attempt = 0; attempt < MAX_FIX_ATTEMPTS; attempt++) {
            final Geometry fixed = fix(result);
            requireAreal(fixed);
            roundCoordinates(fixed);
            if (fixed.isValid()) {
                return toSingleStoreWkt(fixed);
            }
            if (fixed.equalsExact(result)) {
                break;
            }
            result = fixed;
        }
        throw new ConnectException("SingleStore polygon remains invalid after repair.");
    }

    private static LinearRing ring(GeometryFactory factory, LineString source) {
        final List<Point> points = source.getPoints();
        final Coordinate[] coordinates = new Coordinate[points.size()];
        for (int i = 0; i < points.size(); i++) {
            final Point point = points.get(i);
            final double x = round(point.getX());
            final double y = round(point.getY());
            if (x < -180 || x > 180 || y < -90 || y > 90) {
                throw new ConnectException("SingleStore polygon coordinates must be valid longitude and latitude values.");
            }
            coordinates[i] = new Coordinate(x, y);
        }
        if (!CoordinateArrays.isRing(coordinates)) {
            throw new ConnectException("SingleStore polygon ring must be closed and contain at least three vertices.");
        }
        return factory.createLinearRing(coordinates);
    }

    private static void roundCoordinates(Geometry geometry) {
        // GeometryFixer returns a new geometry, so only this repair result is modified.
        geometry.apply(new CoordinateSequenceFilter() {
            @Override
            public void filter(CoordinateSequence sequence, int index) {
                sequence.setOrdinate(index, CoordinateSequence.X, round(sequence.getX(index)));
                sequence.setOrdinate(index, CoordinateSequence.Y, round(sequence.getY(index)));
            }

            @Override
            public boolean isDone() {
                return false;
            }

            @Override
            public boolean isGeometryChanged() {
                return true;
            }
        });
    }

    private static double round(double value) {
        return GeometryType.decimal(value).setScale(8, RoundingMode.HALF_UP).doubleValue();
    }

    private static void requireAreal(Geometry geometry) {
        if (geometry.isEmpty() || geometry.getArea() == 0 || !(geometry instanceof Polygon || geometry instanceof MultiPolygon)) {
            throw new ConnectException("SingleStore polygon repair produced no valid polygonal area.");
        }
    }

    private static Geometry fix(Geometry geometry) {
        try {
            return GeometryFixer.fix(geometry);
        }
        catch (RuntimeException e) {
            throw new ConnectException("SingleStore polygon repair failed while fixing invalid geometry.", e);
        }
    }

    private static String toSingleStoreWkt(Geometry geometry) {
        requireAreal(geometry);
        final StringJoiner rings = new StringJoiner(", ", "POLYGON (", ")");
        for (int i = 0; i < geometry.getNumGeometries(); i++) {
            final Polygon polygon = (Polygon) geometry.getGeometryN(i);
            rings.add(ringWkt(polygon.getExteriorRing().getCoordinates()));
            for (int hole = 0; hole < polygon.getNumInteriorRing(); hole++) {
                rings.add(ringWkt(polygon.getInteriorRingN(hole).getCoordinates()));
            }
        }
        return rings.toString();
    }

    private static String ringWkt(Coordinate[] coordinates) {
        final StringJoiner points = new StringJoiner(", ", "(", ")");
        for (Coordinate coordinate : coordinates) {
            points.add(GeometryType.format(coordinate.x) + " " + GeometryType.format(coordinate.y));
        }
        return points.toString();
    }
}
