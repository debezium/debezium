/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.singlestore;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.type.debezium.AbstractGeometryType;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.spatial.GeometryBytes;

import mil.nga.wkb.geom.LineString;
import mil.nga.wkb.geom.Point;
import mil.nga.wkb.geom.Polygon;
import mil.nga.wkb.io.ByteReader;
import mil.nga.wkb.io.WkbGeometryReader;

/**
 * An implementation of {@link JdbcType} for SingleStore {@code GEOGRAPHY} columns.
 */
class GeometryType extends AbstractGeometryType {

    public static final GeometryType INSTANCE = new GeometryType();

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "?";
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "geography";
    }

    @Override
    protected List<ValueBindDescriptor> getNullGeometryBinding(int index) {
        return bindWkt(index, null);
    }

    @Override
    protected List<ValueBindDescriptor> getEmptyGeometryCollectionBinding(int index, GeometryBytes geometry) {
        throw unsupportedGeometry("GEOMETRYCOLLECTION");
    }

    @Override
    protected List<ValueBindDescriptor> getGeometryBinding(int index, GeometryBytes geometry) {
        return bindWkt(index, toWkt(readGeometry(geometry)));
    }

    protected List<ValueBindDescriptor> bindWkt(int index, String wkt) {
        return List.of(new ValueBindDescriptor(index, wkt));
    }

    protected mil.nga.wkb.geom.Geometry readGeometry(GeometryBytes geometry) {
        try {
            return WkbGeometryReader.readGeometry(new ByteReader(geometry.asWkb().getBytes()));
        }
        catch (RuntimeException e) {
            throw new ConnectException("Failed to decode SingleStore geospatial WKB value.", e);
        }
    }

    protected String toWkt(mil.nga.wkb.geom.Geometry geometry) {
        if (geometry instanceof Point point) {
            return toWkt(point);
        }
        if (geometry instanceof LineString lineString) {
            return toWkt(lineString);
        }
        if (geometry instanceof Polygon polygon) {
            return toWkt(polygon);
        }
        throw unsupportedGeometry(geometry.getGeometryType().getName());
    }

    protected ConnectException unsupportedGeometry(String geometryType) {
        return new ConnectException(String.format(
                "SingleStore dialect supports only POINT, LINESTRING, and POLYGON geospatial WKB types, but found '%s'.",
                geometryType));
    }

    private String toWkt(Point point) {
        return "POINT (" + coordinate(point) + ")";
    }

    private String toWkt(LineString lineString) {
        if (lineString.getPoints().isEmpty()) {
            throw unsupportedGeometry("LINESTRING EMPTY");
        }
        return "LINESTRING (" + points(lineString) + ")";
    }

    private String toWkt(Polygon polygon) {
        if (polygon.getRings().isEmpty()) {
            throw unsupportedGeometry("POLYGON EMPTY");
        }
        return "POLYGON (" + polygon.getRings().stream()
                .map(this::ring)
                .collect(Collectors.joining(", ")) + ")";
    }

    private String ring(LineString ring) {
        if (ring.getPoints().isEmpty()) {
            throw unsupportedGeometry("POLYGON EMPTY RING");
        }
        return "(" + points(ring) + ")";
    }

    private String points(LineString lineString) {
        return lineString.getPoints().stream()
                .map(this::coordinate)
                .collect(Collectors.joining(", "));
    }

    private String coordinate(Point point) {
        return format(point.getX()) + " " + format(point.getY());
    }

    private String format(double value) {
        if (!Double.isFinite(value)) {
            throw new ConnectException(String.format("Invalid SingleStore geospatial coordinate '%s'.", value));
        }
        return BigDecimal.valueOf(value).stripTrailingZeros().toPlainString();
    }
}
