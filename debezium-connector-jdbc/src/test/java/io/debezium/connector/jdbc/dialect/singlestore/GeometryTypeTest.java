/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.singlestore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Base64;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.doc.FixFor;

import mil.nga.wkb.geom.LineString;
import mil.nga.wkb.geom.MultiPoint;
import mil.nga.wkb.geom.MultiPolygon;
import mil.nga.wkb.geom.Point;
import mil.nga.wkb.geom.Polygon;
import mil.nga.wkb.io.ByteWriter;
import mil.nga.wkb.io.WkbGeometryWriter;

/**
 * Unit tests for the SingleStore {@link GeometryType} handler.
 */
@Tag("UnitTests")
class GeometryTypeTest {

    private static final byte[] POINT_WKB = Base64.getDecoder().decode("AQEAAAAAAAAAAADwPwAAAAAAAPA/");

    private static final byte[] POLYGON_WKB = Base64.getDecoder().decode(
            "AQMAAAABAAAABQAAAAAAAAAAAAAAAAAAAAAAFEAAAAAAAAAAQAAAAAAAABRAAAAAAAAAAEAAAAAAAAAcQAAAAAAAAAAAAAAAAAAAHEAAAAAAAAAAAAAAAAAAABRA");

    @Test
    @DisplayName("Should register for Debezium geometry and geography logical names")
    void testRegistrationKeys() {
        assertThat(GeometryType.INSTANCE.getRegistrationKeys())
                .containsExactlyInAnyOrder(Geometry.LOGICAL_NAME, Geography.LOGICAL_NAME);
    }

    @Test
    @DisplayName("Should return simple parameter binding for SingleStore")
    void testQueryBinding() {
        assertThat(GeometryType.INSTANCE.getQueryBinding(null, null, null)).isEqualTo("?");
    }

    @Test
    @DisplayName("Should return geography as type name for SingleStore")
    void testTypeName() {
        assertThat(GeometryType.INSTANCE.getTypeName(Geometry.schema(), false)).isEqualTo("geography");
    }

    @Test
    @DisplayName("Should bind null geometry values as a single parameter")
    void testBindNullGeometry() {
        assertThat(GeometryType.INSTANCE.bind(1, Geometry.schema(), null))
                .singleElement()
                .satisfies(binding -> {
                    assertThat(binding.getIndex()).isEqualTo(1);
                    assertThat(binding.getValue()).isNull();
                });
    }

    @Test
    @DisplayName("Should bind point WKB values as WKT")
    void testBindPointAsWkt() {
        assertThat(GeometryType.INSTANCE.bind(1, Geometry.schema(), Geometry.createValue(Geometry.schema(), POINT_WKB, 4326)))
                .singleElement()
                .satisfies(binding -> {
                    assertThat(binding.getIndex()).isEqualTo(1);
                    assertThat(binding.getValue()).isEqualTo("POINT (1 1)");
                });
    }

    @Test
    @DisplayName("Should bind polygon WKB values as WKT")
    void testBindPolygonAsWkt() {
        assertThat(GeometryType.INSTANCE.bind(1, Geometry.schema(), Geometry.createValue(Geometry.schema(), POLYGON_WKB, 4326)))
                .singleElement()
                .satisfies(binding -> {
                    assertThat(binding.getIndex()).isEqualTo(1);
                    assertThat(binding.getValue()).isEqualTo("POLYGON ((0 5, 2 5, 2 7, 0 7, 0 5))");
                });
    }

    @Test
    @DisplayName("Should reject WKB types that SingleStore does not support")
    void testShouldRejectUnsupportedGeometryTypes() throws Exception {
        final MultiPoint multiPoint = new MultiPoint(false, false);
        multiPoint.addPoint(new mil.nga.wkb.geom.Point(1.0, 1.0));

        assertThatThrownBy(() -> GeometryType.INSTANCE.bind(1, Geometry.schema(), Geometry.createValue(Geometry.schema(), write(multiPoint), 4326)))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("MULTIPOINT");
    }

    @Test
    @DisplayName("Should bind multipolygon with holes as SingleStore polygon WKT")
    @FixFor("DBZ-2880")
    void testBindMultiPolygonWithHoles() throws Exception {
        final var multiPolygon = new MultiPolygon(false, false);
        multiPolygon.addPolygon(polygon(
                new double[][]{ { 0, 0 }, { 4, 0 }, { 4, 4 }, { 0, 4 }, { 0, 0 } },
                new double[][]{ { 1, 1 }, { 1, 2 }, { 2, 2 }, { 2, 1 }, { 1, 1 } }));
        multiPolygon.addPolygon(polygon(
                new double[][]{ { 6, 0 }, { 12, 0 }, { 12, 4 }, { 6, 4 }, { 6, 0 } },
                new double[][]{ { 7, 1 }, { 7, 2 }, { 8, 2 }, { 8, 1 }, { 7, 1 } },
                new double[][]{ { 9, 2 }, { 9, 3 }, { 10, 3 }, { 10, 2 }, { 9, 2 } }));
        assertThat(bind(multiPolygon)).isEqualTo(
                "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1), " +
                        "(6 0, 12 0, 12 4, 6 4, 6 0), (7 1, 7 2, 8 2, 8 1, 7 1), (9 2, 9 3, 10 3, 10 2, 9 2))");
    }

    @Test
    @DisplayName("Should bind a single polygon in multipolygon WKB")
    @FixFor("DBZ-2880")
    void testBindSinglePolygonInMultiPolygon() throws Exception {
        final var multiPolygon = new MultiPolygon(false, false);
        multiPolygon.addPolygon(polygon(new double[][]{ { 0, 0 }, { 2, 0 }, { 2, 2 }, { 0, 0 } }));
        assertThat(bind(multiPolygon)).isEqualTo("POLYGON ((0 0, 2 0, 2 2, 0 0))");
    }

    @Test
    @DisplayName("Should reject empty multipolygon and polygon components")
    @FixFor("DBZ-2880")
    void testRejectEmptyMultiPolygonAndComponents() {
        final var multiPolygon = new MultiPolygon(false, false);
        assertThatThrownBy(() -> bind(multiPolygon))
                .isInstanceOf(ConnectException.class).hasMessageContaining("MULTIPOLYGON EMPTY");
        multiPolygon.addPolygon(new Polygon(false, false));
        assertThatThrownBy(() -> bind(multiPolygon))
                .isInstanceOf(ConnectException.class).hasMessageContaining("POLYGON EMPTY");
    }

    private static Polygon polygon(double[][]... rings) {
        final var polygon = new Polygon(false, false);
        for (var coordinates : rings) {
            final var ring = new LineString(false, false);
            for (var coordinate : coordinates) {
                ring.addPoint(new Point(coordinate[0], coordinate[1]));
            }
            polygon.addRing(ring);
        }
        return polygon;
    }

    private static String bind(MultiPolygon multiPolygon) throws IOException {
        return (String) GeometryType.INSTANCE.bind(1, Geometry.schema(),
                Geometry.createValue(Geometry.schema(), write(multiPolygon), 4326)).get(0).getValue();
    }

    private static byte[] write(mil.nga.wkb.geom.Geometry geometry) throws IOException {
        final ByteWriter writer = new ByteWriter();
        writer.setByteOrder(ByteOrder.LITTLE_ENDIAN);
        WkbGeometryWriter.writeGeometry(writer, geometry);
        return writer.getBytes();
    }
}
