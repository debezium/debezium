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
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
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
    @DisplayName("Should preserve null geometry binding with repair enabled")
    @FixFor("debezium/dbz#2909")
    void testBindNullGeometryWithRepair() {
        assertThat(repairType().bind(1, Geometry.schema(), null).get(0).getValue()).isNull();
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

    @Test
    @DisplayName("Should preserve a narrow notch when rounding keeps its edges distinct")
    @FixFor("debezium/dbz#2909")
    void testRepairPrecisionCollapsedNotch() throws Exception {
        final var polygon = polygon(new double[][]{
                { 127.0, 37.0 }, { 127.1, 37.0 }, { 127.1, 37.1 },
                { 127.050000009, 37.1 }, { 127.050000009, 37.05 },
                { 127.050000001, 37.05 }, { 127.050000001, 37.1 },
                { 127.0, 37.1 }, { 127.0, 37.0 }
        });
        final byte[] wkb = write(polygon);
        final Object value = Geometry.createValue(Geometry.schema(), wkb, 4326);
        assertThat(GeometryType.INSTANCE.bind(1, Geometry.schema(), value).get(0).getValue())
                .isEqualTo("POLYGON ((127 37, 127.1 37, 127.1 37.1, 127.050000009 37.1, 127.050000009 37.05, "
                        + "127.050000001 37.05, 127.050000001 37.1, 127 37.1, 127 37))");
        assertThat(repairType().bind(1, Geometry.schema(), value).get(0).getValue())
                .isEqualTo("POLYGON ((127 37, 127.1 37, 127.1 37.1, 127.05000001 37.1, 127.05000001 37.05, "
                        + "127.05 37.05, 127.05 37.1, 127 37.1, 127 37))");
    }

    @Test
    @DisplayName("Should repair a backtracking notch that collapses after rounding")
    @FixFor("debezium/dbz#2909")
    void testRepairRoundedCollapsedNotch() {
        final var polygon = polygon(new double[][]{
                { 127, 37 }, { 127.1, 37 }, { 127.1, 37.1 },
                { 127.050000004, 37.1 }, { 127.050000004, 37.05 },
                { 127.050000001, 37.05 }, { 127.050000001, 37.1 },
                { 127, 37.1 }, { 127, 37 }
        });
        assertThat(repairType().toWkt(polygon))
                .isEqualTo("POLYGON ((127 37, 127 37.1, 127.05 37.1, 127.1 37.1, 127.1 37, 127 37))");
    }

    @Test
    @DisplayName("Should reject a polygon ring that collapses after precision rounding")
    @FixFor("debezium/dbz#2909")
    void testRejectCollapsedRing() throws Exception {
        final var polygon = polygon(new double[][]{
                { 127.0, 37.0 }, { 127.000000004, 37.0 }, { 127.000000004, 37.000000004 }, { 127.0, 37.0 }
        });
        final Object value = Geometry.createValue(Geometry.schema(), write(polygon), 4326);
        assertThatThrownBy(() -> repairType().bind(1, Geometry.schema(), value))
                .isInstanceOf(ConnectException.class).hasMessageContaining("no valid polygonal area");
    }

    @Test
    @DisplayName("Should preserve two polygons and their holes when repair is enabled")
    @FixFor("debezium/dbz#2909")
    void testRepairPreservesMultiPolygonHoles() throws Exception {
        final var multiPolygon = new MultiPolygon(false, false);
        multiPolygon.addPolygon(polygon(
                new double[][]{ { 0, 0 }, { 4, 0 }, { 4, 4 }, { 0, 4 }, { 0, 0 } },
                new double[][]{ { 1, 1 }, { 1, 2 }, { 2, 2 }, { 2, 1 }, { 1, 1 } }));
        multiPolygon.addPolygon(polygon(
                new double[][]{ { 6, 0 }, { 12, 0 }, { 12, 4 }, { 6, 4 }, { 6, 0 } },
                new double[][]{ { 7, 1 }, { 7, 2 }, { 8, 2 }, { 8, 1 }, { 7, 1 } },
                new double[][]{ { 9, 2 }, { 9, 3 }, { 10, 3 }, { 10, 2 }, { 9, 2 } }));
        final Object value = Geometry.createValue(Geometry.schema(), write(multiPolygon), 4326);
        assertThat(repairType().bind(1, Geometry.schema(), value).get(0).getValue())
                .isEqualTo(bind(multiPolygon));
    }

    @Test
    @DisplayName("Should reject coordinates outside longitude and latitude bounds")
    @FixFor("debezium/dbz#2909")
    void testRejectOutOfBoundsCoordinates() throws Exception {
        final var polygon = polygon(new double[][]{ { 0, 0 }, { 2, 0 }, { 2, 91 }, { 0, 0 } });
        final Object value = Geometry.createValue(Geometry.schema(), write(polygon), 4326);
        assertThatThrownBy(() -> repairType().bind(1, Geometry.schema(), value))
                .isInstanceOf(ConnectException.class).hasMessageContaining("longitude and latitude");
    }

    @Test
    @DisplayName("Should preserve existing SRID-independent WKT conversion")
    @FixFor("debezium/dbz#2909")
    void testSridDoesNotChangeWkt() throws Exception {
        final var polygon = polygon(new double[][]{ { 0, 0 }, { 2, 0 }, { 2, 2 }, { 0, 0 } });
        final byte[] wkb = write(polygon);
        final Object unspecified = Geometry.createValue(Geometry.schema(), wkb, 0);
        final Object other = Geometry.createValue(Geometry.schema(), wkb, 3857);
        assertThat(repairType().bind(1, Geometry.schema(), other).get(0).getValue())
                .isEqualTo(repairType().bind(1, Geometry.schema(), unspecified).get(0).getValue());
    }

    @Test
    @DisplayName("Should repair a backtracking spike across the ring closure")
    @FixFor("debezium/dbz#2909")
    void testRepairSpikeAtRingClosure() throws Exception {
        final var polygon = polygon(new double[][]{
                { 0, 0 }, { 0, 2 }, { 2, 2 }, { 2, 0 }, { 0, 0 }, { 0, 0.5 }, { 0, 0 }
        });
        final Object value = Geometry.createValue(Geometry.schema(), write(polygon), 4326);
        assertThat(repairType().bind(1, Geometry.schema(), value).get(0).getValue())
                .isEqualTo("POLYGON ((0 0, 0 0.5, 0 2, 2 2, 2 0, 0 0))");
    }

    @Test
    @DisplayName("Should preserve WKT ring orientation for valid polygons")
    @FixFor("debezium/dbz#2909")
    void testPreserveRingOrientation() throws Exception {
        final var polygon = polygon(
                new double[][]{ { 0, 0 }, { 0, 4 }, { 4, 4 }, { 4, 0 }, { 0, 0 } },
                new double[][]{ { 1, 1 }, { 2, 1 }, { 2, 2 }, { 1, 2 }, { 1, 1 } });
        final Object value = Geometry.createValue(Geometry.schema(), write(polygon), 4326);
        assertThat(repairType().bind(1, Geometry.schema(), value).get(0).getValue())
                .isEqualTo("POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))");
    }

    @Test
    @DisplayName("Should retain both polygon regions created by a self-intersection")
    @FixFor("debezium/dbz#2909")
    void testRepairSelfIntersectionIntoTwoRegions() {
        final var polygon = polygon(new double[][]{ { 0, 0 }, { 2, 2 }, { 0, 2 }, { 2, 0 }, { 0, 0 } });
        final String wkt = repairType().toWkt(polygon);
        assertThat(wkt).startsWith("POLYGON (").contains("1 1");
        assertThat(wkt.split("\\), \\(")).hasSize(2);
        assertThat(wkt).isEqualTo(repairType().toWkt(polygon));
        assertThat(polygon.getRings().get(0).getPoints().get(1).getX()).isEqualTo(2);
    }

    @Test
    @DisplayName("Should repair a hole crossing its shell")
    @FixFor("debezium/dbz#2909")
    void testRepairHoleCrossingShell() throws ParseException {
        final var polygon = polygon(
                new double[][]{ { 0, 0 }, { 4, 0 }, { 4, 4 }, { 0, 4 }, { 0, 0 } },
                new double[][]{ { 3, 1 }, { 5, 1 }, { 5, 3 }, { 3, 3 }, { 3, 1 } });
        final var repaired = new WKTReader().read(repairType().toWkt(polygon));
        assertThat(repaired.isValid()).isTrue();
        assertThat(repaired.getArea()).isEqualTo(14);
    }

    @Test
    @DisplayName("Should union overlapping polygons during whole-geometry repair")
    @FixFor("debezium/dbz#2909")
    void testRepairOverlappingMultiPolygon() throws ParseException {
        final var multiPolygon = new MultiPolygon(false, false);
        multiPolygon.addPolygon(polygon(new double[][]{ { 0, 0 }, { 2, 0 }, { 2, 2 }, { 0, 2 }, { 0, 0 } }));
        multiPolygon.addPolygon(polygon(new double[][]{ { 1, 1 }, { 3, 1 }, { 3, 3 }, { 1, 3 }, { 1, 1 } }));
        final var repaired = new WKTReader().read(repairType().toWkt(multiPolygon));
        assertThat(repaired.isValid()).isTrue();
        assertThat(repaired.getArea()).isEqualTo(7);
    }

    @Test
    @DisplayName("Should retain holes in separate polygon regions while repairing one region")
    @FixFor("debezium/dbz#2909")
    void testRepairMultiPolygonRetainsSeparateHoles() {
        final var multiPolygon = new MultiPolygon(false, false);
        multiPolygon.addPolygon(polygon(
                new double[][]{
                        { 0, 0 }, { 4, 0 }, { 4, 4 }, { 2.000000009, 4 }, { 2.000000009, 2 },
                        { 2.000000001, 2 }, { 2.000000001, 4 }, { 0, 4 }, { 0, 0 }
                },
                new double[][]{ { 1, 1 }, { 1, 1.5 }, { 1.5, 1.5 }, { 1.5, 1 }, { 1, 1 } }));
        multiPolygon.addPolygon(polygon(
                new double[][]{ { 6, 0 }, { 10, 0 }, { 10, 4 }, { 6, 4 }, { 6, 0 } },
                new double[][]{ { 7, 1 }, { 7, 1.5 }, { 7.5, 1.5 }, { 7.5, 1 }, { 7, 1 } }));
        final String wkt = repairType().toWkt(multiPolygon);
        assertThat(wkt.split("\\), \\(")).hasSize(4);
        assertThat(wkt).contains("1.5 1.5", "7.5 1.5");
    }

    @Test
    @DisplayName("Should round new intersection coordinates to eight decimals")
    @FixFor("debezium/dbz#2909")
    void testRepairRoundsGeneratedVertices() throws ParseException {
        final var polygon = polygon(new double[][]{ { 0, 0 }, { 2, 2 }, { 0, 1 }, { 2, 0 }, { 0, 0 } });
        final String wkt = repairType().toWkt(polygon);
        assertThat(wkt).contains("0.66666667");
        assertThat(wkt).doesNotContain("0.666666666");
        final var decimals = Pattern.compile("-?\\d+\\.\\d+").matcher(wkt);
        while (decimals.find()) {
            assertThat(decimals.group().split("\\.")[1].length()).isLessThanOrEqualTo(8);
        }
        final String body = wkt.substring("POLYGON (".length(), wkt.length() - 1);
        for (String ring : body.split("\\), \\(")) {
            final String coordinates = ring.replace("(", "").replace(")", "");
            assertThat(new WKTReader().read("POLYGON ((" + coordinates + "))").isValid()).isTrue();
        }
    }

    @Test
    @DisplayName("Should round positive and negative coordinates to the nearest eight decimals")
    @FixFor("debezium/dbz#2909")
    void testRepairUsesHalfUpRounding() {
        final var polygon = polygon(new double[][]{
                { -1.123456785, 0 }, { 1.123456785, 0 }, { 0, 1 }, { -1.123456785, 0 }
        });
        assertThat(repairType().toWkt(polygon))
                .isEqualTo("POLYGON ((-1.12345679 0, 1.12345679 0, 0 1, -1.12345679 0))");
    }

    @Test
    @DisplayName("Should let GeometryFixer remove a hole that collapses after rounding")
    @FixFor("debezium/dbz#2909")
    void testRepairCollapsedHole() {
        final var polygon = polygon(
                new double[][]{ { 0, 0 }, { 2, 0 }, { 2, 2 }, { 0, 2 }, { 0, 0 } },
                new double[][]{ { 1, 1 }, { 1.000000004, 1 }, { 1.000000004, 1.000000004 }, { 1, 1 } });
        assertThat(repairType().toWkt(polygon))
                .isEqualTo("POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))");
    }

    @Test
    @DisplayName("Should let GeometryFixer remove a collapsed multipolygon component")
    @FixFor("debezium/dbz#2909")
    void testRepairCollapsedPolygonComponent() {
        final var multiPolygon = new MultiPolygon(false, false);
        multiPolygon.addPolygon(polygon(new double[][]{ { 0, 0 }, { 2, 0 }, { 2, 2 }, { 0, 2 }, { 0, 0 } }));
        multiPolygon.addPolygon(polygon(new double[][]{
                { 3, 3 }, { 3.000000004, 3 }, { 3.000000004, 3.000000004 }, { 3, 3 }
        }));
        assertThat(repairType().toWkt(multiPolygon))
                .isEqualTo("POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))");
    }

    @Test
    @DisplayName("Should reject empty polygons and non-finite coordinates with repair enabled")
    @FixFor("debezium/dbz#2909")
    void testRejectInvalidRepairInputs() {
        assertThatThrownBy(() -> repairType().toWkt(new Polygon(false, false)))
                .isInstanceOf(ConnectException.class).hasMessageContaining("POLYGON EMPTY");
        final var polygon = polygon(new double[][]{ { 0, 0 }, { Double.NaN, 0 }, { 0, 1 }, { 0, 0 } });
        assertThatThrownBy(() -> repairType().toWkt(polygon))
                .isInstanceOf(ConnectException.class).hasMessageContaining("NaN");
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

    private static GeometryType repairType() {
        final GeometryType type = new GeometryType();
        type.configure(new JdbcSinkConnectorConfig(Map.of(JdbcSinkConnectorConfig.SINGLESTORE_GEOMETRY_REPAIR_ENABLED, "true")), null);
        return type;
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
