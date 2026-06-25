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

import mil.nga.wkb.geom.MultiPoint;
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

    private static byte[] write(mil.nga.wkb.geom.Geometry geometry) throws IOException {
        final ByteWriter writer = new ByteWriter();
        writer.setByteOrder(ByteOrder.LITTLE_ENDIAN);
        WkbGeometryWriter.writeGeometry(writer, geometry);
        return writer.getBytes();
    }
}
