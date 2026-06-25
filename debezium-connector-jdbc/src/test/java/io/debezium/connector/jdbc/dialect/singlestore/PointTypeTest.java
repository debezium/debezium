/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.singlestore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Base64;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;

/**
 * Unit tests for the SingleStore {@link PointType} handler.
 */
@Tag("UnitTests")
class PointTypeTest {

    private static final byte[] POINT_WKB = Base64.getDecoder().decode("AQEAAAAAAAAAAADwPwAAAAAAAPA/");

    private static final byte[] POLYGON_WKB = Base64.getDecoder().decode(
            "AQMAAAABAAAABQAAAAAAAAAAAAAAAAAAAAAAFEAAAAAAAAAAQAAAAAAAABRAAAAAAAAAAEAAAAAAAAAcQAAAAAAAAAAAAAAAAAAAHEAAAAAAAAAAAAAAAAAAABRA");

    @Test
    @DisplayName("Should register for Debezium point logical name")
    void testRegistrationKeys() {
        assertThat(PointType.INSTANCE.getRegistrationKeys()).containsExactly(Point.LOGICAL_NAME);
    }

    @Test
    @DisplayName("Should return geographypoint as type name for SingleStore")
    void testTypeName() {
        assertThat(PointType.INSTANCE.getTypeName(pointSchema(), false)).isEqualTo("geographypoint");
    }

    @Test
    @DisplayName("Should bind point values as WKT")
    void testBindPointAsWkt() {
        final Schema schema = pointSchema();

        assertThat(PointType.INSTANCE.bind(1, schema, Point.createValue(schema, POINT_WKB, 4326)))
                .singleElement()
                .satisfies(binding -> {
                    assertThat(binding.getIndex()).isEqualTo(1);
                    assertThat(binding.getValue()).isEqualTo("POINT (1 1)");
                });
    }

    @Test
    @DisplayName("Should reject non-point WKB values")
    void testShouldRejectNonPointValues() {
        final Schema schema = pointSchema();

        assertThatThrownBy(() -> PointType.INSTANCE.bind(1, schema, Geometry.createValue(schema, POLYGON_WKB, 4326)))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("POLYGON");
    }

    private static Schema pointSchema() {
        return Point.builder().build();
    }
}
