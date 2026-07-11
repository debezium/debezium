/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data.geometry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link Circle} and {@link Line} semantic types and the {@link Geometry}
 * extensions field added for dbz#2135.
 *
 * @author Debezium Authors
 */
class CircleLineTest {

    @Test
    void circleShouldCarryCenterPointAndRadius() {
        final Schema schema = Circle.schema();
        final Struct value = Circle.createValue(schema, 10.0, 4.0, 10.0);

        final Struct center = value.getStruct(Circle.CENTER_FIELD);
        assertThat(center.getFloat64(Point.X_FIELD)).isEqualTo(10.0);
        assertThat(center.getFloat64(Point.Y_FIELD)).isEqualTo(4.0);
        assertThat(center.getBytes(Point.WKB_FIELD)).isNotNull(); // center is a real OGC point
        assertThat(value.getFloat64(Circle.RADIUS_FIELD)).isEqualTo(10.0);
        assertThat(schema.name()).isEqualTo("io.debezium.data.geometry.Circle");
    }

    @Test
    void lineShouldCarryCoefficients() {
        final Schema schema = Line.schema();
        final Struct value = Line.createValue(schema, -1.0, 0.0, 0.0);

        assertThat(value.getFloat64(Line.A_FIELD)).isEqualTo(-1.0);
        assertThat(value.getFloat64(Line.B_FIELD)).isEqualTo(0.0);
        assertThat(value.getFloat64(Line.C_FIELD)).isEqualTo(0.0);
        assertThat(schema.name()).isEqualTo("io.debezium.data.geometry.Line");
    }

    @Test
    void geometrySchemaShouldExposeOptionalExtensions() {
        final Schema schema = Geometry.schema();

        assertThat(schema.version()).isEqualTo(2);
        assertThat(schema.field(Geometry.EXTENSIONS_FIELD)).isNotNull();
        assertThat(schema.field(Geometry.EXTENSIONS_FIELD).schema().isOptional()).isTrue();
    }

    @Test
    void geometryCreateValueShouldStoreExtensionsWhenPresent() {
        final Schema schema = Geometry.schema();
        final byte[] wkb = new byte[]{ 0x01 };

        final Struct withExtensions = Geometry.createValue(schema, wkb, null,
                Map.of(Geometry.EXTENSION_TYPE_KEY, "box"));
        assertThat(withExtensions.getMap(Geometry.EXTENSIONS_FIELD)).containsEntry(Geometry.EXTENSION_TYPE_KEY, "box");

        final Struct withoutExtensions = Geometry.createValue(schema, wkb, null, Map.of());
        assertThat(withoutExtensions.getMap(Geometry.EXTENSIONS_FIELD)).isNull();
    }

    @Test
    void geographyShouldRejectExtensions() {
        // Geography (v1) has no extensions field, so a non-empty extensions map cannot be represented.
        final Schema schema = Geography.schema();
        final byte[] wkb = new byte[]{ 0x01 };

        assertThatThrownBy(() -> Geography.createValue(schema, wkb, null, Map.of(Geometry.EXTENSION_TYPE_KEY, "box")))
                .isInstanceOf(IllegalArgumentException.class);

        // A null or empty map is fine and delegates to the plain three-argument form.
        assertThat(Geography.createValue(schema, wkb, null, Map.of()).getBytes(Geometry.WKB_FIELD)).isEqualTo(wkb);
    }
}
