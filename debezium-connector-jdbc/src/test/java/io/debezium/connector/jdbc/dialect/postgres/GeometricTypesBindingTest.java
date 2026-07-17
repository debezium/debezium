/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.data.geometry.Circle;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Line;
import io.debezium.data.geometry.Point;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.spatial.WkbWriter;

/**
 * Unit tests for the PostgreSQL sink binding of the geometric types: {@link GeometryType} native
 * reconstruction (box/lseg/path/polygon) and the standalone {@link CircleType}/{@link LineType} added
 * in dbz#2135, plus {@link PointType} for the built-in point (dbz#2100, case 9).
 *
 * @author Debezium Authors
 */
@Tag("UnitTests")
class GeometricTypesBindingTest {

    /**
     * Builds a shared-{@link Geometry} schema whose propagated source-column type drives the native
     * binding branch, mirroring how {@code column.propagate.source.type} surfaces the type on the sink.
     */
    private static Schema geometrySchemaFor(String sourceType) {
        return new SchemaBuilder(Schema.Type.STRUCT)
                .name(Geometry.LOGICAL_NAME)
                .version(2)
                .optional()
                .parameter("__debezium.source.column.type", sourceType)
                .field(Geometry.WKB_FIELD, Schema.BYTES_SCHEMA)
                .field(Geometry.SRID_FIELD, Schema.OPTIONAL_INT32_SCHEMA)
                .field(Geometry.EXTENSIONS_FIELD, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().build())
                .build();
    }

    @Test
    void boxShouldBindAsNativeText() {
        final Schema schema = geometrySchemaFor("box");
        final Struct value = Geometry.createValue(schema,
                WkbWriter.buildPolygon(List.of(List.of(
                        new double[]{ 1.0, 1.0 }, new double[]{ 1.0, 0.0 }, new double[]{ 0.0, 0.0 },
                        new double[]{ 0.0, 1.0 }, new double[]{ 1.0, 1.0 }))),
                null, Map.of(Geometry.EXTENSION_TYPE_KEY, "box"));

        assertThat(GeometryType.INSTANCE.getQueryBinding(null, schema, value)).isEqualTo("cast(? as box)");
        assertThat(bindValue(GeometryType.INSTANCE, schema, value)).isEqualTo("(1.0,1.0),(0.0,0.0)");
    }

    @Test
    void lsegShouldBindAsNativeText() {
        final Schema schema = geometrySchemaFor("lseg");
        final Struct value = Geometry.createValue(schema,
                WkbWriter.buildLineString(List.of(new double[]{ 0.0, 0.0 }, new double[]{ 0.0, 1.0 })),
                null, Map.of(Geometry.EXTENSION_TYPE_KEY, "lseg"));

        assertThat(GeometryType.INSTANCE.getQueryBinding(null, schema, value)).isEqualTo("cast(? as lseg)");
        assertThat(bindValue(GeometryType.INSTANCE, schema, value)).isEqualTo("[(0.0,0.0),(0.0,1.0)]");
    }

    @Test
    void closedPathShouldBindWithParentheses() {
        final Schema schema = geometrySchemaFor("path");
        final Map<String, String> extensions = new LinkedHashMap<>();
        extensions.put(Geometry.EXTENSION_TYPE_KEY, "path");
        extensions.put(Geometry.EXTENSION_CLOSED_KEY, "true");
        final Struct value = Geometry.createValue(schema,
                WkbWriter.buildLineString(List.of(
                        new double[]{ 0.0, 0.0 }, new double[]{ 0.0, 1.0 }, new double[]{ 0.0, 2.0 })),
                null, extensions);

        assertThat(bindValue(GeometryType.INSTANCE, schema, value)).isEqualTo("((0.0,0.0),(0.0,1.0),(0.0,2.0))");
    }

    @Test
    void openPathShouldBindWithBrackets() {
        final Schema schema = geometrySchemaFor("path");
        final Map<String, String> extensions = new LinkedHashMap<>();
        extensions.put(Geometry.EXTENSION_TYPE_KEY, "path");
        extensions.put(Geometry.EXTENSION_CLOSED_KEY, "false");
        final Struct value = Geometry.createValue(schema,
                WkbWriter.buildLineString(List.of(new double[]{ 0.0, 0.0 }, new double[]{ 0.0, 1.0 })),
                null, extensions);

        assertThat(bindValue(GeometryType.INSTANCE, schema, value)).isEqualTo("[(0.0,0.0),(0.0,1.0)]");
    }

    @Test
    void polygonShouldDropClosingVertex() {
        final Schema schema = geometrySchemaFor("polygon");
        final Struct value = Geometry.createValue(schema,
                WkbWriter.buildPolygon(List.of(List.of(
                        new double[]{ 0.0, 0.0 }, new double[]{ 0.0, 1.0 }, new double[]{ 1.0, 0.0 },
                        new double[]{ 0.0, 0.0 }))),
                null, Map.of(Geometry.EXTENSION_TYPE_KEY, "polygon"));

        assertThat(bindValue(GeometryType.INSTANCE, schema, value)).isEqualTo("((0.0,0.0),(0.0,1.0),(1.0,0.0))");
    }

    @Test
    void multiRingPolygonShouldBeRejected() {
        // A polygon with a hole cannot be reconstructed as a native single-ring PostgreSQL polygon.
        final Schema schema = geometrySchemaFor("polygon");
        final Struct value = Geometry.createValue(schema,
                WkbWriter.buildPolygon(List.of(
                        List.of(new double[]{ 0.0, 0.0 }, new double[]{ 0.0, 3.0 }, new double[]{ 3.0, 3.0 },
                                new double[]{ 3.0, 0.0 }, new double[]{ 0.0, 0.0 }),
                        List.of(new double[]{ 1.0, 1.0 }, new double[]{ 1.0, 2.0 }, new double[]{ 2.0, 2.0 },
                                new double[]{ 2.0, 1.0 }, new double[]{ 1.0, 1.0 }))),
                null, Map.of(Geometry.EXTENSION_TYPE_KEY, "polygon"));

        assertThatThrownBy(() -> GeometryType.INSTANCE.bind(1, schema, value))
                .hasMessageContaining("multi-ring");
    }

    @Test
    void degenerateFallbackShapesShouldReconstructWithoutCrashing() {
        // The source emits these origin-only shapes as the NOT NULL fallback for a null geometric value
        // (PostgresValueConverter#geometryFallback). A box reconstructs from ring vertices 0 and 2, so its
        // fallback must carry a full ring; an lseg needs two points. This guards against the previous
        // one-point fallback that threw IndexOutOfBounds / produced invalid native text (dbz#2135).
        final Schema boxSchema = geometrySchemaFor("box");
        final Struct box = Geometry.createValue(boxSchema,
                WkbWriter.buildPolygon(List.of(List.of(
                        new double[]{ 0, 0 }, new double[]{ 0, 0 }, new double[]{ 0, 0 },
                        new double[]{ 0, 0 }, new double[]{ 0, 0 }))),
                null, Map.of(Geometry.EXTENSION_TYPE_KEY, "box"));
        assertThat(bindValue(GeometryType.INSTANCE, boxSchema, box)).isEqualTo("(0.0,0.0),(0.0,0.0)");

        final Schema lsegSchema = geometrySchemaFor("lseg");
        final Struct lseg = Geometry.createValue(lsegSchema,
                WkbWriter.buildLineString(List.of(new double[]{ 0, 0 }, new double[]{ 0, 0 })),
                null, Map.of(Geometry.EXTENSION_TYPE_KEY, "lseg"));
        assertThat(bindValue(GeometryType.INSTANCE, lsegSchema, lseg)).isEqualTo("[(0.0,0.0),(0.0,0.0)]");
    }

    @Test
    void genuinePostGisGeometryShouldUseWkbFunction() {
        // No propagated native type -> falls back to the PostGIS ST_GeomFromWKB path (two placeholders)
        final Schema schema = Geometry.schema();
        assertThat(GeometryType.INSTANCE.getQueryBinding(null, schema, null)).isEqualTo("public.ST_GeomFromWKB(?, ?)");
    }

    @Test
    void circleShouldBindAsNativeText() {
        final Schema schema = Circle.schema();
        final Struct value = Circle.createValue(schema, 10.0, 4.0, 10.0);

        assertThat(CircleType.INSTANCE.getQueryBinding(null, schema, value)).isEqualTo("cast(? as circle)");
        assertThat(bindValue(CircleType.INSTANCE, schema, value)).isEqualTo("<(10.0,4.0),10.0>");
    }

    @Test
    void lineShouldBindAsNativeText() {
        final Schema schema = Line.schema();
        final Struct value = Line.createValue(schema, -1.0, 0.0, 0.0);

        assertThat(LineType.INSTANCE.getQueryBinding(null, schema, value)).isEqualTo("cast(? as line)");
        assertThat(bindValue(LineType.INSTANCE, schema, value)).isEqualTo("{-1.0,0.0,0.0}");
    }

    @Test
    void pointShouldBindAsNativeText() {
        final Schema schema = Point.builder().build();
        final Struct value = Point.createValue(schema, 1.5, -2.0);

        assertThat(PointType.INSTANCE.getQueryBinding(null, schema, value)).isEqualTo("cast(? as point)");
        assertThat(bindValue(PointType.INSTANCE, schema, value)).isEqualTo("(1.5,-2.0)");
    }

    @Test
    void nullPointShouldBindAsNull() {
        final Schema schema = Point.builder().build();

        assertThat(bindValue(PointType.INSTANCE, schema, null)).isNull();
    }

    private static Object bindValue(JdbcType type, Schema schema, Object value) {
        final List<ValueBindDescriptor> bindings = type.bind(1, schema, value);
        assertThat(bindings).hasSize(1);
        assertThat(bindings.get(0).getIndex()).isEqualTo(1);
        return bindings.get(0).getValue();
    }
}
