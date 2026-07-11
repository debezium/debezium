/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import io.debezium.data.geometry.Geometry;
import io.debezium.doc.FixFor;
import io.debezium.spatial.WkbReader;

/**
 * Verifies the non-null fallback {@link PostgresValueConverter#geometryFallback} emits for a NOT NULL
 * geometric column that receives a null with no default. When {@code column.propagate.source.type} is
 * unset the sink binds these through PostGIS {@code ST_GeomFromWKB}, which enforces the OGC
 * minimum-vertex rules, so each fallback ring/line must be a genuinely valid shape rather than a
 * repeated origin point (see dbz#2135 review).
 *
 * @author Debezium Authors
 */
class GeometryFallbackTest {

    @Test
    @FixFor("debezium/dbz#2135")
    void boxFallbackIsAValidClosedRing() {
        // A box is rebuilt on the sink from ring vertices 0 and 2, so the ring must have both and
        // ST_GeomFromWKB requires at least four points forming a valid ring.
        assertValidRing(readRing("box"));
    }

    @Test
    @FixFor("debezium/dbz#2135")
    void polygonFallbackIsAValidClosedRing() {
        assertValidRing(readRing("polygon"));
    }

    @Test
    @FixFor("debezium/dbz#2135")
    void lsegFallbackIsATwoDistinctPointLine() {
        assertValidLine(readLine("lseg"));
    }

    @Test
    @FixFor("debezium/dbz#2135")
    void pathFallbackIsATwoDistinctPointLine() {
        assertValidLine(readLine("path"));
    }

    private static List<double[]> readRing(String type) {
        return WkbReader.readPolygonRing(fallbackWkb(type));
    }

    private static List<double[]> readLine(String type) {
        return WkbReader.readLineString(fallbackWkb(type));
    }

    private static byte[] fallbackWkb(String type) {
        final Struct fallback = PostgresValueConverter.geometryFallback(Geometry.schema(), type);
        return fallback.getBytes(Geometry.WKB_FIELD);
    }

    /**
     * A ring PostGIS accepts: closed (first == last) with at least four points and at least three
     * distinct vertices.
     */
    private static void assertValidRing(List<double[]> ring) {
        assertThat(ring.size()).as("ring vertex count").isGreaterThanOrEqualTo(4);
        assertThat(ring.get(0)).as("ring is closed").containsExactly(ring.get(ring.size() - 1));
        assertThat(distinctCount(ring)).as("distinct ring vertices").isGreaterThanOrEqualTo(3);
    }

    /**
     * A line PostGIS accepts: at least two points and at least two distinct ones.
     */
    private static void assertValidLine(List<double[]> line) {
        assertThat(line.size()).as("line point count").isGreaterThanOrEqualTo(2);
        assertThat(distinctCount(line)).as("distinct line points").isGreaterThanOrEqualTo(2);
    }

    private static long distinctCount(List<double[]> points) {
        return points.stream().map(p -> p[0] + "," + p[1]).distinct().count();
    }
}
