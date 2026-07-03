/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spatial;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;

/**
 * Unit tests for {@link WkbWriter} and {@link WkbReader}, verifying that coordinates survive a
 * round-trip through the WKB encoding used for PostgreSQL geometric types.
 *
 * @author Debezium Authors
 */
class WkbWriterReaderTest {

    @Test
    void shouldRoundTripLineString() {
        final List<double[]> points = List.of(new double[]{ 0.0, 0.0 }, new double[]{ 0.0, 1.0 }, new double[]{ 0.0, 2.0 });

        final byte[] wkb = WkbWriter.buildLineString(points);

        // little-endian byte order marker + LINE_STRING type
        assertThat(wkb[0]).isEqualTo((byte) 0x01);
        assertCoordinates(WkbReader.readLineString(wkb), points);
    }

    @Test
    void shouldRoundTripTwoPointLineStringForLseg() {
        final List<double[]> points = List.of(new double[]{ 0.0, 0.0 }, new double[]{ 0.0, 1.0 });

        assertCoordinates(WkbReader.readLineString(WkbWriter.buildLineString(points)), points);
    }

    @Test
    void shouldRoundTripPolygonRing() {
        // A box '(1,1),(0,0)' becomes a closed 5-point ring
        final List<double[]> ring = List.of(
                new double[]{ 1.0, 1.0 },
                new double[]{ 1.0, 0.0 },
                new double[]{ 0.0, 0.0 },
                new double[]{ 0.0, 1.0 },
                new double[]{ 1.0, 1.0 });

        final byte[] wkb = WkbWriter.buildPolygon(List.of(ring));

        assertThat(wkb[0]).isEqualTo((byte) 0x01);
        assertCoordinates(WkbReader.readPolygonRing(wkb), ring);
    }

    @Test
    void shouldRejectMultiRingPolygonWhenReadingRing() {
        // A polygon with a hole: an outer ring and an inner ring. Native PostgreSQL polygons are always
        // single-ring, so reconstructing one from this would silently merge the inner ring into the outer.
        final List<double[]> outer = List.of(
                new double[]{ 0.0, 0.0 }, new double[]{ 0.0, 3.0 }, new double[]{ 3.0, 3.0 },
                new double[]{ 3.0, 0.0 }, new double[]{ 0.0, 0.0 });
        final List<double[]> inner = List.of(
                new double[]{ 1.0, 1.0 }, new double[]{ 1.0, 2.0 }, new double[]{ 2.0, 2.0 },
                new double[]{ 2.0, 1.0 }, new double[]{ 1.0, 1.0 });

        final byte[] wkb = WkbWriter.buildPolygon(List.of(outer, inner));

        assertThatThrownBy(() -> WkbReader.readPolygonRing(wkb))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("multi-ring");
    }

    private static void assertCoordinates(List<double[]> actual, List<double[]> expected) {
        assertThat(actual).hasSameSizeAs(expected);
        for (int i = 0; i < expected.size(); i++) {
            assertThat(actual.get(i)).containsExactly(expected.get(i));
        }
    }
}
