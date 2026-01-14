/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;

import io.debezium.connector.oracle.util.TimestampUtils;
import io.debezium.doc.FixFor;

/**
 * Unit tests for the {@link TimestampUtils} class.
 *
 * @author Chris Cranford
 */
public class TimestampUtilsTest {

    @Test
    @FixFor("dbz#1508")
    public void testNormalDate() {
        final String value = "TO_DATE('2025-01-02 01:02:03', 'YYYY-MM-DD HH24:MI:SS')";
        final Instant expected = LocalDateTime.of(2025, 1, 2, 1, 2, 3, 0).toInstant(ZoneOffset.UTC);
        assertThat(TimestampUtils.convertTimestampNoZoneToInstant(value)).isEqualTo(expected);
    }

    @Test
    @FixFor("dbz#1508")
    public void testZeroYearDate() {
        final String value = "TO_DATE('0000-01-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS')";
        final Instant expected = LocalDateTime.of(0, 1, 2, 0, 0, 0, 0).toInstant(ZoneOffset.UTC);
        assertThat(TimestampUtils.convertTimestampNoZoneToInstant(value)).isEqualTo(expected);
    }

    @Test
    @FixFor("dbz#1508")
    public void testNormalTimestamp() {
        final String value = "TO_TIMESTAMP('2025-01-02 01:02:03.123456789')";
        final Instant expected = LocalDateTime.of(2025, 1, 2, 1, 2, 3, 123456789).toInstant(ZoneOffset.UTC);
        assertThat(TimestampUtils.convertTimestampNoZoneToInstant(value)).isEqualTo(expected);
    }

    @Test
    @FixFor("dbz#1508")
    public void testZeroYearTimestamp() {
        final String value = "TO_TIMESTAMP('0000-01-02 00:00:00.123456789')";
        final Instant expected = LocalDateTime.of(0, 1, 2, 0, 0, 0, 123456789).toInstant(ZoneOffset.UTC);
        assertThat(TimestampUtils.convertTimestampNoZoneToInstant(value)).isEqualTo(expected);
    }
}
