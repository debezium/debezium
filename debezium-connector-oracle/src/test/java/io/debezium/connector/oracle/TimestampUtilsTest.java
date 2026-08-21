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

    @Test
    @FixFor("debezium/dbz#1286")
    public void testEraSuffixedDate() {
        // 2018 BC (Oracle year -2018) is ISO proleptic year -2017
        final String bc = "TO_DATE('2018-03-27 12:34:56 BC', 'YYYY-MM-DD HH24:MI:SS AD')";
        assertThat(TimestampUtils.convertTimestampNoZoneToInstant(bc))
                .isEqualTo(LocalDateTime.of(-2017, 3, 27, 12, 34, 56).toInstant(ZoneOffset.UTC));

        final String ad = "TO_DATE('2018-03-27 12:34:56 AD', 'YYYY-MM-DD HH24:MI:SS AD')";
        assertThat(TimestampUtils.convertTimestampNoZoneToInstant(ad))
                .isEqualTo(LocalDateTime.of(2018, 3, 27, 12, 34, 56).toInstant(ZoneOffset.UTC));
    }

    @Test
    @FixFor("debezium/dbz#1286")
    public void testEraSuffixedTimestamp() {
        final String bc = "TO_TIMESTAMP('2018-03-27 12:34:56.007890000 BC')";
        assertThat(TimestampUtils.convertTimestampNoZoneToInstant(bc))
                .isEqualTo(LocalDateTime.of(-2017, 3, 27, 12, 34, 56, 7_890_000).toInstant(ZoneOffset.UTC));

        final String ad = "TO_TIMESTAMP('2018-03-27 12:34:56.007890000 AD')";
        assertThat(TimestampUtils.convertTimestampNoZoneToInstant(ad))
                .isEqualTo(LocalDateTime.of(2018, 3, 27, 12, 34, 56, 7_890_000).toInstant(ZoneOffset.UTC));

        // 1 BC (Oracle year -1) is ISO proleptic year 0
        final String oneBc = "TO_TIMESTAMP('0001-12-31 23:59:59 BC')";
        assertThat(TimestampUtils.convertTimestampNoZoneToInstant(oneBc))
                .isEqualTo(LocalDateTime.of(0, 12, 31, 23, 59, 59).toInstant(ZoneOffset.UTC));
    }

    @Test
    @FixFor("debezium/dbz#1286")
    public void testEraSuffixedSqlCompliantFunctionCall() {
        assertThat(TimestampUtils.toSqlCompliantFunctionCall("TO_TIMESTAMP('2018-03-27 12:34:56.00789 BC')"))
                .isEqualTo("TO_TIMESTAMP('2018-03-27 12:34:56.00789 BC', 'YYYY-MM-DD HH24:MI:SS.FF AD')");
        assertThat(TimestampUtils.toSqlCompliantFunctionCall("TO_TIMESTAMP('2018-03-27 12:34:56.00789 AD')"))
                .isEqualTo("TO_TIMESTAMP('2018-03-27 12:34:56.00789 AD', 'YYYY-MM-DD HH24:MI:SS.FF AD')");
        assertThat(TimestampUtils.toSqlCompliantFunctionCall("TO_TIMESTAMP('2018-03-27 12:34:56.00789')"))
                .isEqualTo("TO_TIMESTAMP('2018-03-27 12:34:56.00789', 'YYYY-MM-DD HH24:MI:SS.FF')");
    }
}
