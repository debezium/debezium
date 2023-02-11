/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.chrono.IsoEra;
import java.time.format.TextStyle;
import java.util.Locale;

import org.junit.Test;

public class ISODateTimeFormatTest {
    private static final String BCE_DISPLAY_NAME = IsoEra.BCE.getDisplayName(TextStyle.SHORT, Locale.getDefault());

    @Test
    public void testTimestampToInstant() {
        ZoneOffset offset = ZoneOffset.UTC;
        assertThat(DateTimeFormat.get().timestampToInstant("2016-11-04 13:51:30"))
                .isEqualTo(OffsetDateTime.of(2016, 11, 4, 13, 51, 30, 0, offset).toInstant());
        assertThat(DateTimeFormat.get().timestampToInstant("2016-11-04 13:51:30.123"))
                .isEqualTo(OffsetDateTime.of(2016, 11, 4, 13, 51, 30, 123_000_000, offset).toInstant());
        assertThat(DateTimeFormat.get().timestampToInstant("2016-11-04 13:51:30.123000"))
                .isEqualTo(OffsetDateTime.of(2016, 11, 4, 13, 51, 30, 123_000_000, offset).toInstant());
        assertThat(DateTimeFormat.get().timestampToInstant("2016-11-04 13:51:30.123456"))
                .isEqualTo(OffsetDateTime.of(2016, 11, 4, 13, 51, 30, 123_456_000, offset).toInstant());
        assertThat(DateTimeFormat.get().timestampToInstant("2016-11-04 13:51:30.123456"))
                .isEqualTo(OffsetDateTime.of(2016, 11, 4, 13, 51, 30, 123_456_000, offset).toInstant());
        assertThat(DateTimeFormat.get().timestampToInstant("0002-12-01 17:00:00 " + BCE_DISPLAY_NAME))
                .isEqualTo(OffsetDateTime.of(-1, 12, 1, 17, 0, 0, 0, offset).toInstant());
        assertThat(DateTimeFormat.get().timestampToInstant("20160-11-04 13:51:30.123456"))
                .isEqualTo(OffsetDateTime.of(20160, 11, 4, 13, 51, 30, 123_456_000, offset).toInstant());
    }

    @Test
    public void testTimestampWithTimeZoneToOffsetTime() {
        assertThat(DateTimeFormat.get().timestampWithTimeZoneToOffsetDateTime("2016-11-04 13:51:30+02"))
                .isEqualTo(OffsetDateTime.of(2016, 11, 4, 13, 51, 30, 0, ZoneOffset.ofHours(2)));
        assertThat(DateTimeFormat.get().timestampWithTimeZoneToOffsetDateTime("2016-11-04 13:51:30.123+02"))
                .isEqualTo(OffsetDateTime.of(2016, 11, 4, 13, 51, 30, 123_000_000, ZoneOffset.ofHours(2)));
        assertThat(DateTimeFormat.get().timestampWithTimeZoneToOffsetDateTime("2016-11-04 13:51:30.123000+02"))
                .isEqualTo(OffsetDateTime.of(2016, 11, 4, 13, 51, 30, 123_000_000, ZoneOffset.ofHours(2)));
        assertThat(DateTimeFormat.get().timestampWithTimeZoneToOffsetDateTime("2016-11-04 13:51:30.123789+02"))
                .isEqualTo(OffsetDateTime.of(2016, 11, 4, 13, 51, 30, 123_789_000, ZoneOffset.ofHours(2)));
        assertThat(DateTimeFormat.get().timestampWithTimeZoneToOffsetDateTime("2016-11-04 13:51:30.123789+02:30"))
                .isEqualTo(OffsetDateTime.of(2016, 11, 4, 13, 51, 30, 123_789_000, ZoneOffset.ofHoursMinutes(2, 30)));
        assertThat(DateTimeFormat.get().timestampWithTimeZoneToOffsetDateTime("2016-11-04 13:51:30.123789+02:30 " + BCE_DISPLAY_NAME))
                .isEqualTo(OffsetDateTime.of(-2015, 11, 4, 13, 51, 30, 123_789_000, ZoneOffset.ofHoursMinutes(2, 30)));
        assertThat(DateTimeFormat.get().timestampWithTimeZoneToOffsetDateTime("20160-11-04 13:51:30.123789+02:30 " + BCE_DISPLAY_NAME))
                .isEqualTo(OffsetDateTime.of(-20159, 11, 4, 13, 51, 30, 123_789_000, ZoneOffset.ofHoursMinutes(2, 30)));
    }

    @Test
    public void testDate() {
        assertThat(DateTimeFormat.get().date("2016-11-04")).isEqualTo(LocalDate.of(2016, 11, 4));
        assertThat(DateTimeFormat.get().date("2016-11-04 " + BCE_DISPLAY_NAME)).isEqualTo(LocalDate.of(-2015, 11, 4));
        assertThat(DateTimeFormat.get().date("20160-11-04")).isEqualTo(LocalDate.of(20160, 11, 4));
        assertThat(DateTimeFormat.get().date("20160-11-04 " + BCE_DISPLAY_NAME)).isEqualTo(LocalDate.of(-20159, 11, 4));
        assertThat(DateTimeFormat.get().date("12345678-11-04")).isEqualTo(LocalDate.of(12345678, 11, 4));
    }

    @Test
    public void testTime() {
        assertThat(DateTimeFormat.get().time("13:51:30")).isEqualTo(LocalTime.of(13, 51, 30));
    }

    @Test
    public void testTimeWithTimeZone() {
        assertThat(DateTimeFormat.get().timeWithTimeZone("13:51:30+02")).isEqualTo(OffsetTime.of(11, 51, 30, 0, ZoneOffset.UTC));
    }

    @Test
    public void testSystemTimestampToInstant() {
        assertThat(DateTimeFormat.get().systemTimestampToInstant("2017-10-17 13:51:30Z"))
                .isEqualTo(OffsetDateTime.of(2017, 10, 17, 13, 51, 30, 0, ZoneOffset.UTC).toInstant());
        assertThat(DateTimeFormat.get().systemTimestampToInstant("2017-10-17 13:51:30.000Z"))
                .isEqualTo(OffsetDateTime.of(2017, 10, 17, 13, 51, 30, 0, ZoneOffset.UTC).toInstant());
        assertThat(DateTimeFormat.get().systemTimestampToInstant("2017-10-17 13:51:30.456Z"))
                .isEqualTo(OffsetDateTime.of(2017, 10, 17, 13, 51, 30, Duration.ofMillis(456).getNano(), ZoneOffset.UTC).toInstant());
        assertThat(DateTimeFormat.get().systemTimestampToInstant("2017-10-17 13:51:30.345123Z"))
                .isEqualTo(OffsetDateTime.of(2017, 10, 17, 13, 51, 30, 345_123_000, ZoneOffset.UTC).toInstant());
        assertThat(DateTimeFormat.get().systemTimestampToInstant("2018-03-22 12:30:56.824452+05:30"))
                .isEqualTo(OffsetDateTime.of(2018, 3, 22, 12, 30, 56, 824_452_000, ZoneOffset.ofHoursMinutes(5, 30)).toInstant());
        assertThat(DateTimeFormat.get().systemTimestampToInstant("2018-03-22 12:30:56.824452+05"))
                .isEqualTo(OffsetDateTime.of(2018, 3, 22, 12, 30, 56, 824_452_000, ZoneOffset.ofHours(5)).toInstant());
        assertThat(DateTimeFormat.get().systemTimestampToInstant("20180-03-22 12:30:56.824452+05"))
                .isEqualTo(OffsetDateTime.of(20180, 3, 22, 12, 30, 56, 824_452_000, ZoneOffset.ofHours(5)).toInstant());
    }
}
