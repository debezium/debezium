/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.wal2json;

import org.fest.assertions.Assertions;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.chrono.IsoEra;
import java.time.format.TextStyle;
import java.util.Locale;

public class ISODateTimeFormatTest {
    private static final String BCE_DISPLAY_NAME = IsoEra.BCE.getDisplayName(TextStyle.SHORT, Locale.getDefault());

    @Test
    public void testTimestampToOffsetDateTime() {
        ZoneOffset offset = ZoneOffset.UTC;
        ZoneOffset otherOffset = ZoneOffset.ofHoursMinutes(2, 30);
        Assertions.assertThat(DateTimeFormat.get().timestampToOffsetDateTime("2016-11-04 13:51:30", offset))
                .isEqualTo(OffsetDateTime.of(2016, 11, 4, 13, 51, 30, 0, offset));
        Assertions.assertThat(DateTimeFormat.get().timestampToOffsetDateTime("2016-11-04 13:51:30.123", offset))
                .isEqualTo(OffsetDateTime.of(2016, 11, 4, 13, 51, 30, 123_000_000, offset));
        Assertions.assertThat(DateTimeFormat.get().timestampToOffsetDateTime("2016-11-04 13:51:30.123000", offset))
                .isEqualTo(OffsetDateTime.of(2016, 11, 4, 13, 51, 30, 123_000_000, offset));
        Assertions.assertThat(DateTimeFormat.get().timestampToOffsetDateTime("2016-11-04 13:51:30.123456", offset))
                .isEqualTo(OffsetDateTime.of(2016, 11, 4, 13, 51, 30, 123_456_000, offset));
        Assertions.assertThat(DateTimeFormat.get().timestampToOffsetDateTime("2016-11-04 13:51:30.123456", offset))
                .isEqualTo(OffsetDateTime.of(2016, 11, 4, 13, 51, 30, 123_456_000, offset));
        Assertions.assertThat(DateTimeFormat.get().timestampToOffsetDateTime("0002-12-01 17:00:00 " + BCE_DISPLAY_NAME, offset))
                .isEqualTo(OffsetDateTime.of(-1, 12, 1, 17, 0, 0, 0, offset));
        Assertions.assertThat(DateTimeFormat.get().timestampToOffsetDateTime("20160-11-04 13:51:30.123456", offset))
                .isEqualTo(OffsetDateTime.of(20160, 11, 4, 13, 51, 30, 123_456_000, offset));
        Assertions.assertThat(DateTimeFormat.get().timestampToOffsetDateTime("20160-11-04 13:51:30.123456", otherOffset))
                .isEqualTo(OffsetDateTime.of(20160, 11, 4, 13, 51, 30, 123_456_000, otherOffset));
    }

    @Test
    public void testTimestampWithTimeZoneToOffsetTime() {
        Assertions.assertThat(DateTimeFormat.get().timestampWithTimeZoneToOffsetDateTime("2016-11-04 13:51:30+02"))
                .isEqualTo(OffsetDateTime.of(2016, 11, 4, 13, 51, 30, 0, ZoneOffset.ofHours(2)));
        Assertions.assertThat(DateTimeFormat.get().timestampWithTimeZoneToOffsetDateTime("2016-11-04 13:51:30.123+02"))
                .isEqualTo(OffsetDateTime.of(2016, 11, 4, 13, 51, 30, 123_000_000, ZoneOffset.ofHours(2)));
        Assertions.assertThat(DateTimeFormat.get().timestampWithTimeZoneToOffsetDateTime("2016-11-04 13:51:30.123000+02"))
                .isEqualTo(OffsetDateTime.of(2016, 11, 4, 13, 51, 30, 123_000_000, ZoneOffset.ofHours(2)));
        Assertions.assertThat(DateTimeFormat.get().timestampWithTimeZoneToOffsetDateTime("2016-11-04 13:51:30.123789+02"))
                .isEqualTo(OffsetDateTime.of(2016, 11, 4, 13, 51, 30, 123_789_000, ZoneOffset.ofHours(2)));
        Assertions.assertThat(DateTimeFormat.get().timestampWithTimeZoneToOffsetDateTime("2016-11-04 13:51:30.123789+02:30"))
                .isEqualTo(OffsetDateTime.of(2016, 11, 4, 13, 51, 30, 123_789_000, ZoneOffset.ofHoursMinutes(2, 30)));
        Assertions.assertThat(DateTimeFormat.get().timestampWithTimeZoneToOffsetDateTime("2016-11-04 13:51:30.123789+02:30 " + BCE_DISPLAY_NAME))
                .isEqualTo(OffsetDateTime.of(-2015, 11, 4, 13, 51, 30, 123_789_000, ZoneOffset.ofHoursMinutes(2, 30)));
        Assertions.assertThat(DateTimeFormat.get().timestampWithTimeZoneToOffsetDateTime("20160-11-04 13:51:30.123789+02:30 " + BCE_DISPLAY_NAME))
                .isEqualTo(OffsetDateTime.of(-20159, 11, 4, 13, 51, 30, 123_789_000, ZoneOffset.ofHoursMinutes(2, 30)));
    }

    @Test
    public void testDate() {
        Assertions.assertThat(DateTimeFormat.get().date("2016-11-04")).isEqualTo(LocalDate.of(2016, 11, 4));
        Assertions.assertThat(DateTimeFormat.get().date("2016-11-04 " + BCE_DISPLAY_NAME)).isEqualTo(LocalDate.of(-2015, 11, 4));
        Assertions.assertThat(DateTimeFormat.get().date("20160-11-04")).isEqualTo(LocalDate.of(20160, 11, 4));
        Assertions.assertThat(DateTimeFormat.get().date("20160-11-04 " + BCE_DISPLAY_NAME)).isEqualTo(LocalDate.of(-20159, 11, 4));
        Assertions.assertThat(DateTimeFormat.get().date("12345678-11-04")).isEqualTo(LocalDate.of(12345678, 11, 4));
    }

    @Test
    public void testTime() {
        Assertions.assertThat(DateTimeFormat.get().time("13:51:30")).isEqualTo(LocalTime.of(13,  51, 30));
    }

    @Test
    public void testTimeWithTimeZone() {
        Assertions.assertThat(DateTimeFormat.get().timeWithTimeZone("13:51:30+02")).isEqualTo(OffsetTime.of(11, 51, 30, 0, ZoneOffset.UTC));
    }

    @Test
    public void testSystemTimestampToOffsetDateTime() {
        Assertions.assertThat(DateTimeFormat.get().systemTimestampToOffsetDateTime("2017-10-17 13:51:30Z")).isEqualTo(OffsetDateTime.of(2017, 10, 17, 13, 51, 30, 0, ZoneOffset.UTC));
        Assertions.assertThat(DateTimeFormat.get().systemTimestampToOffsetDateTime("2017-10-17 13:51:30.000Z")).isEqualTo(OffsetDateTime.of(2017, 10, 17, 13, 51, 30, 0, ZoneOffset.UTC));
        Assertions.assertThat(DateTimeFormat.get().systemTimestampToOffsetDateTime("2017-10-17 13:51:30.456Z")).isEqualTo(OffsetDateTime.of(2017, 10, 17, 13, 51, 30, Duration.ofMillis(456).getNano(), ZoneOffset.UTC));
        Assertions.assertThat(DateTimeFormat.get().systemTimestampToOffsetDateTime("2017-10-17 13:51:30.345123Z")).isEqualTo(OffsetDateTime.of(2017, 10, 17, 13, 51, 30, 345_123_000, ZoneOffset.UTC));
        Assertions.assertThat(DateTimeFormat.get().systemTimestampToOffsetDateTime("2018-03-22 12:30:56.824452+05:30")).isEqualTo(OffsetDateTime.of(2018, 3, 22, 12, 30, 56, 824_452_000, ZoneOffset.ofHoursMinutes(5, 30)));
        Assertions.assertThat(DateTimeFormat.get().systemTimestampToOffsetDateTime("2018-03-22 12:30:56.824452+05")).isEqualTo(OffsetDateTime.of(2018, 3, 22, 12, 30, 56, 824_452_000, ZoneOffset.ofHours(5)));
        Assertions.assertThat(DateTimeFormat.get().systemTimestampToOffsetDateTime("20180-03-22 12:30:56.824452+05")).isEqualTo(OffsetDateTime.of(20180, 3, 22, 12, 30, 56, 824_452_000, ZoneOffset.ofHours(5)));
    }

}
