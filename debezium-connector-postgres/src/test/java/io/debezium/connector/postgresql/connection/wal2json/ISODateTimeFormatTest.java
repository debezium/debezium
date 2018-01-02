/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.wal2json;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;

import org.fest.assertions.Assertions;
import org.junit.Test;

public class ISODateTimeFormatTest {

    @Test
    public void testTimestamp() {
        Assertions.assertThat(DateTimeFormat.get().timestamp("2016-11-04 13:51:30")).isEqualTo(1478267490_000_000_000l);
        Assertions.assertThat(DateTimeFormat.get().timestamp("2016-11-04 13:51:30.123")).isEqualTo(1478267490_123_000_000l);
        Assertions.assertThat(DateTimeFormat.get().timestamp("2016-11-04 13:51:30.123000")).isEqualTo(1478267490_123_000_000l);
        Assertions.assertThat(DateTimeFormat.get().timestamp("2016-11-04 13:51:30.123456")).isEqualTo(1478267490_123_456_000l);
    }

    @Test
    public void testTimestampWithTimeZone() {
        Assertions.assertThat(DateTimeFormat.get().timestampWithTimeZone("2016-11-04 13:51:30+02")).isEqualTo(1478260290_000_000_000l);
        Assertions.assertThat(DateTimeFormat.get().timestampWithTimeZone("2016-11-04 13:51:30.123+02")).isEqualTo(1478260290_123_000_000l);
        Assertions.assertThat(DateTimeFormat.get().timestampWithTimeZone("2016-11-04 13:51:30.123000+02")).isEqualTo(1478260290_123_000_000l);
        Assertions.assertThat(DateTimeFormat.get().timestampWithTimeZone("2016-11-04 13:51:30.123789+02")).isEqualTo(1478260290_123_789_000l);
    }

    @Test
    public void testDate() {
        Assertions.assertThat(DateTimeFormat.get().date("2016-11-04")).isEqualTo(LocalDate.of(2016, 11, 4));
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
    public void testSystemTimestamp() {
        Assertions.assertThat(DateTimeFormat.get().systemTimestamp("2017-10-17 13:51:30Z")).isEqualTo(1508248290_000_000_000l);
        Assertions.assertThat(DateTimeFormat.get().systemTimestamp("2017-10-17 13:51:30.000Z")).isEqualTo(1508248290_000_000_000l);
        Assertions.assertThat(DateTimeFormat.get().systemTimestamp("2017-10-17 13:51:30.456Z")).isEqualTo(1508248290_456_000_000l);
        Assertions.assertThat(DateTimeFormat.get().systemTimestamp("2017-10-17 13:51:30.345123Z")).isEqualTo(1508248290_345_123_000l);
    }

}
