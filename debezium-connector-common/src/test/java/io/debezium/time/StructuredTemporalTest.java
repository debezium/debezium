/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

class StructuredTemporalTest {

    @Test
    void shouldPreserveZeroDateComponentsAsFiniteDate() {
        final Schema schema = StructuredDate.schema();

        final Struct value = StructuredDate.from(schema, 0, 0, 0);

        assertThat(StructuredTemporal.isFinite(value)).isTrue();
        assertThat(value.getString(StructuredTemporal.SPECIAL_VALUE_FIELD)).isNull();
        assertThat(value.getInt32(StructuredTemporal.YEAR_FIELD)).isZero();
        assertThat(value.getInt8(StructuredTemporal.MONTH_FIELD)).isEqualTo((byte) 0);
        assertThat(value.getInt8(StructuredTemporal.DAY_FIELD)).isEqualTo((byte) 0);
    }

    @Test
    void shouldRepresentTemporalInfinityAsSpecialValue() {
        final Schema schema = StructuredTimestamp.schema();

        final Struct positiveInfinity = StructuredTimestamp.positiveInfinity(schema);
        final Struct negativeInfinity = StructuredTimestamp.negativeInfinity(schema);

        assertThat(StructuredTemporal.isPositiveInfinity(positiveInfinity)).isTrue();
        assertThat(StructuredTemporal.isNegativeInfinity(negativeInfinity)).isTrue();
        assertThat(StructuredTemporal.isFinite(positiveInfinity)).isFalse();
        assertThat(positiveInfinity.getInt32(StructuredTemporal.YEAR_FIELD)).isNull();
        assertThat(positiveInfinity.getString(StructuredTemporal.SPECIAL_VALUE_FIELD)).isEqualTo(StructuredTemporal.POSITIVE_INFINITY);
    }

    @Test
    void shouldPreserveZonedTimestampComponents() {
        final Schema schema = StructuredZonedTimestamp.schema();

        final Struct value = StructuredZonedTimestamp.from(
                schema,
                OffsetDateTime.of(294276, 12, 31, 23, 59, 59, 999_999_000, ZoneOffset.ofHours(9)),
                "Asia/Seoul");

        assertThat(value.getInt32(StructuredTemporal.YEAR_FIELD)).isEqualTo(294276);
        assertThat(value.getInt8(StructuredTemporal.MONTH_FIELD)).isEqualTo((byte) 12);
        assertThat(value.getInt8(StructuredTemporal.DAY_FIELD)).isEqualTo((byte) 31);
        assertThat(value.getInt8(StructuredTemporal.HOUR_FIELD)).isEqualTo((byte) 23);
        assertThat(value.getInt8(StructuredTemporal.MINUTE_FIELD)).isEqualTo((byte) 59);
        assertThat(value.getInt8(StructuredTemporal.SECOND_FIELD)).isEqualTo((byte) 59);
        assertThat(value.getInt32(StructuredTemporal.NANOS_FIELD)).isEqualTo(999_999_000);
        assertThat(value.getInt32(StructuredTemporal.OFFSET_SECONDS_FIELD)).isEqualTo(32_400);
        assertThat(value.getString(StructuredTemporal.ZONE_ID_FIELD)).isEqualTo("Asia/Seoul");
    }

    @Test
    void shouldPreserveSignedDurationComponents() {
        final Schema schema = StructuredDuration.schema();

        final Struct value = StructuredDuration.from(schema, -1, -2, 3, -4, -5, -6, -7);

        assertThat(value.getInt32(StructuredTemporal.YEARS_FIELD)).isEqualTo(-1);
        assertThat(value.getInt32(StructuredTemporal.MONTHS_FIELD)).isEqualTo(-2);
        assertThat(value.getInt32(StructuredTemporal.DAYS_FIELD)).isEqualTo(3);
        assertThat(value.getInt32(StructuredTemporal.HOURS_FIELD)).isEqualTo(-4);
        assertThat(value.getInt32(StructuredTemporal.MINUTES_FIELD)).isEqualTo(-5);
        assertThat(value.getInt64(StructuredTemporal.SECONDS_FIELD)).isEqualTo(-6L);
        assertThat(value.getInt32(StructuredTemporal.NANOS_FIELD)).isEqualTo(-7);
    }
}
