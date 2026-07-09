/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.data.VerifyRecord;

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
    void shouldPreserveZonedTimeBoundaryHourAndRawOffset() {
        final Schema schema = StructuredZonedTime.schema();

        // OffsetTime/LocalTime cannot hold hour 24; the raw factory must accept it and keep the offset as-is.
        final Struct value = StructuredZonedTime.from(schema, 24, 0, 0, 0, 5 * 3600 + 30 * 60, -1);

        assertThat(value.getInt8(StructuredTemporal.HOUR_FIELD)).isEqualTo((byte) 24);
        assertThat(value.getInt8(StructuredTemporal.MINUTE_FIELD)).isEqualTo((byte) 0);
        assertThat(value.getInt8(StructuredTemporal.SECOND_FIELD)).isEqualTo((byte) 0);
        assertThat(value.getInt32(StructuredTemporal.NANOS_FIELD)).isZero();
        assertThat(value.getInt32(StructuredTemporal.OFFSET_SECONDS_FIELD)).isEqualTo(19_800);
        assertThat(StructuredTemporal.isFinite(value)).isTrue();
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

    @Test
    void shouldPreservePrecisionAsValueComponent() {
        final Schema timestampSchema = StructuredTimestamp.builder().build();
        final Struct timestamp = StructuredTimestamp.from(timestampSchema, 2026, 6, 20, 12, 13, 14, 123_000_000, 3);

        assertThat(timestampSchema.parameters()).isNullOrEmpty();
        assertThat(timestamp.getInt32(StructuredTemporal.PRECISION_FIELD)).isEqualTo(3);

        final Schema durationSchema = StructuredDuration.builder().build();
        final Struct duration = StructuredDuration.from(durationSchema, 0, 0, 0, 1, 2, 3, 456_000_000, 6);

        assertThat(durationSchema.parameters()).isNullOrEmpty();
        assertThat(duration.getInt32(StructuredTemporal.PRECISION_FIELD)).isEqualTo(6);
        assertThat(StructuredTimestamp.schema().parameters()).isNullOrEmpty();
    }

    @Test
    void shouldSerializeRepeatedStructuredTimestampSchemasWithDifferentPrecisionValues() {
        final Schema timestamp3Schema = StructuredTimestamp.builder().build();
        final Schema timestamp6Schema = StructuredTimestamp.builder().build();
        final Schema valueSchema = SchemaBuilder.struct()
                .name("server.schema.table.Value")
                .field("ts3", timestamp3Schema)
                .field("ts6", timestamp6Schema)
                .build();
        final Struct value = new Struct(valueSchema)
                .put("ts3", StructuredTimestamp.from(timestamp3Schema, 2026, 6, 20, 12, 13, 14, 123_000_000, 3))
                .put("ts6", StructuredTimestamp.from(timestamp6Schema, 2026, 6, 20, 12, 13, 14, 123_456_000, 6));
        final SourceRecord record = new SourceRecord(Collections.emptyMap(), Collections.emptyMap(), "server.schema.table", null, null, valueSchema, value);

        VerifyRecord.isValid(record);
    }
}
