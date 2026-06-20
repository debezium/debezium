/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAdjuster;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Structured timestamp-with-zone semantic type that preserves calendar components, clock components, and zone metadata.
 */
public final class StructuredZonedTimestamp {

    public static final String SCHEMA_NAME = "io.debezium.time.StructuredZonedTimestamp";

    public static SchemaBuilder builder() {
        return SchemaBuilder.struct()
                .name(SCHEMA_NAME)
                .version(1)
                .field(StructuredTemporal.YEAR_FIELD, StructuredTemporal.optionalInt32())
                .field(StructuredTemporal.MONTH_FIELD, StructuredTemporal.optionalInt8())
                .field(StructuredTemporal.DAY_FIELD, StructuredTemporal.optionalInt8())
                .field(StructuredTemporal.HOUR_FIELD, StructuredTemporal.optionalInt8())
                .field(StructuredTemporal.MINUTE_FIELD, StructuredTemporal.optionalInt8())
                .field(StructuredTemporal.SECOND_FIELD, StructuredTemporal.optionalInt8())
                .field(StructuredTemporal.NANOS_FIELD, StructuredTemporal.optionalInt32())
                .field(StructuredTemporal.OFFSET_SECONDS_FIELD, StructuredTemporal.optionalInt32())
                .field(StructuredTemporal.ZONE_ID_FIELD, StructuredTemporal.optionalString())
                .field(StructuredTemporal.SPECIAL_VALUE_FIELD, StructuredTemporal.optionalString())
                .field(StructuredTemporal.PRECISION_FIELD, StructuredTemporal.optionalInt32());
    }

    public static Schema schema() {
        return builder().build();
    }

    public static Struct from(OffsetDateTime value) {
        return from(schema(), value, null);
    }

    public static Struct from(Schema schema, OffsetDateTime value, String zoneId) {
        return from(schema, value, zoneId, -1);
    }

    public static Struct from(Schema schema, OffsetDateTime value, String zoneId, int precision) {
        return from(schema, value.getYear(), value.getMonthValue(), value.getDayOfMonth(), value.getHour(), value.getMinute(), value.getSecond(), value.getNano(),
                value.getOffset().getTotalSeconds(), zoneId, precision);
    }

    public static Struct from(Schema schema, int year, int month, int day, int hour, int minute, int second, int nanos, int offsetSeconds, String zoneId) {
        return from(schema, year, month, day, hour, minute, second, nanos, offsetSeconds, zoneId, -1);
    }

    public static Struct from(Schema schema, int year, int month, int day, int hour, int minute, int second, int nanos, int offsetSeconds, String zoneId,
                              int precision) {
        final Struct struct = new Struct(schema)
                .put(StructuredTemporal.YEAR_FIELD, year)
                .put(StructuredTemporal.MONTH_FIELD, (byte) month)
                .put(StructuredTemporal.DAY_FIELD, (byte) day)
                .put(StructuredTemporal.HOUR_FIELD, (byte) hour)
                .put(StructuredTemporal.MINUTE_FIELD, (byte) minute)
                .put(StructuredTemporal.SECOND_FIELD, (byte) second)
                .put(StructuredTemporal.NANOS_FIELD, nanos)
                .put(StructuredTemporal.OFFSET_SECONDS_FIELD, offsetSeconds);
        if (zoneId != null) {
            struct.put(StructuredTemporal.ZONE_ID_FIELD, zoneId);
        }
        return StructuredTemporal.withPrecision(struct, precision);
    }

    public static Struct toStructuredZonedTimestamp(Schema schema, Object value, ZoneOffset defaultOffset, TemporalAdjuster adjuster) {
        return toStructuredZonedTimestamp(schema, value, defaultOffset, adjuster, -1);
    }

    public static Struct toStructuredZonedTimestamp(Schema schema, Object value, ZoneOffset defaultOffset, TemporalAdjuster adjuster, int precision) {
        OffsetDateTime timestamp;
        String zoneId = null;
        if (value instanceof ZonedDateTime) {
            final ZonedDateTime zonedDateTime = (ZonedDateTime) value;
            timestamp = zonedDateTime.toOffsetDateTime();
            zoneId = zonedDateTime.getZone().getId();
        }
        else if (value instanceof OffsetDateTime) {
            timestamp = (OffsetDateTime) value;
        }
        else if (value instanceof Instant) {
            timestamp = ((Instant) value).atOffset(defaultOffset);
        }
        else {
            final LocalDateTime localDateTime = Conversions.toLocalDateTime(value);
            timestamp = OffsetDateTime.of(localDateTime, defaultOffset);
        }
        if (adjuster != null) {
            timestamp = timestamp.with(adjuster);
        }
        return from(schema, timestamp, zoneId, precision);
    }

    public static Struct positiveInfinity(Schema schema) {
        return StructuredTemporal.specialValue(schema, StructuredTemporal.POSITIVE_INFINITY);
    }

    public static Struct positiveInfinity(Schema schema, int precision) {
        return StructuredTemporal.specialValue(schema, StructuredTemporal.POSITIVE_INFINITY, precision);
    }

    public static Struct negativeInfinity(Schema schema) {
        return StructuredTemporal.specialValue(schema, StructuredTemporal.NEGATIVE_INFINITY);
    }

    public static Struct negativeInfinity(Schema schema, int precision) {
        return StructuredTemporal.specialValue(schema, StructuredTemporal.NEGATIVE_INFINITY, precision);
    }

    public static ZoneId zoneId(Struct value) {
        final String zoneId = value.getString(StructuredTemporal.ZONE_ID_FIELD);
        if (zoneId != null) {
            return ZoneId.of(zoneId);
        }
        final Integer offsetSeconds = value.getInt32(StructuredTemporal.OFFSET_SECONDS_FIELD);
        return offsetSeconds == null ? ZoneOffset.UTC : ZoneOffset.ofTotalSeconds(offsetSeconds);
    }

    private StructuredZonedTimestamp() {
    }
}
