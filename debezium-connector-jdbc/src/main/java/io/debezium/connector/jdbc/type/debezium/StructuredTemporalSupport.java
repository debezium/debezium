/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.time.StructuredTemporal;

final class StructuredTemporalSupport {

    static LocalDate toLocalDate(Struct value) {
        requireFinite(value);
        return LocalDate.of(
                value.getInt32(StructuredTemporal.YEAR_FIELD),
                value.getInt8(StructuredTemporal.MONTH_FIELD),
                value.getInt8(StructuredTemporal.DAY_FIELD));
    }

    static LocalTime toLocalTime(Struct value) {
        requireFinite(value);
        return LocalTime.of(
                value.getInt8(StructuredTemporal.HOUR_FIELD),
                value.getInt8(StructuredTemporal.MINUTE_FIELD),
                value.getInt8(StructuredTemporal.SECOND_FIELD),
                value.getInt32(StructuredTemporal.NANOS_FIELD));
    }

    static LocalDateTime toLocalDateTime(Struct value) {
        return LocalDateTime.of(toLocalDate(value), toLocalTime(value));
    }

    static OffsetDateTime toOffsetDateTime(Struct value) {
        final LocalDateTime localDateTime = toLocalDateTime(value);
        final Integer offsetSeconds = value.getInt32(StructuredTemporal.OFFSET_SECONDS_FIELD);
        return OffsetDateTime.of(localDateTime, offsetSeconds == null ? ZoneOffset.UTC : ZoneOffset.ofTotalSeconds(offsetSeconds));
    }

    static void requireFinite(Struct value) {
        if (!StructuredTemporal.isFinite(value)) {
            throw new ConnectException("Non-finite structured temporal values require dialect-specific handling");
        }
    }

    private StructuredTemporalSupport() {
    }
}
