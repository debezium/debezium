/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.time.StructuredTemporal;

final class StructuredTemporalLiteral {

    static String date(Struct value) {
        requireFinite(value);
        return String.format("%04d-%02d-%02d",
                value.getInt32(StructuredTemporal.YEAR_FIELD),
                value.getInt8(StructuredTemporal.MONTH_FIELD),
                value.getInt8(StructuredTemporal.DAY_FIELD));
    }

    static String timestamp(Struct value) {
        requireFinite(value);
        final Integer nanos = value.getInt32(StructuredTemporal.NANOS_FIELD);
        return String.format("%04d-%02d-%02d %02d:%02d:%02d.%06d",
                value.getInt32(StructuredTemporal.YEAR_FIELD),
                value.getInt8(StructuredTemporal.MONTH_FIELD),
                value.getInt8(StructuredTemporal.DAY_FIELD),
                value.getInt8(StructuredTemporal.HOUR_FIELD),
                value.getInt8(StructuredTemporal.MINUTE_FIELD),
                value.getInt8(StructuredTemporal.SECOND_FIELD),
                (nanos == null ? 0 : nanos) / 1_000);
    }

    static String duration(Struct value) {
        final int hours = value.getInt32(StructuredTemporal.HOURS_FIELD) == null ? 0 : value.getInt32(StructuredTemporal.HOURS_FIELD);
        final int minutes = value.getInt32(StructuredTemporal.MINUTES_FIELD) == null ? 0 : value.getInt32(StructuredTemporal.MINUTES_FIELD);
        final Long seconds = value.getInt64(StructuredTemporal.SECONDS_FIELD);
        final int nanos = value.getInt32(StructuredTemporal.NANOS_FIELD) == null ? 0 : value.getInt32(StructuredTemporal.NANOS_FIELD);
        final boolean negative = hours < 0 || minutes < 0 || (seconds != null && seconds < 0) || nanos < 0;
        return String.format("%s%03d:%02d:%02d.%06d",
                negative ? "-" : "",
                Math.abs(hours),
                Math.abs(minutes),
                Math.abs(seconds == null ? 0 : seconds),
                Math.abs(nanos) / 1_000);
    }

    private static void requireFinite(Struct value) {
        if (!StructuredTemporal.isFinite(value)) {
            throw new ConnectException("MySQL does not support structured temporal infinity values");
        }
    }

    private StructuredTemporalLiteral() {
    }
}
