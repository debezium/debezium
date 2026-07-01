/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Boundary helpers for PostgreSQL temporal values.
 */
public final class PostgresTimeBoundary {

    public static final String TIME_WITH_TIMEZONE_BOUNDARY_AT_UTC = "24:00:00Z";

    private static final long DAY_MICROS = TimeUnit.DAYS.toMicros(1);
    private static final Pattern TIME_WITH_TIMEZONE_BOUNDARY_AT_UTC_PATTERN = Pattern.compile("^24:00:00(?:\\.0{1,6})?(?:Z|[+-]00(?::?00)?)$");

    public static boolean isTimeWithTimeZoneBoundaryAtUtc(String value) {
        return value != null && TIME_WITH_TIMEZONE_BOUNDARY_AT_UTC_PATTERN.matcher(value).matches();
    }

    public static boolean isBoundaryMicroseconds(long value) {
        return value == DAY_MICROS;
    }

    private PostgresTimeBoundary() {
    }
}
