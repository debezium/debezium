/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Boundary helpers for PostgreSQL {@code TIME} values.
 */
final class PostgresTimeBoundary {

    static final String BOUNDARY_TIME = "24:00:00";
    static final String BOUNDARY_TIME_WITH_TIMEZONE = "24:00:00Z";
    static final String TIME_QUERY_BINDING = "cast(? as time)";
    static final String TIME_WITH_TIMEZONE_QUERY_BINDING = "cast(? as time with time zone)";

    private static final long DAY_MILLIS = TimeUnit.DAYS.toMillis(1);
    private static final long DAY_MICROS = TimeUnit.DAYS.toMicros(1);
    private static final long DAY_NANOS = TimeUnit.DAYS.toNanos(1);
    private static final Pattern BOUNDARY_TIME_WITH_TIMEZONE_PATTERN = Pattern.compile("^24:00:00(?:\\.0{1,6})?(?:Z|[+-]00(?::?00)?)$");

    static boolean isBoundaryMilliseconds(Object value) {
        return value instanceof Number number && number.longValue() == DAY_MILLIS;
    }

    static boolean isBoundaryMicroseconds(Object value) {
        return value instanceof Number number && number.longValue() == DAY_MICROS;
    }

    static boolean isBoundaryNanoseconds(Object value) {
        return value instanceof Number number && number.longValue() == DAY_NANOS;
    }

    static boolean isBoundaryTimeWithTimezone(Object value) {
        return value instanceof String string && BOUNDARY_TIME_WITH_TIMEZONE_PATTERN.matcher(string).matches();
    }

    private PostgresTimeBoundary() {
    }
}
