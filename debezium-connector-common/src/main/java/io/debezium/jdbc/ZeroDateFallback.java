/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

import java.sql.Types;
import java.time.LocalDate;
import java.util.Objects;

import io.debezium.annotation.Immutable;

/**
 * Per-type zero-date sentinel values used when a non-nullable temporal column receives a zero
 * date (typically MySQL/MariaDB {@code 0000-00-00}). Each sentinel's derived epoch
 * representations are emitted in place of the historic hard-coded {@code 1970-01-01} fallback.
 *
 * <p>The three slots correspond to the JDBC column types that participate in MySQL/MariaDB
 * zero-date semantics:
 * <ul>
 *   <li>{@code DATE} columns map to {@link Types#DATE}</li>
 *   <li>{@code DATETIME} columns map to {@link Types#TIMESTAMP}</li>
 *   <li>{@code TIMESTAMP} columns map to {@link Types#TIMESTAMP_WITH_TIMEZONE}</li>
 * </ul>
 */
@Immutable
public final class ZeroDateFallback {

    private static final ZeroDateFallback DEFAULT_EPOCH = new ZeroDateFallback(LocalDate.EPOCH, LocalDate.EPOCH, LocalDate.EPOCH);

    private final LocalDate forDate;
    private final LocalDate forDatetime;
    private final LocalDate forTimestamp;

    public ZeroDateFallback(LocalDate forDate, LocalDate forDatetime, LocalDate forTimestamp) {
        this.forDate = Objects.requireNonNull(forDate, "forDate");
        this.forDatetime = Objects.requireNonNull(forDatetime, "forDatetime");
        this.forTimestamp = Objects.requireNonNull(forTimestamp, "forTimestamp");
    }

    /**
     * Returns the historic 1970-01-01 fallback for all three temporal types — equivalent to the
     * behavior before per-type sentinels were introduced.
     */
    public static ZeroDateFallback defaultEpoch() {
        return DEFAULT_EPOCH;
    }

    public LocalDate forDate() {
        return forDate;
    }

    public LocalDate forDatetime() {
        return forDatetime;
    }

    public LocalDate forTimestamp() {
        return forTimestamp;
    }

    /**
     * Returns the configured sentinel for the given JDBC column type. Only the three temporal
     * types touched by MySQL/MariaDB zero-date semantics are recognized; all other JDBC types
     * fall back to {@link LocalDate#EPOCH}.
     */
    public LocalDate forJdbcType(int jdbcType) {
        switch (jdbcType) {
            case Types.DATE:
                return forDate;
            case Types.TIMESTAMP:
                return forDatetime;
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return forTimestamp;
            default:
                return LocalDate.EPOCH;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ZeroDateFallback)) {
            return false;
        }
        ZeroDateFallback that = (ZeroDateFallback) o;
        return forDate.equals(that.forDate)
                && forDatetime.equals(that.forDatetime)
                && forTimestamp.equals(that.forTimestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(forDate, forDatetime, forTimestamp);
    }

    @Override
    public String toString() {
        return "ZeroDateFallback{forDate=" + forDate
                + ", forDatetime=" + forDatetime
                + ", forTimestamp=" + forTimestamp + "}";
    }
}
