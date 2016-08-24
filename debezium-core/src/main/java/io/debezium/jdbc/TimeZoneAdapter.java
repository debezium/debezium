/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

import io.debezium.annotation.Immutable;

/**
 * An adapter that can convert {@link java.util.Date}, {@link java.sql.Date}, {@link java.sql.Time}, and
 * {@link java.sql.Timestamp} objects to {@link ZonedDateTime} instances, where the time zone in which the temporal objects
 * were created by the database/driver can be adjusted.
 * 
 * @author Randall Hauch
 */
@Immutable
public class TimeZoneAdapter {

    private static final LocalDate EPOCH = LocalDate.ofEpochDay(0);
    public static final ZoneId UTC = ZoneId.of("UTC");

    /**
     * Create a new adapter with UTC as the target zone and for a database that uses UTC for all temporal values.
     * 
     * @return the new adapter
     */
    public static TimeZoneAdapter create() {
        return new TimeZoneAdapter(UTC, UTC, UTC, UTC, UTC);
    }

    /**
     * Create a new adapter for a database that uses the specified zone for all temporal values.
     * 
     * @param zoneId the zone in which all temporal values are created by the database; may not be null
     * @return the new adapter
     */
    public static TimeZoneAdapter originatingIn(ZoneId zoneId) {
        return new TimeZoneAdapter(ZoneId.systemDefault(), zoneId, zoneId, zoneId, zoneId);
    }

    /**
     * Create a new adapter for a database that creates all temporal values in UTC.
     * 
     * @return the new adapter
     */
    public static TimeZoneAdapter originatingInUtc() {
        return originatingIn(UTC);
    }

    /**
     * Create a new adapter for a database that creates all temporal values in the local system time zone,
     * which is the same time zone used by {@link java.util.Calendar#getInstance()}.
     * 
     * @return the new adapter
     */
    public static TimeZoneAdapter originatingInLocal() {
        return originatingIn(ZoneId.systemDefault()); // same as Calendar.getInstance().getTimeZone().toZoneId()
    }

    private final ZoneId targetZoneId;
    private final ZoneId utilDateZoneId;
    private final ZoneId sqlDateZoneId;
    private final ZoneId sqlTimeZoneId;
    private final ZoneId sqlTimestampZoneId;

    /**
     * Create an adapter for temporal values defined in terms of the given zone.
     * 
     * @param targetZoneId the zone in which the output temporal values are defined; may not be null
     * @param utilDateZoneId the zone in which {@link java.util.Date} values are defined; may not be null
     * @param sqlDateZoneId the zone in which {@link java.sql.Date} values are defined; may not be null
     * @param sqlTimeZoneId the zone in which {@link java.sql.Time} values are defined; may not be null
     * @param sqlTimestampZoneId the zone in which {@link java.sql.Timestamp} values are defined; may not be null
     */
    protected TimeZoneAdapter(ZoneId targetZoneId, ZoneId utilDateZoneId, ZoneId sqlDateZoneId, ZoneId sqlTimeZoneId,
            ZoneId sqlTimestampZoneId) {
        this.targetZoneId = targetZoneId;
        this.utilDateZoneId = utilDateZoneId;
        this.sqlDateZoneId = sqlDateZoneId;
        this.sqlTimeZoneId = sqlTimeZoneId;
        this.sqlTimestampZoneId = sqlTimestampZoneId;
    }

    protected ZoneId targetZoneId() {
        return targetZoneId;
    }

    /**
     * Convert the specified database {@link java.util.Date}, {@link java.sql.Date}, {@link java.sql.Time}, or
     * {@link java.sql.Timestamp} objects to a date and time in the same time zone in which the database created the
     * value. If only {@link java.sql.Time time} information is provided in the input value, the date information will
     * be set to the first day of the epoch. If only {@link java.sql.Date date} information is provided in the input
     * value, the time information will be at midnight on the specified day.
     * 
     * @param dbDate the database-generated value; may not be null
     * @return the date time in the same zone used by the database; never null
     */
    public ZonedDateTime toZonedDateTime(java.util.Date dbDate) {
        if (dbDate instanceof java.sql.Date) {
            return toZonedDateTime((java.sql.Date) dbDate);
        }
        if (dbDate instanceof java.sql.Time) {
            return toZonedDateTime((java.sql.Time) dbDate);
        }
        if (dbDate instanceof java.sql.Timestamp) {
            return toZonedDateTime((java.sql.Timestamp) dbDate);
        }
        return dbDate.toInstant().atZone(UTC) // milliseconds is in terms of UTC
                     .withZoneSameInstant(sqlTimeZoneId) // correct value in the zone where it was created
                     .withZoneSameLocal(targetZoneId); // use same value, but in our desired timezone
    }

    /**
     * Convert the specified database {@link java.sql.Date} to a date (at midnight) in the same time zone in which the
     * database created the value.
     * 
     * @param dbDate the database-generated value; may not be null
     * @return the date (at midnight) in the same zone used by the database; never null
     */
    public ZonedDateTime toZonedDateTime(java.sql.Date dbDate) {
        long millis = dbDate.getTime();
        Instant instant = Instant.ofEpochMilli(millis).truncatedTo(ChronoUnit.DAYS);
        return instant.atZone(sqlDateZoneId).withZoneSameInstant(targetZoneId);
    }

    /**
     * Convert the specified database {@link java.sql.Time} to a time (on the first epoch day) in the same time zone in which
     * the database created the value.
     * 
     * @param dbTime the database-generated value; may not be null
     * @return the time (on the first epoch day) in the same zone used by the database; never null
     */
    public ZonedDateTime toZonedDateTime(java.sql.Time dbTime) {
        long millis = dbTime.getTime();
        LocalTime local = LocalTime.ofNanoOfDay(millis * 1000 * 1000);
        return ZonedDateTime.of(EPOCH, local, UTC) // milliseconds is in terms of UTC
                            .withZoneSameInstant(sqlTimeZoneId) // correct value in the zone where it was created
                            .withZoneSameLocal(targetZoneId); // use same value, but in our desired timezone
    }

    /**
     * Convert the specified database {@link java.sql.Timestamp} to a timestamp in the same time zone in which
     * the database created the value.
     * 
     * @param dbTimestamp the database-generated value; may not be null
     * @return the timestamp in the same zone used by the database; never null
     */
    public ZonedDateTime toZonedDateTime(java.sql.Timestamp dbTimestamp) {
        return dbTimestamp.toInstant().atZone(UTC) // milliseconds is in terms of UTC
                          .withZoneSameInstant(sqlTimestampZoneId) // correct value in the zone where it was created
                          .withZoneSameLocal(targetZoneId); // use same value, but in our desired timezone
    }

    /**
     * Create a new adapter that produces temporal values in the specified time zone.
     * 
     * @param zoneId the zone in which all temporal values are to be defined; may not be null
     * @return the new adapter
     */
    public TimeZoneAdapter withTargetZone(ZoneId zoneId) {
        if (targetZoneId.equals(zoneId)) return this;
        return new TimeZoneAdapter(zoneId, utilDateZoneId, sqlDateZoneId, sqlTimeZoneId, sqlTimestampZoneId);
    }

    /**
     * Create a new adapter for a database that uses the specified zone for all temporal values and this adapter's target zone.
     * 
     * @param zoneId the zone in which all temporal values are created by the database; may not be null
     * @return the new adapter
     */
    public TimeZoneAdapter withZoneForAll(ZoneId zoneId) {
        return new TimeZoneAdapter(targetZoneId, zoneId, zoneId, zoneId, zoneId);
    }

    /**
     * Create a new adapter for a database that uses the same time zones as this adapter except it uses the specified
     * zone for {@link java.util.Date} temporal values.
     * 
     * @param zoneId the zone in which all {@link java.util.Date} values are created by the database; may not be null
     * @return the new adapter; never null
     */
    public TimeZoneAdapter withZoneForUtilDate(ZoneId zoneId) {
        if (utilDateZoneId.equals(zoneId)) return this;
        return new TimeZoneAdapter(targetZoneId, zoneId, sqlDateZoneId, sqlTimeZoneId, sqlTimestampZoneId);
    }

    /**
     * Create a new adapter for a database that uses the same time zones as this adapter except it uses the specified
     * zone for {@link java.sql.Date} temporal values.
     * 
     * @param zoneId the zone in which all {@link java.sql.Date} values are created by the database; may not be null
     * @return the new adapter; never null
     */
    public TimeZoneAdapter withZoneForSqlDate(ZoneId zoneId) {
        if (sqlDateZoneId.equals(zoneId)) return this;
        return new TimeZoneAdapter(targetZoneId, utilDateZoneId, zoneId, sqlTimeZoneId, sqlTimestampZoneId);
    }

    /**
     * Create a new adapter for a database that uses the same time zones as this adapter except it uses the specified
     * zone for {@link java.sql.Time} temporal values.
     * 
     * @param zoneId the zone in which all {@link java.sql.Time} values are created by the database; may not be null
     * @return the new adapter; never null
     */
    public TimeZoneAdapter withZoneForSqlTime(ZoneId zoneId) {
        if (sqlTimeZoneId.equals(zoneId)) return this;
        return new TimeZoneAdapter(targetZoneId, utilDateZoneId, sqlDateZoneId, zoneId, sqlTimestampZoneId);
    }

    /**
     * Create a new adapter for a database that uses the same time zones as this adapter except it uses the specified
     * zone for {@link java.sql.Timestamp} temporal values.
     * 
     * @param zoneId the zone in which all {@link java.sql.Timestamp} values are created by the database; may not be null
     * @return the new adapter; never null
     */
    public TimeZoneAdapter withZoneForSqlTimestamp(ZoneId zoneId) {
        if (sqlTimestampZoneId.equals(zoneId)) return this;
        return new TimeZoneAdapter(targetZoneId, utilDateZoneId, sqlDateZoneId, sqlTimeZoneId, zoneId);
    }

    /**
     * Create a new adapter for a database that uses the same time zones as this adapter except it uses the UTC
     * zone for the target.
     * 
     * @return the new adapter; never null
     */
    public TimeZoneAdapter withUtcTargetZone() {
        return withTargetZone(UTC);
    }

    /**
     * Create a new adapter for a database that uses the same time zones as this adapter except it uses the UTC
     * zone for {@link java.util.Date} temporal values.
     * 
     * @return the new adapter; never null
     */
    public TimeZoneAdapter withUtcZoneForUtilDate() {
        return withZoneForUtilDate(UTC);
    }

    /**
     * Create a new adapter for a database that uses the same time zones as this adapter except it uses the UTC
     * zone for {@link java.sql.Date} temporal values.
     * 
     * @return the new adapter; never null
     */
    public TimeZoneAdapter withUtcZoneForSqlDate() {
        return withZoneForSqlDate(UTC);
    }

    /**
     * Create a new adapter for a database that uses the same time zones as this adapter except it uses the UTC
     * zone for {@link java.sql.Time} temporal values.
     * 
     * @return the new adapter; never null
     */
    public TimeZoneAdapter withUtcZoneForSqlTime() {
        return withZoneForSqlTime(UTC);
    }

    /**
     * Create a new adapter for a database that uses the same time zones as this adapter except it uses the UTC
     * zone for {@link java.sql.Timestamp} temporal values.
     * 
     * @return the new adapter; never null
     */
    public TimeZoneAdapter withUtcZoneForSqlTimestamp() {
        return withZoneForSqlTimestamp(UTC);
    }

    /**
     * Create a new adapter for a database that uses the same time zones as this adapter except it uses the UTC
     * zone for the target.
     * 
     * @return the new adapter; never null
     */
    public TimeZoneAdapter withLocalTargetZone() {
        return withTargetZone(ZoneId.systemDefault());
    }

    /**
     * Create a new adapter for a database that uses the same time zones as this adapter except it uses the UTC
     * zone for {@link java.util.Date} temporal values.
     * 
     * @return the new adapter; never null
     */
    public TimeZoneAdapter withLocalZoneForUtilDate() {
        return withZoneForUtilDate(ZoneId.systemDefault());
    }

    /**
     * Create a new adapter for a database that uses the same time zones as this adapter except it uses the UTC
     * zone for {@link java.sql.Date} temporal values.
     * 
     * @return the new adapter; never null
     */
    public TimeZoneAdapter withLocalZoneForSqlDate() {
        return withZoneForSqlDate(ZoneId.systemDefault());
    }

    /**
     * Create a new adapter for a database that uses the same time zones as this adapter except it uses the UTC
     * zone for {@link java.sql.Time} temporal values.
     * 
     * @return the new adapter; never null
     */
    public TimeZoneAdapter withLocalZoneForSqlTime() {
        return withZoneForSqlTime(ZoneId.systemDefault());
    }

    /**
     * Create a new adapter for a database that uses the same time zones as this adapter except it uses the UTC
     * zone for {@link java.sql.Timestamp} temporal values.
     * 
     * @return the new adapter; never null
     */
    public TimeZoneAdapter withLocalZoneForSqlTimestamp() {
        return withZoneForSqlTimestamp(ZoneId.systemDefault());
    }
}