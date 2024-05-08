/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.time;

import java.time.temporal.ChronoUnit;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * A utility representing a duration into a corresponding {@link SchemaBuilder#int64() INT64}
 * number of <em>nanosecond</em>, and for defining a Kafka Connect {@link Schema} for duration values.
 *
 **/
public class NanoDuration {

    public static final String SCHEMA_NAME = "io.debezium.time.NanoDuration";

    /**
     * Returns a {@link SchemaBuilder} for a {@link NanoDuration}. The resulting schema will describe a field
     * with the {@value #SCHEMA_NAME} as the {@link Schema#name() name} and {@link SchemaBuilder#int64()} ()} INT64} for the literal
     * type storing the number of <em>nanoseconds</em> for that duration.
     * <p>
     * You can use the resulting SchemaBuilder to set or override additional schema settings such as required/optional, default
     * value, and documentation.
     *
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.int64()
                .name(SCHEMA_NAME)
                .version(1);
    }

    /**
     * Returns a Schema for a {@link NanoDuration} but with all other default Schema settings. The schema describes a field
     * with the {@value #SCHEMA_NAME} as the {@link Schema#name() name} and {@link SchemaBuilder#int64()} ()} INT64} for the literal
     * type storing the number of <em>nanoseconds</em>.
     *
     * @return the schema
     * @see #builder()
     */
    public static Schema schema() {
        return builder().build();
    }

    private NanoDuration() {
    }

    /**
     * Converts a number of time units to a duration in nanoseconds.
     *
     * @param years a number of years
     * @param months a number of months
     * @param days a number of days
     * @param hours a number of hours
     * @param minutes a number of minutes
     * @param seconds a number of seconds
     * @param nanos a number of nanoseconds
     * @return Approximate representation of the given interval as a number of nanoseconds
     */
    public static long durationNanos(int years, int months, int days, int hours, int minutes, long seconds, long nanos) {
        long daysPerMonthAvg = ChronoUnit.MONTHS.getDuration().toDays();
        long numberOfDays = ((years * 12) + months) * daysPerMonthAvg + days;
        long numberOfSeconds = (((numberOfDays * 24 + hours) * 60) + minutes) * 60 + seconds;
        return numberOfSeconds * ChronoUnit.SECONDS.getDuration().toNanos() + nanos;
    }

    /**
     * Converts a number of time units to a duration in nanoseconds.
     *
     * @param years a number of years
     * @param months a number of months
     * @param days a number of days
     * @param hours a number of hours
     * @param minutes a number of minutes
     * @param seconds a number of seconds
     * @return Approximate representation of the given interval as a number of nanoseconds
     */
    public static long durationNanos(int years, int months, int days, int hours, int minutes, long seconds) {
        return durationNanos(years, months, days, hours, minutes, seconds, 0);
    }
}
