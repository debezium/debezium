/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import java.time.Duration;
import java.time.LocalTime;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * A utility for converting various Java time representations into the {@link SchemaBuilder#int32() INT32} number of
 * <em>milliseconds</em> since midnight, and for defining a Kafka Connect {@link Schema} for time values with no date or timezone
 * information.
 *
 * @author Randall Hauch
 * @see MicroTime
 * @see NanoTime
 */
public class Time {

    public static final String SCHEMA_NAME = "io.debezium.time.Time";

    private static final Duration ONE_DAY = Duration.ofDays(1);

    /**
     * Returns a {@link SchemaBuilder} for a {@link Time}. The resulting schema will describe a field
     * with the {@value #SCHEMA_NAME} as the {@link Schema#name() name} and {@link SchemaBuilder#int32() INT32} for the literal
     * type storing the number of <em>milliseconds</em> past midnight.
     * <p>
     * You can use the resulting SchemaBuilder to set or override additional schema settings such as required/optional, default
     * value, and documentation.
     *
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.int32()
                .name(SCHEMA_NAME)
                .version(1);
    }

    /**
     * Returns a Schema for a {@link Time} but with all other default Schema settings. The schema describes a field
     * with the {@value #SCHEMA_NAME} as the {@link Schema#name() name} and {@link SchemaBuilder#int32() INT32} for the literal
     * type storing the number of <em>milliseconds</em> past midnight.
     *
     * @return the schema
     * @see #builder()
     */
    public static Schema schema() {
        return builder().build();
    }

    /**
     * Get the number of milliseconds past midnight of the given {@link Duration}.
     *
     * @param value the duration value; may not be null
     * @param acceptLargeValues whether to accept values less than 00:00:00 and larger than 24:00:00 or not
     * @return the milliseconds past midnight
     * @throws IllegalArgumentException if the value is not an instance of the acceptable types or it is out of the supported range
     */
    public static int toMilliOfDay(Object value, boolean acceptLargeValues) {
        if (value instanceof Duration) {
            Duration duration = (Duration) value;
            if (!acceptLargeValues && (duration.isNegative() || duration.compareTo(ONE_DAY) > 0)) {
                throw new IllegalArgumentException("Time values must be between 00:00:00 and 24:00:00 (inclusive): " + duration);
            }

            // int conversion is ok for the range of TIME
            return (int) ((Duration) value).toMillis();
        }

        // TODO only needed for SQL Server/Oracle, where we don't produce Duration right away;
        // this should go eventually, as the conversion to LocalTime is superfluous
        LocalTime time = Conversions.toLocalTime(value);
        long millis = Math.floorDiv(time.toNanoOfDay(), Conversions.NANOSECONDS_PER_MILLISECOND);
        assert Math.abs(millis) < Integer.MAX_VALUE;
        return (int) millis;
    }

    private Time() {
    }
}
