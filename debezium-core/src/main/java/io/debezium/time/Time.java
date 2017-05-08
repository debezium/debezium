/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import java.time.LocalTime;
import java.time.temporal.TemporalAdjuster;

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
     * Get the number of milliseconds past midnight of the given {@link java.time.LocalDateTime}, {@link java.time.LocalDate},
     * {@link java.time.LocalTime}, {@link java.util.Date}, {@link java.sql.Date}, {@link java.sql.Time}, or
     * {@link java.sql.Timestamp}, ignoring any date portions of the supplied value.
     * 
     * @param value the local or SQL date, time, or timestamp value; may not be null
     * @param adjuster the optional component that adjusts the local date value before obtaining the epoch day; may be null if no
     * adjustment is necessary
     * @return the milliseconds past midnight
     * @throws IllegalArgumentException if the value is not an instance of the acceptable types
     */
    public static int toMilliOfDay(Object value, TemporalAdjuster adjuster) {
        LocalTime time = Conversions.toLocalTime(value);
        if (adjuster != null) {
            time = time.with(adjuster);
        }
        long micros = Math.floorDiv(time.toNanoOfDay(), Conversions.NANOSECONDS_PER_MILLISECOND);
        assert Math.abs(micros) < Integer.MAX_VALUE;
        return (int) micros;
    }

    private Time() {
    }
}
