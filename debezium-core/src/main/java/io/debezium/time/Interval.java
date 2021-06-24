/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.time;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.temporal.ChronoUnit;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * A utility representing a duration into a string value formatted using ISO string format.
 *
 * @author Jiri Pechanec (jpechane@redhat.com)
 */
public class Interval {

    public static final String SCHEMA_NAME = "io.debezium.time.Interval";

    /**
     * Returns a {@link SchemaBuilder} for a {@link Interval}. The resulting schema will describe a field
     * with the {@value #SCHEMA_NAME} as the {@link Schema#name() name} and {@link SchemaBuilder#string()()} STRING} for the literal
     * type storing the components of the interval.
     * <p>
     * You can use the resulting SchemaBuilder to set or override additional schema settings such as required/optional, default
     * value, and documentation.
     *
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.string()
                .name(SCHEMA_NAME)
                .version(1);
    }

    /**
     * Returns a Schema for a {@link Interval} but with all other default Schema settings. The schema describes a field
     * with the {@value #SCHEMA_NAME} as the {@link Schema#name() name} and {@link SchemaBuilder#string()} STRING} for the literal
     * type storing the components of the interval.
     *
     * @return the schema
     * @see #builder()
     */
    public static Schema schema() {
        return builder().build();
    }

    private Interval() {
    }

    /**
     * Converts a number of time units to a ISO formatted string.
     *
     * @param years a number of years
     * @param months a number of months
     * @param days a number of days
     * @param hours a number of hours
     * @param minutes a number of minutes
     * @param seconds a number of seconds
     * @param micros a number of microseconds
     * @param daysPerMonthAvg an optional value representing a days per month average; if null, the default duration
     * from {@link ChronoUnit#MONTHS} is used.
     * @return @return Approximate representation of the given interval as a number of microseconds
    */
    public static String toIsoString(int years, int months, int days, int hours, int minutes, BigDecimal seconds) {
        // ISO pattern - PnYnMnDTnHnMnS
        if (seconds.scale() > 9) {
            seconds = seconds.setScale(9, RoundingMode.DOWN);
        }
        return "P" + years + "Y" + months + "M" + days + "D" + "T" + hours + "H" + minutes + "M" + seconds.stripTrailingZeros().toPlainString() + "S";
    }
}
