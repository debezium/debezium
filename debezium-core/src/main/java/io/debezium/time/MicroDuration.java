/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.time;

import java.math.BigDecimal;
import java.time.temporal.ChronoUnit;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * A utility representing a duration into a corresponding {@link SchemaBuilder#float64() FLOAT64} 
 * number of <em>microsecond</em>, and for defining a Kafka Connect {@link Schema} for duration values.
 * 
 * The duration is amount is stored in floating point as opposed to an integer because certain DBs (e.g. Postgres) will store
 * this amount internally into more than 8 bytes. 
 *  
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class MicroDuration {
    
    public static final String SCHEMA_NAME = "io.debezium.time.MicroDuration";
    
    /**
     * Returns a {@link SchemaBuilder} for a {@link MicroDuration}. The resulting schema will describe a field
     * with the {@value #SCHEMA_NAME} as the {@link Schema#name() name} and {@link SchemaBuilder#float64()} FLOAT64} for the literal
     * type storing the number of <em>microseconds</em> for that duration.
     * <p>
     * You can use the resulting SchemaBuilder to set or override additional schema settings such as required/optional, default
     * value, and documentation.
     *
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.float64()
                            .name(SCHEMA_NAME)
                            .version(1);
    }
    
    /**
     * Returns a Schema for a {@link MicroDuration} but with all other default Schema settings. The schema describes a field
     * with the {@value #SCHEMA_NAME} as the {@link Schema#name() name} and {@link SchemaBuilder#float64()} FLOAT64} for the literal
     * type storing the number of <em>microseconds</em>.
     *
     * @return the schema
     * @see #builder()
     */
    public static Schema schema() {
        return builder().build();
    }
 
    private MicroDuration() {
    }
    
    /**
     * Converts a number of time units to a duration in microseconds.
     *
     * @param years a number of years
     * @param months a number of months
     * @param days a number of days
     * @param hours a number of hours
     * @param minutes a number of minutes
     * @param seconds a number of seconds
     * @param daysPerMonthAvg an optional value representing a days per month average; if null, the default duration 
     * from {@link ChronoUnit#MONTHS} is used.
     * @return a {@link BigDecimal} value which contains the number of microseconds, never {@code null}
     */
    public static double durationMicros(int years, int months, int days, int hours, int minutes, double seconds, 
                                        Double daysPerMonthAvg) {
        if (daysPerMonthAvg == null) {
            daysPerMonthAvg = (double) ChronoUnit.MONTHS.getDuration().toDays();
        }
        double numberOfDays = ((years * 12) + months) * daysPerMonthAvg + days;
        double numberOfSeconds = (((numberOfDays * 24 + hours) * 60) + minutes) * 60 + seconds;
        return numberOfSeconds * 1e6;          
    }
}
