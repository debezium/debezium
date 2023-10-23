/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.time.LocalDateTime;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.jdbc.ValueBindDescriptor;
import io.debezium.connector.jdbc.type.AbstractTimestampType;

/**
 * An abstract Debezium timestamp-type implementation of {@link AbstractTimestampType}.
 *
 * @author Chris Cranford
 */
public abstract class AbstractDebeziumTimestampType extends AbstractTimestampType {

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {

        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        if (value instanceof Number) {

            final LocalDateTime localDateTime = getLocalDateTime(((Number) value).longValue());
            if (getDialect().isTimeZoneSet()) {

                if (getDialect().isZonedTimeSupported()) {

                    return List.of(new ValueBindDescriptor(index, localDateTime.atZone(getDatabaseTimeZone().toZoneId())));
                }
                // TODO tested only with PostgreSQL check with others Databases
                // For timestamp with time zone, the internally stored value is always in UTC (Universal Coordinated Time, traditionally known as Greenwich Mean Time, GMT).
                // An input value that has an explicit time zone specified is converted to UTC using the appropriate offset for that time zone.
                // If no time zone is stated in the input string, then it is assumed to be in the time zone indicated by the system's TimeZone parameter,
                // and is converted to UTC using the offset for the timezone zone.
                //
                // When a timestamp with time zone value is output, it is always converted from UTC to the current timezone zone, and displayed as local time in that zone.
                // https://www.postgresql.org/docs/current/datatype-datetime.html
                return List.of(new ValueBindDescriptor(index, localDateTime.atZone(getDatabaseTimeZone().toZoneId()).toOffsetDateTime()));
            }

            return List.of(new ValueBindDescriptor(index, localDateTime));
        }

        throw new ConnectException(String.format("Unexpected %s value '%s' with type '%s'", getClass().getSimpleName(),
                value, value.getClass().getName()));
    }

    protected abstract LocalDateTime getLocalDateTime(long value);

}
