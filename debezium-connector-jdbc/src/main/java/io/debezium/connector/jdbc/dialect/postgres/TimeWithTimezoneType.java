/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.time.LocalDate;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.type.debezium.ZonedTimeType;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.ZonedTime;

/**
 * An implementation of {@link JdbcType} for {@link ZonedTime} types for PostgreSQL.
 *
 * @author Chris Cranford
 */
class TimeWithTimezoneType extends ZonedTimeType {

    public static final TimeWithTimezoneType INSTANCE = new TimeWithTimezoneType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ ZonedTime.SCHEMA_NAME };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        if (PostgresTimeBoundary.isBoundaryTimeWithTimezone(value)) {
            return PostgresTimeBoundary.TIME_WITH_TIMEZONE_QUERY_BINDING;
        }
        return super.getQueryBinding(column, schema, value);
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        if (PostgresTimeBoundary.isBoundaryTimeWithTimezone(value)) {
            return "'" + PostgresTimeBoundary.BOUNDARY_TIME_WITH_TIMEZONE + "'";
        }
        return super.getDefaultValueBinding(schema, value);
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {

        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }

        if (PostgresTimeBoundary.isBoundaryTimeWithTimezone(value)) {
            return List.of(new ValueBindDescriptor(index, PostgresTimeBoundary.BOUNDARY_TIME_WITH_TIMEZONE));
        }

        if (value instanceof String) {

            final ZonedDateTime zdt = OffsetTime.parse((String) value, ZonedTime.FORMATTER).atDate(LocalDate.now()).toZonedDateTime();

            if (getDialect().isTimeZoneSet()) {
                if (getDialect().shouldBindTimeWithTimeZoneAsDatabaseTimeZone()) {
                    return List.of(new ValueBindDescriptor(index, zdt.withZoneSameInstant(getDatabaseTimeZone().toZoneId())));
                }

                return List.of(new ValueBindDescriptor(index, zdt.toOffsetDateTime().toOffsetTime()));
            }

            return List.of(new ValueBindDescriptor(index, zdt));

        }

        throw new ConnectException(String.format("Unexpected %s value '%s' with type '%s'", getClass().getSimpleName(),
                value, value.getClass().getName()));
    }
}
