/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import java.sql.Types;
import java.time.LocalDate;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.jdbc.ValueBindDescriptor;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.time.ZonedTime;

/**
 * An implementation of {@link Type} for {@link ZonedTime} values.
 *
 * @author Chris Cranford
 */
public class ZonedTimeType extends io.debezium.connector.jdbc.type.debezium.ZonedTimeType {

    public static final ZonedTimeType INSTANCE = new ZonedTimeType();

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {

        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }

        if (value instanceof String) {
            final ZonedDateTime zdt = OffsetTime.parse((String) value, ZonedTime.FORMATTER).atDate(LocalDate.now()).toZonedDateTime();

            if (getDialect().isTimeZoneSet()) {
                if (getDialect().shouldBindTimeWithTimeZoneAsDatabaseTimeZone()) {
                    return List
                            .of(new ValueBindDescriptor(index, zdt.withZoneSameInstant(getDatabaseTimeZone().toZoneId()).toOffsetDateTime(), Types.TIMESTAMP));
                }

                return List.of(new ValueBindDescriptor(index, zdt.toOffsetDateTime(), Types.TIMESTAMP)); // TIMESTAMP_WITH_TIMEZONE not supported
            }
            return List.of(new ValueBindDescriptor(index, zdt.toOffsetDateTime(), Types.TIMESTAMP));

        }

        throw new ConnectException(String.format("Unexpected %s value '%s' with type '%s'", getClass().getSimpleName(),
                value, value.getClass().getName()));
    }
}
