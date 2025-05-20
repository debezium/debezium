/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.db2.connect;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.jdbc.type.AbstractTimeType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.connector.jdbc.util.DateTimeUtils;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An implementation of {@link Type} for {@link org.apache.kafka.connect.data.Date} values.
 *
 * @author Chris Cranford
 */
public class ConnectTimeType extends AbstractTimeType {

    public static final ConnectTimeType INSTANCE = new ConnectTimeType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Time.LOGICAL_NAME };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return getDialect().getTimeQueryBinding();
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        return getDialect().getFormattedTime(DateTimeUtils.toZonedDateTimeFromDate((Date) value, getDatabaseTimeZone()));
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {

        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        if (value instanceof Date) {

            final LocalTime localTime = DateTimeUtils.toLocalTimeFromUtcDate((Date) value);
            final LocalDateTime localDateTime = localTime.atDate(LocalDate.now());
            if (getDialect().isTimeZoneSet()) {
                ZonedDateTime zonedDateTime = localDateTime.atZone(getDatabaseTimeZone().toZoneId());
                return List.of(
                        new ValueBindDescriptor(index, java.sql.Time.valueOf(zonedDateTime.toLocalDateTime().toLocalTime())));
            }

            return List.of(new ValueBindDescriptor(index, localDateTime));
        }

        throw new ConnectException(String.format("Unexpected %s value '%s' with type '%s'", getClass().getSimpleName(),
                value, value.getClass().getName()));
    }

}
