/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.jdbc.type.AbstractTimeType;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An abstract Debezium time-type implementation of {@link AbstractTimeType}.
 *
 * @author Chris Cranford
 */
public abstract class AbstractDebeziumTimeType extends AbstractTimeType {

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return getDialect().getTimeQueryBinding();
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        final LocalTime localTime = getLocalTime((Number) value);
        if (getDialect().isTimeZoneSet()) {
            return getDialect().getFormattedDateTime(localTime.atDate(LocalDate.now()).atZone(getDatabaseTimeZone().toZoneId()));
        }
        return getDialect().getFormattedTime(localTime);
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {

        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        if (value instanceof Number) {
            final LocalTime localTime = getLocalTime((Number) value);
            final LocalDateTime localDateTime = localTime.atDate(LocalDate.now());
            if (getDialect().isTimeZoneSet()) {
                return List.of(new ValueBindDescriptor(index,
                        localDateTime.atZone(getDatabaseTimeZone().toZoneId()).toLocalDateTime(), getJdbcType()));
            }
            return List.of(new ValueBindDescriptor(index, localDateTime));
        }
        throw new ConnectException(String.format("Unexpected %s value '%s' with type '%s'", getClass().getSimpleName(),
                value, value.getClass().getName()));
    }

    protected int getJdbcType() {
        return Types.TIMESTAMP;
    }

    protected abstract LocalTime getLocalTime(Number value);
}
