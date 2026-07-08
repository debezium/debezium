/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.starrocks;

import java.time.Duration;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An abstract {@link JdbcType} implementation for time column types for StarRocks, which has
 * no TIME type. Values are stored as {@code HH:mm:ss.ffffff} formatted strings in
 * {@code VARCHAR} columns.
 *
 * MySQL TIME columns accept values from {@code -838:59:59} to {@code 838:59:59}, so values
 * outside the 24-hour clock range are formatted with the same sign and rolled-over hour
 * notation that MySQL uses rather than a time-of-day representation.
 */
abstract class AbstractTimeToStringType extends AbstractType {

    private static final long MICROS_PER_NANO = 1_000L;

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        if (value instanceof Number number) {
            return String.format("'%s'", format(toDuration(number)));
        }
        return null;
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "varchar(18)";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        if (value instanceof Number number) {
            return List.of(new ValueBindDescriptor(index, format(toDuration(number))));
        }
        throw new ConnectException(String.format("Unexpected %s value '%s' with type '%s'", getClass().getSimpleName(),
                value, value.getClass().getName()));
    }

    protected abstract Duration toDuration(Number value);

    protected static String format(Duration duration) {
        final boolean negative = duration.isNegative();
        final Duration absolute = duration.abs();
        return String.format("%s%02d:%02d:%02d.%06d",
                negative ? "-" : "",
                absolute.toHours(),
                absolute.toMinutesPart(),
                absolute.toSecondsPart(),
                absolute.toNanosPart() / MICROS_PER_NANO);
    }
}
