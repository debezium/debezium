/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.starrocks;

import java.time.Duration;
import java.util.Date;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.util.DateTimeUtils;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An implementation of {@link JdbcType} for {@link Time} values for StarRocks; times are
 * stored as formatted strings.
 */
class ConnectTimeType extends AbstractTimeToStringType {

    public static final ConnectTimeType INSTANCE = new ConnectTimeType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Time.LOGICAL_NAME };
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        if (value instanceof Date date) {
            return String.format("'%s'", format(toDurationFromDate(date)));
        }
        return null;
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        if (value instanceof Date date) {
            return List.of(new ValueBindDescriptor(index, format(toDurationFromDate(date))));
        }
        throw new ConnectException(String.format("Unexpected %s value '%s' with type '%s'", getClass().getSimpleName(),
                value, value.getClass().getName()));
    }

    @Override
    protected Duration toDuration(Number value) {
        return Duration.ofMillis(value.longValue());
    }

    private static Duration toDurationFromDate(Date value) {
        return Duration.ofNanos(DateTimeUtils.toLocalTimeFromUtcDate(value).toNanoOfDay());
    }
}
