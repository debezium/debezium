/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.starrocks;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.ZonedTime;

/**
 * An implementation of {@link JdbcType} for {@link ZonedTime} values for StarRocks, which has
 * no time-with-time-zone type. The ISO-8601 offset time representation emitted by the source
 * is stored verbatim in a {@code VARCHAR} column, preserving the original offset without loss.
 */
class ZonedTimeType extends AbstractType {

    public static final ZonedTimeType INSTANCE = new ZonedTimeType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ ZonedTime.SCHEMA_NAME };
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        if (value instanceof String string) {
            return String.format("'%s'", string);
        }
        return null;
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "varchar(32)";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        if (value instanceof String) {
            return List.of(new ValueBindDescriptor(index, value));
        }
        throw new ConnectException(String.format("Unexpected %s value '%s' with type '%s'", getClass().getSimpleName(),
                value, value.getClass().getName()));
    }
}
