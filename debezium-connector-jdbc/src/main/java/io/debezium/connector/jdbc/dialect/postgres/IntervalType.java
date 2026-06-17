/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.math.BigDecimal;
import java.util.List;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.MicroDuration;

/**
 * An implementation of {@link JdbcType} for {@link MicroDuration} types.
 *
 * @author Chris Cranford
 */
class IntervalType extends AbstractType {

    public static final IntervalType INSTANCE = new IntervalType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ MicroDuration.SCHEMA_NAME };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "cast(? as interval)";
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "interval";
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        if (value instanceof Long) {
            return String.format("'%s'", getIntervalSeconds((Long) value));
        }
        // apply no default
        return null;
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {

        if (value != null && Long.class.isAssignableFrom(value.getClass())) {
            return List.of(new ValueBindDescriptor(index, getIntervalSeconds((Long) value)));
        }

        return super.bind(index, schema, value);
    }

    private String getIntervalSeconds(long durationMicros) {
        return BigDecimal.valueOf(durationMicros, 6).stripTrailingZeros().toPlainString() + " seconds";
    }
}
