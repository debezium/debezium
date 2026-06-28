/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.time.LocalDateTime;
import java.util.List;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractTimestampType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.StructuredTimestamp;

/**
 * An implementation of {@link JdbcType} for {@link StructuredTimestamp} values.
 */
public class StructuredTimestampType extends AbstractTimestampType {

    public static final StructuredTimestampType INSTANCE = new StructuredTimestampType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ StructuredTimestamp.SCHEMA_NAME };
    }

    @Override
    protected int getTimePrecision(Schema schema) {
        final String length = getSourceColumnSize(schema).orElse("-1");
        return getSourceColumnPrecision(schema).map(Integer::parseInt).orElseGet(() -> Integer.parseInt(length));
    }

    @Override
    protected boolean shouldUseSourcePrecision(int precision, int maxPrecision) {
        return precision >= 0 && precision <= maxPrecision;
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        return getDialect().getFormattedDateTime(StructuredTemporalSupport.toLocalDateTime(requireStruct(value)));
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        final LocalDateTime localDateTime = StructuredTemporalSupport.toLocalDateTime(requireStruct(value));
        return List.of(new ValueBindDescriptor(index, localDateTime, getJdbcType()));
    }
}
