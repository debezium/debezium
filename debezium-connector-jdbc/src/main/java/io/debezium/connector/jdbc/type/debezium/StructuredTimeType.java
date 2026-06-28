/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.time.LocalTime;
import java.util.List;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractTimeType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.StructuredTime;

/**
 * An implementation of {@link JdbcType} for {@link StructuredTime} values.
 */
public class StructuredTimeType extends AbstractTimeType {

    public static final StructuredTimeType INSTANCE = new StructuredTimeType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ StructuredTime.SCHEMA_NAME };
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
        return getDialect().getFormattedTime(StructuredTemporalSupport.toLocalTime(requireStruct(value)));
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        final LocalTime localTime = StructuredTemporalSupport.toLocalTime(requireStruct(value));
        return List.of(new ValueBindDescriptor(index, localTime));
    }
}
