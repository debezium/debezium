/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.sql.Types;
import java.time.OffsetDateTime;
import java.util.List;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractTimestampType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.StructuredZonedTimestamp;

/**
 * An implementation of {@link JdbcType} for {@link StructuredZonedTimestamp} values.
 */
public class StructuredZonedTimestampType extends AbstractTimestampType {

    public static final StructuredZonedTimestampType INSTANCE = new StructuredZonedTimestampType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ StructuredZonedTimestamp.SCHEMA_NAME };
    }

    @Override
    protected int getJdbcType() {
        return Types.TIMESTAMP_WITH_TIMEZONE;
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
        return getDialect().getFormattedDateTime(StructuredTemporalSupport.toOffsetDateTime(requireStruct(value)));
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        final OffsetDateTime offsetDateTime = StructuredTemporalSupport.toOffsetDateTime(requireStruct(value));
        return List.of(new ValueBindDescriptor(index, offsetDateTime, getJdbcType()));
    }
}
