/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.oracle;

import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.engine.jdbc.Size;

import io.debezium.connector.jdbc.type.debezium.StructuredTemporalSupport;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * Oracle implementation of {@link io.debezium.time.StructuredTime} values.
 */
public class StructuredTimeType extends io.debezium.connector.jdbc.type.debezium.StructuredTimeType {

    public static final StructuredTimeType INSTANCE = new StructuredTimeType();

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return getDialect().getJdbcTypeName(Types.TIMESTAMP, Size.precision(getSchemaTimePrecision(schema)));
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        final LocalDateTime localDateTime = StructuredTemporalSupport.toLocalTime(requireStruct(value)).atDate(LocalDate.EPOCH);
        return List.of(new ValueBindDescriptor(index, localDateTime, Types.TIMESTAMP));
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, ColumnDescriptor column, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        validate(column, schema, value);
        final int precision = getDialect().getTargetTemporalCapabilities().targetTimePrecision(column);
        final LocalDateTime localDateTime = StructuredTemporalSupport
                .toLocalTime(requireStruct(value), precision, getPrecisionLossHandlingMode())
                .atDate(LocalDate.EPOCH);
        return List.of(new ValueBindDescriptor(index, localDateTime, Types.TIMESTAMP));
    }

    @Override
    protected int getSchemaTimePrecision(Schema schema) {
        final int precision = getTimePrecision(schema);
        return precision >= 0 ? precision : getDialect().getMaxTimePrecision();
    }
}
