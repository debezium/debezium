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

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.debezium.StructuredTemporalSupport;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * Oracle implementation of {@link io.debezium.time.StructuredTime} values.
 */
public class StructuredTimeType extends io.debezium.connector.jdbc.type.debezium.StructuredTimeType {

    public static final StructuredTimeType INSTANCE = new StructuredTimeType();

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        final int precision = getTimePrecision(schema);
        final DatabaseDialect dialect = getDialect();
        if (precision > 0) {
            return dialect.getJdbcTypeName(Types.TIMESTAMP, Size.precision(precision));
        }
        return dialect.getJdbcTypeName(Types.TIMESTAMP, Size.precision(dialect.getMaxTimePrecision()));
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        final LocalDateTime localDateTime = StructuredTemporalSupport.toLocalTime(requireStruct(value)).atDate(LocalDate.EPOCH);
        return List.of(new ValueBindDescriptor(index, localDateTime, Types.TIMESTAMP));
    }
}
