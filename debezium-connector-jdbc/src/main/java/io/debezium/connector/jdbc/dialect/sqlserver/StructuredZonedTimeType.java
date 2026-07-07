/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.sqlserver;

import java.sql.Types;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.hibernate.engine.jdbc.Size;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.debezium.StructuredTemporalSupport;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.StructuredTemporal;

/**
 * SQL Server implementation of {@link io.debezium.time.StructuredZonedTime} values.
 */
public class StructuredZonedTimeType extends io.debezium.connector.jdbc.type.debezium.StructuredZonedTimeType {

    public static final StructuredZonedTimeType INSTANCE = new StructuredZonedTimeType();

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "cast(? as datetimeoffset(7))";
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        final int precision = getTimePrecision(schema);
        final DatabaseDialect dialect = getDialect();
        if (precision > 0) {
            return dialect.getJdbcTypeName(Types.TIMESTAMP_WITH_TIMEZONE, Size.precision(precision));
        }
        return dialect.getJdbcTypeName(Types.TIMESTAMP_WITH_TIMEZONE, Size.precision(dialect.getMaxTimePrecision()));
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        final Struct struct = requireStruct(value);
        final OffsetTime offsetTime = OffsetTime.of(
                StructuredTemporalSupport.toLocalTime(struct),
                ZoneOffset.ofTotalSeconds(struct.getInt32(StructuredTemporal.OFFSET_SECONDS_FIELD)));
        final OffsetDateTime offsetDateTime = offsetTime.atDate(LocalDate.EPOCH);
        return List.of(new ValueBindDescriptor(index, offsetDateTime, Types.TIMESTAMP_WITH_TIMEZONE));
    }
}
