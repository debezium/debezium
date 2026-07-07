/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.oracle;

import java.sql.Types;
import java.time.LocalDate;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.hibernate.engine.jdbc.Size;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.debezium.StructuredTemporalSupport;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.StructuredTemporal;

/**
 * Oracle implementation of {@link io.debezium.time.StructuredZonedTime} values.
 */
public class StructuredZonedTimeType extends io.debezium.connector.jdbc.type.debezium.StructuredZonedTimeType {

    public static final StructuredZonedTimeType INSTANCE = new StructuredZonedTimeType();

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        final int precision = getTimePrecision(schema);
        final DatabaseDialect dialect = getDialect();
        if (precision > 0) {
            return dialect.getJdbcTypeName(Types.TIME_WITH_TIMEZONE, Size.precision(precision));
        }
        return dialect.getJdbcTypeName(Types.TIME_WITH_TIMEZONE, Size.precision(dialect.getMaxTimePrecision()));
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
        final ZonedDateTime zonedDateTime = offsetTime.atDate(LocalDate.EPOCH).toZonedDateTime();
        return List.of(new ValueBindDescriptor(index, zonedDateTime, Types.TIME_WITH_TIMEZONE));
    }
}
