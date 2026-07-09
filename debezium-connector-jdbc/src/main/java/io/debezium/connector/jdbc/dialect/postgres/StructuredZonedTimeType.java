/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.jdbc.type.debezium.StructuredTemporalSupport;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * PostgreSQL implementation of {@link io.debezium.time.StructuredZonedTime} values.
 */
public class StructuredZonedTimeType extends io.debezium.connector.jdbc.type.debezium.StructuredZonedTimeType {

    public static final StructuredZonedTimeType INSTANCE = new StructuredZonedTimeType();

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "cast(? as timetz)";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }

        final Struct struct = requireStruct(value);
        // Build the timetz literal from the raw components so the original offset and the PostgreSQL
        // end-of-day boundary hour 24 are preserved (OffsetTime/LocalTime cannot represent hour 24).
        return List.of(new ValueBindDescriptor(index, StructuredTemporalSupport.toTimetzLiteral(struct)));
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "timetz";
    }
}
