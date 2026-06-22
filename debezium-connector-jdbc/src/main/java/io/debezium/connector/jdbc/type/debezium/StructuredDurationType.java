/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.sql.Types;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.engine.jdbc.Size;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.StructuredDuration;

/**
 * An implementation of {@link StructuredDuration} values.
 */
public class StructuredDurationType extends AbstractType {

    public static final StructuredDurationType INSTANCE = new StructuredDurationType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ StructuredDuration.SCHEMA_NAME };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "?";
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return getDialect().getJdbcTypeName(Types.VARCHAR, Size.length(128));
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        return "'" + StructuredTemporalSupport.toDurationString(requireStruct(value)) + "'";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        return List.of(new ValueBindDescriptor(index, StructuredTemporalSupport.toDurationString(requireStruct(value)), Types.VARCHAR));
    }
}
