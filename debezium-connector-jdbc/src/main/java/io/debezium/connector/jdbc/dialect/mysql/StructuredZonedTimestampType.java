/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.debezium.StructuredTemporalPreflightValidator;
import io.debezium.sink.column.ColumnDescriptor;

/**
 * MySQL implementation of {@link io.debezium.time.StructuredZonedTimestamp} values.
 */
public class StructuredZonedTimestampType extends StructuredTimestampType {

    public static final StructuredZonedTimestampType INSTANCE = new StructuredZonedTimestampType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ io.debezium.time.StructuredZonedTimestamp.SCHEMA_NAME };
    }

    @Override
    public void validate(ColumnDescriptor column, Schema schema, Object value) {
        super.validate(column, schema, value);
        if (value != null) {
            StructuredTemporalPreflightValidator.validateZonedTimestamp(
                    requireStruct(value), getDialect().getTargetTemporalCapabilities());
        }
    }
}
