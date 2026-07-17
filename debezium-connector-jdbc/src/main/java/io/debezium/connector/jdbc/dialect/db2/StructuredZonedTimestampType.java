/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.db2;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.jdbc.type.debezium.StructuredTemporalPreflightValidator;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.time.StructuredZonedTimestamp;

/**
 * Db2 binding for structured zoned timestamps. Db2 LUW has no zoned timestamp type, so only values whose zone
 * metadata can be discarded without changing their meaning pass validation.
 */
public class StructuredZonedTimestampType extends StructuredTimestampType {

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ StructuredZonedTimestamp.SCHEMA_NAME };
    }

    @Override
    public void validate(ColumnDescriptor column, Schema schema, Object value) {
        if (value != null) {
            final Struct struct = requireStruct(value);
            StructuredTemporalPreflightValidator.validateZonedTimestamp(struct, getDialect().getTargetTemporalCapabilities());
        }
        super.validate(column, schema, value);
    }

    @Override
    protected String toLiteral(Schema schema, Struct value) {
        StructuredTemporalPreflightValidator.validateZonedTimestamp(value, getDialect().getTargetTemporalCapabilities());
        return super.toLiteral(schema, value);
    }
}
