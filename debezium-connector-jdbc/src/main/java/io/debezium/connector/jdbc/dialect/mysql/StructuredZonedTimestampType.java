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
        return io.debezium.time.StructuredZonedTimestamp.schemaNames();
    }

    @Override
    public void validate(ColumnDescriptor column, Schema schema, Object value) {
        // MySQL cannot store a zone or an offset at all, so that failure offers the user no remedy, while the
        // precision and range failures each name a handling mode that resolves them. Report the unrecoverable
        // one first, consistent with the shared base type and the Db2 dialect.
        if (value != null) {
            StructuredTemporalPreflightValidator.validateZonedTimestamp(
                    requireStruct(value), getDialect().getTargetTemporalCapabilities());
        }
        super.validate(column, schema, value);
    }
}
