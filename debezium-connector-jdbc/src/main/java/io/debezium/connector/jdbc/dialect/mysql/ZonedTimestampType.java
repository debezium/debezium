/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import java.sql.Types;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.relational.ColumnDescriptor;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.time.ZonedTimestamp;

/**
 * An implementation of {@link Type} for {@link ZonedTimestamp} values.
 *
 * @author Chris Cranford
 */
public class ZonedTimestampType extends io.debezium.connector.jdbc.type.debezium.ZonedTimestampType {

    public static final ZonedTimestampType INSTANCE = new ZonedTimestampType();

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {

        return getDialect().getQueryBindingWithValueCast(column, schema, this);
    }

    @Override
    protected int getJdbcBindType() {
        return Types.TIMESTAMP; // TIMESTAMP_WITH_TIMEZONE not supported
    }
}
