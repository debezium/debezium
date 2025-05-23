/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.sink.column.ColumnDescriptor;

/**
 * An implementation of {@link JdbcType} for {@code INET} column types.
 *
 * @author Chris Cranford
 */
class InetType extends AbstractType {

    public static final InetType INSTANCE = new InetType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "INET" };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "cast(? as inet)";
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "inet";
    }

}
