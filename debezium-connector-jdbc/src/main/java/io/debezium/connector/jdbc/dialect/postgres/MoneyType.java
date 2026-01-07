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
 * An implementation of {@link JdbcType} for {@code MONEY} data types.
 *
 * @author Chris Cranford
 */
class MoneyType extends AbstractType {

    public static final MoneyType INSTANCE = new MoneyType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "MONEY" };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "cast(? as money)";
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "money";
    }

}
