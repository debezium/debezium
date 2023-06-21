/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.relational.ColumnDescriptor;
import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.Type;

/**
 * An implementation of {@link Type} for {@code MACADDR} column types.
 *
 * @author Chris Cranford
 */
class MacAddressType extends AbstractType {

    public static final MacAddressType INSTANCE = new MacAddressType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "MACADDR" };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema) {
        return "cast(? as macaddr)";
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        return "macaddr";
    }

}
