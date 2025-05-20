/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.connect;

import java.sql.Types;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.Type;

/**
 * An implementation of {@link Type} that supports {@code BOOLEAN} connect schema types.
 *
 * @author Chris Cranford
 */
public class ConnectBooleanType extends AbstractConnectSchemaType {

    public static final ConnectBooleanType INSTANCE = new ConnectBooleanType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "BOOLEAN" };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return getDialect().getJdbcTypeName(Types.BOOLEAN);
    }

}
