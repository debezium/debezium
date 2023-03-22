/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.connect;

import java.sql.Types;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.Type;

/**
 * An implementation of {@link Type} that supports {@code INT64} connect schema types.
 *
 * @author Chris Cranford
 */
public class ConnectInt64Type extends AbstractConnectSchemaType {

    public static final ConnectInt64Type INSTANCE = new ConnectInt64Type();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "INT64" };
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        return dialect.getTypeName(Types.BIGINT);
    }

}
