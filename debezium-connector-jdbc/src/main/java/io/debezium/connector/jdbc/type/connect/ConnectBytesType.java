/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.connect;

import java.sql.Types;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.engine.jdbc.Size;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.connector.jdbc.util.ByteArrayUtils;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An implementation of {@link Type} that supports {@code BYTES} connect schema types.
 *
 * @author Chris Cranford
 */
public class ConnectBytesType extends AbstractConnectSchemaType {

    public static final ConnectBytesType INSTANCE = new ConnectBytesType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "BYTES" };
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        return String.format(getDialect().getByteArrayFormat(), ByteArrayUtils.getByteArrayAsHex(value));
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        final int columnSize = Integer.parseInt(getSourceColumnSize(schema).orElse("0"));
        DatabaseDialect dialect = getDialect();
        if (columnSize > 0) {
            return dialect.getJdbcTypeName(Types.VARBINARY, Size.length(columnSize));
        }
        else if (isKey) {
            return dialect.getJdbcTypeName(Types.VARBINARY, Size.length(dialect.getMaxVarbinaryLength()));
        }
        return dialect.getJdbcTypeName(Types.VARBINARY);
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {

        return List.of(new ValueBindDescriptor(index, value, Types.VARBINARY));
    }
}
