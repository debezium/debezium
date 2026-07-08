/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.starrocks;

import java.sql.Types;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.engine.jdbc.Size;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractBytesType;
import io.debezium.connector.jdbc.type.JdbcType;

/**
 * An implementation of {@link JdbcType} for {@code BYTES} column types for StarRocks; values
 * are stored in {@code VARBINARY} columns. The StarRocks JDBC driver cannot bind
 * {@link java.nio.ByteBuffer} values directly, which the base class converts to byte arrays.
 */
class BytesType extends AbstractBytesType {

    public static final BytesType INSTANCE = new BytesType();

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        final int columnSize = Integer.parseInt(getSourceColumnSize(schema).orElse("0"));
        final DatabaseDialect dialect = getDialect();
        if (columnSize > 0) {
            return dialect.getJdbcTypeName(Types.VARBINARY, Size.length(columnSize));
        }
        return dialect.getJdbcTypeName(Types.VARBINARY);
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        // No default values permitted
        return null;
    }
}
