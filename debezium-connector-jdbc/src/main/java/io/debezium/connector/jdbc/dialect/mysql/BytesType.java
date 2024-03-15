/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import java.sql.Types;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.engine.jdbc.Size;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractBytesType;
import io.debezium.connector.jdbc.type.Type;

/**
 * An implementation of {@link Type} for {@code BYTES} column types.
 *
 * @author Chris Cranford
 */
class BytesType extends AbstractBytesType {

    public static final BytesType INSTANCE = new BytesType();

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        final Optional<String> columnType = getSourceColumnType(schema);
        if (columnType.isPresent()) {
            if ("TINYBLOB".equalsIgnoreCase(columnType.get())) {
                return "tinyblob";
            }
            else if ("MEDIUMBLOB".equalsIgnoreCase(columnType.get())) {
                return "mediumblob";
            }
            else if ("BLOB".equalsIgnoreCase(columnType.get())) {
                return "blob";
            }
            else if ("LARGEBLOB".equalsIgnoreCase(columnType.get())) {
                return "largeblob";
            }
            final int columnSize = Integer.parseInt(getSourceColumnSize(schema).orElse("0"));
            if (columnSize > 0) {
                return dialect.getTypeName(Types.VARBINARY, Size.length(columnSize));
            }
        }
        return dialect.getTypeName(Types.VARBINARY);
    }

    @Override
    public String getDefaultValueBinding(DatabaseDialect dialect, Schema schema, Object value) {
        // No default values permitted
        return null;
    }
}
