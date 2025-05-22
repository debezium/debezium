/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.sqlserver;

import java.nio.ByteBuffer;
import java.sql.Types;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.hibernate.engine.jdbc.Size;

import io.debezium.connector.jdbc.type.AbstractBytesType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.util.ByteArrayUtils;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An implementation of {@link JdbcType} for {@code BYTES} column types.
 *
 * @author Bertrand Paquet
 */
class BytesType extends AbstractBytesType {

    public static final BytesType INSTANCE = new BytesType();

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        return String.format(getDialect().getByteArrayFormat(), ByteArrayUtils.getByteArrayAsHex(value));
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        final int columnSize = Integer.parseInt(getSourceColumnSize(schema).orElse("0"));
        if (columnSize > 0) {
            return getDialect().getJdbcTypeName(Types.VARBINARY, Size.length(columnSize));
        }
        else if (isKey) {
            return getDialect().getJdbcTypeName(Types.VARBINARY, Size.length(getDialect().getMaxVarbinaryLength()));
        }
        return getDialect().getJdbcTypeName(Types.VARBINARY);
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schemas, Object value) {
        return "convert(varbinary(max),?,1)";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schemas, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        if (value instanceof ByteBuffer) {
            return List.of(new ValueBindDescriptor(index, ((ByteBuffer) value).array()));
        }
        if (value instanceof byte[]) {
            return List.of(new ValueBindDescriptor(index, value));
        }

        throw new ConnectException(String.format("Unexpected %s value '%s' with type'%s'", getClass().getSimpleName(),
                value, value.getClass().getSimpleName()));
    }
}
