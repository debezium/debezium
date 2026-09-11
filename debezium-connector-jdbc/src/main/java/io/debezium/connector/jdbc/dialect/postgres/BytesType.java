/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.sql.Types;
import java.util.Collection;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.engine.jdbc.Size;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractBytesType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.util.ByteArrayUtils;

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
    public Object[] convertArray(Schema schema, Collection<?> values) {
        if (schema.name() != null) {
            // A named BYTES schema can represent a logical value rather than raw binary data.
            return super.convertArray(schema, values);
        }
        // PostgreSQL selects its bytea[] encoder by the runtime array type, including for empty arrays.
        return values.stream().map(BytesType::toBinaryElement).toArray(byte[][]::new);
    }

    private static byte[] toBinaryElement(Object element) {
        if (element == null) {
            return null;
        }
        final byte[] bytes = ByteArrayUtils.getByteArrayFromValue(element);
        if (bytes == null) {
            throw new IllegalArgumentException("Unsupported BYTES array element type: " + element.getClass().getName());
        }
        return bytes;
    }
}
