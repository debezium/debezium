/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.sqlserver;

import java.math.BigInteger;
import java.sql.Types;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.engine.jdbc.Size;
import org.hibernate.query.Query;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.connector.jdbc.util.ByteArrayUtils;
import io.debezium.data.Bits;

/**
 * An implementation of {@link Type} for {@link Bits} types.
 *
 * @author Chris Cranford
 */
class BitType extends AbstractType {

    public static final BitType INSTANCE = new BitType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Bits.LOGICAL_NAME };
    }

    @Override
    public String getQueryBinding(Schema schema) {
        if (Bits.LOGICAL_NAME.equals(schema.name())) {
            final int bitSize = Integer.parseInt(schema.parameters().get(Bits.LENGTH_FIELD));
            return String.format("cast(? as %s)", bitSize > 1 ? String.format("varbinary(%d)", bitSize) : "bit");
        }
        return "cast(? as varbinary)";
    }

    @Override
    public String getDefaultValueBinding(DatabaseDialect dialect, Schema schema, Object value) {
        return String.format(dialect.getByteArrayFormat(), ByteArrayUtils.getByteArrayAsHex(value));
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        if (Bits.LOGICAL_NAME.equals(schema.name())) {
            final int bitSize = Integer.parseInt(schema.parameters().get(Bits.LENGTH_FIELD));
            if (bitSize == 1) {
                return dialect.getTypeName(Types.BIT);
            }
            return dialect.getTypeName(Types.VARBINARY, Size.length(bitSize));
        }
        return dialect.getTypeName(Types.VARBINARY);
    }

    @Override
    public void bind(Query<?> query, int index, Schema schema, Object value) {
        if (value == null) {
            query.setParameter(index, null);
        }
        else if (value instanceof byte[]) {
            final BigInteger bigIntegerValue = new BigInteger((byte[]) value);
            query.setParameter(index, bigIntegerValue.intValue());
        }
        else {
            throwUnexpectedValue(value);
        }
    }
}
