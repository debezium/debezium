/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import org.apache.kafka.connect.data.Schema;

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
    public String getDefaultValueBinding(Schema schema, Object value) {
        return String.format(getDialect().getByteArrayFormat(), ByteArrayUtils.getByteArrayAsHex(value));
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        final int bitSize = Integer.parseInt(schema.parameters().get(Bits.LENGTH_FIELD));
        return String.format("bit(%d)", bitSize);
    }

}
