/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import java.math.BigInteger;
import java.util.BitSet;
import java.util.List;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.util.ByteArrayUtils;
import io.debezium.data.Bits;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An implementation of {@link JdbcType} for {@link Bits} types.
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
        return String.format("b'%s'", toBitString(value, getBitSize(schema)));
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        final int bitSize = getBitSize(schema);
        return String.format("bit(%d)", bitSize);
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }

        return List.of(new ValueBindDescriptor(index, new BigInteger(toBitString(value, getBitSize(schema)), 2)));
    }

    private int getBitSize(Schema schema) {
        return Integer.parseInt(schema.parameters().get(Bits.LENGTH_FIELD));
    }

    private String toBitString(Object value, int length) {
        final byte[] bytes = ByteArrayUtils.getByteArrayFromValue(value);
        if (bytes == null) {
            throw unexpectedValue(value);
        }

        final BitSet bits = BitSet.valueOf(bytes);
        final int width = length == Integer.MAX_VALUE ? bits.length() : length;
        if (width == 0) {
            return "0";
        }

        final StringBuilder builder = new StringBuilder(width);
        for (int i = width - 1; i >= 0; --i) {
            builder.append(bits.get(i) ? '1' : '0');
        }
        return builder.toString();
    }

}
