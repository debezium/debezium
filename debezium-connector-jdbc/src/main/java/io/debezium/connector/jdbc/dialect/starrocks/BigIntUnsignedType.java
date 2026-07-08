/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.starrocks;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An implementation of {@link JdbcType} that provides support for {@code BIGINT UNSIGNED}
 * column types for StarRocks, which has no unsigned integer types; values are stored in
 * {@code LARGEINT} (128-bit) columns so the full unsigned 64-bit range is preserved.
 */
class BigIntUnsignedType extends AbstractType {

    public static final BigIntUnsignedType INSTANCE = new BigIntUnsignedType();
    private static final String[] REGISTRATION_KEYS = new String[]{ "BIGINT UNSIGNED", "BIGINT UNSIGNED ZEROFILL", "INT8 UNSIGNED", "INT8 UNSIGNED ZEROFILL" };

    @Override
    public String[] getRegistrationKeys() {
        return REGISTRATION_KEYS;
    }

    public boolean matchesSourceColumnType(Schema schema) {
        if (schema.parameters() == null) {
            return false;
        }

        final String columnType = schema.parameters().get("__debezium.source.column.type");
        if (columnType == null) {
            return false;
        }

        for (String registrationKey : REGISTRATION_KEYS) {
            if (registrationKey.equalsIgnoreCase(columnType)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "largeint";
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        return toUnsignedBigInt(value).toString();
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return super.bind(index, schema, null);
        }
        return List.of(new ValueBindDescriptor(index, toUnsignedBigInt(value)));
    }

    private BigDecimal toUnsignedBigInt(Object value) {
        if (value instanceof BigDecimal decimal) {
            return decimal;
        }
        if (value instanceof BigInteger integer) {
            return new BigDecimal(integer);
        }
        if (value instanceof Long longValue) {
            // Values above Long.MAX_VALUE arrive as negative two's-complement longs when the
            // source uses bigint.unsigned.handling.mode=long; restore the unsigned value.
            return new BigDecimal(Long.toUnsignedString(longValue));
        }
        if (value instanceof Number number) {
            return new BigDecimal(number.toString());
        }
        if (value instanceof String string) {
            return new BigDecimal(string);
        }

        throw unexpectedValue(value);
    }
}
