/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgoutput;

import java.math.BigDecimal;

import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.connector.postgresql.connection.AbstractColumnValue;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.util.Strings;

/**
 * @author Chris Cranford
 */
class PgOutputColumnValue extends AbstractColumnValue<String> {

    private String value;

    PgOutputColumnValue(String value) {
        this.value = value;
    }

    @Override
    public String getRawValue() {
        return value;
    }

    @Override
    public boolean isNull() {
        return value == null;
    }

    @Override
    public String asString() {
        return value;
    }

    @Override
    public Boolean asBoolean() {
        return "t".equalsIgnoreCase(value);
    }

    @Override
    public Integer asInteger() {
        return Integer.valueOf(value);
    }

    @Override
    public Long asLong() {
        return Long.valueOf(value);
    }

    @Override
    public Float asFloat() {
        return Float.valueOf(value);
    }

    @Override
    public Double asDouble() {
        return Double.valueOf(value);
    }

    @Override
    public SpecialValueDecimal asDecimal() {
        return PostgresValueConverter.toSpecialValue(value).orElseGet(() -> new SpecialValueDecimal(new BigDecimal(value)));
    }

    @Override
    public byte[] asByteArray() {
        return Strings.hexStringToByteArray(value.substring(2));
    }
}
