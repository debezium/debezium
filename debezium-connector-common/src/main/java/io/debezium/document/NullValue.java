/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.document;

import java.math.BigDecimal;
import java.math.BigInteger;

import io.debezium.annotation.Immutable;

/**
 * A specialization of {@link Value} that represents a null value.
 * @author Randall Hauch
 */
@Immutable
final class NullValue implements Value {

    public static final Value INSTANCE = new NullValue();

    private NullValue() {
        // prevent instantiation
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this;
    }

    @Override
    public String toString() {
        return "null";
    }

    @Override
    public int compareTo(Value that) {
        if (this == that) {
            return 0;
        }
        return -1;
    }

    @Override
    public Type getType() {
        return Type.NULL;
    }

    @Override
    public Object asObject() {
        return null;
    }

    @Override
    public String asString() {
        return null;
    }

    @Override
    public Integer asInteger() {
        return null;
    }

    @Override
    public Long asLong() {
        return null;
    }

    @Override
    public Boolean asBoolean() {
        return null;
    }

    @Override
    public Number asNumber() {
        return null;
    }

    @Override
    public BigInteger asBigInteger() {
        return null;
    }

    @Override
    public BigDecimal asBigDecimal() {
        return null;
    }

    @Override
    public Float asFloat() {
        return null;
    }

    @Override
    public Double asDouble() {
        return null;
    }

    @Override
    public byte[] asBytes() {
        return null;
    }

    @Override
    public Document asDocument() {
        return null;
    }

    @Override
    public Array asArray() {
        return null;
    }

    @Override
    public boolean isNull() {
        return true;
    }

    @Override
    public boolean isString() {
        return false;
    }

    @Override
    public boolean isBoolean() {
        return false;
    }

    @Override
    public boolean isInteger() {
        return false;
    }

    @Override
    public boolean isLong() {
        return false;
    }

    @Override
    public boolean isFloat() {
        return false;
    }

    @Override
    public boolean isDouble() {
        return false;
    }

    @Override
    public boolean isNumber() {
        return false;
    }

    @Override
    public boolean isBigInteger() {
        return false;
    }

    @Override
    public boolean isBigDecimal() {
        return false;
    }

    @Override
    public boolean isBinary() {
        return false;
    }

    @Override
    public boolean isDocument() {
        return false;
    }

    @Override
    public boolean isArray() {
        return false;
    }

    @Override
    public Value convert() {
        return this;
    }

    @Override
    public Value clone() {
        return this;
    }

}
