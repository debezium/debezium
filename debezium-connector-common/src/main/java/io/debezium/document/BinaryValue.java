/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.document;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

import io.debezium.annotation.Immutable;

/**
 * A specialization of {@link Value} that represents a binary value.
 *
 * @author Randall Hauch
 */
@Immutable
final class BinaryValue implements Value {

    private final byte[] value;

    BinaryValue(byte[] value) {
        assert value != null;
        this.value = value;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(this.value);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof Value) {
            Value that = (Value) obj;
            if (that.isNull()) {
                return false;
            }
            if (that.isBinary()) {
                return Arrays.equals(this.value, that.asBytes());
            }
            if (that.isString()) {
                return Arrays.equals(this.value, that.asString().getBytes());
            }
            return false;
        }
        return false;
    }

    @Override
    public String toString() {
        return new String(value);
    }

    @Override
    public int compareTo(Value that) {
        if (that.isNull()) {
            return 1;
        }
        if (that.isBinary()) {
            return this.value.length - that.asBytes().length;
        }
        return 1;
    }

    @Override
    public Type getType() {
        return Type.BINARY;
    }

    @Override
    public Object asObject() {
        return value;
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
        return value;
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
        return false;
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
        return true;
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
        return new ConvertingValue(this);
    }

    @Override
    public Value clone() {
        byte[] copy = new byte[this.value.length];
        System.arraycopy(this.value, 0, copy, 0, this.value.length);
        return new BinaryValue(copy);
    }

}
