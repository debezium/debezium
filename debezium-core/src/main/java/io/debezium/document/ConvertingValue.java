/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.document;

import java.math.BigDecimal;
import java.math.BigInteger;

import io.debezium.annotation.Immutable;
import io.debezium.util.Strings;

/**
 * A specialization of {@link Value} that wraps another {@link Value} to allow conversion of types.
 *
 * @author Randall Hauch
 */
@Immutable
final class ConvertingValue implements Value {

    private final Value value;

    ConvertingValue(Value value) {
        assert value != null;
        this.value = value;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return value.equals(obj);
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public int compareTo(Value that) {
        return value.compareTo(that);
    }

    @Override
    public Type getType() {
        return value.getType();
    }

    @Override
    public Object asObject() {
        return value.asObject();
    }

    @Override
    public String asString() {
        return value.isNull() ? null : value.toString();
    }

    @Override
    public Boolean asBoolean() {
        if (value.isBoolean()) {
            return value.asBoolean();
        }
        if (value.isNumber()) {
            return value.asNumber().intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        }
        if (value.isString()) {
            return Boolean.valueOf(asString());
        }
        return null;
    }

    @Override
    public Integer asInteger() {
        if (value.isInteger()) {
            return value.asInteger();
        }
        if (value.isNumber()) {
            return Integer.valueOf(asNumber().intValue());
        }
        if (value.isString()) {
            try {
                return Integer.valueOf(asString());
            }
            catch (NumberFormatException e) {
            }
        }
        return null;
    }

    @Override
    public Long asLong() {
        if (value.isLong()) {
            return value.asLong();
        }
        if (value.isNumber()) {
            return Long.valueOf(asNumber().longValue());
        }
        if (value.isString()) {
            try {
                return Long.valueOf(asString());
            }
            catch (NumberFormatException e) {
            }
        }
        return null;
    }

    @Override
    public Float asFloat() {
        if (value.isFloat()) {
            return value.asFloat();
        }
        if (value.isNumber()) {
            return Float.valueOf(asNumber().floatValue());
        }
        if (value.isString()) {
            try {
                return Float.valueOf(asString());
            }
            catch (NumberFormatException e) {
            }
        }
        return null;
    }

    @Override
    public Double asDouble() {
        if (value.isDouble()) {
            return value.asDouble();
        }
        if (value.isNumber()) {
            return Double.valueOf(asNumber().doubleValue());
        }
        if (value.isString()) {
            try {
                return Double.valueOf(asString());
            }
            catch (NumberFormatException e) {
            }
        }
        return null;
    }

    @Override
    public Number asNumber() {
        if (value.isNumber()) {
            return value.asNumber();
        }
        if (value.isString()) {
            String str = value.asString();
            Number number = Strings.asNumber(str);
            if (number instanceof Short) {
                // Shorts aren't allowed, so just use an integer ...
                number = Integer.valueOf(number.intValue());
            }
            return number;
        }
        return null;
    }

    @Override
    public BigInteger asBigInteger() {
        if (value.isBigInteger()) {
            return value.asBigInteger();
        }
        if (value.isBigDecimal()) {
            return value.asBigDecimal().toBigInteger();
        }
        if (value instanceof Number) {
            return BigInteger.valueOf(asLong().longValue());
        }
        if (value.isString()) {
            try {
                return new BigInteger(asString());
            }
            catch (NumberFormatException e) {
            }
        }
        return null;
    }

    @Override
    public BigDecimal asBigDecimal() {
        if (value.isBigDecimal()) {
            return value.asBigDecimal();
        }
        if (value.isBigInteger()) {
            return new BigDecimal(value.asBigInteger());
        }
        if (value.isInteger() || value.isLong()) {
            return BigDecimal.valueOf(asLong().longValue());
        }
        if (value.isFloat() || value.isDouble()) {
            return BigDecimal.valueOf(asDouble().doubleValue());
        }
        if (value.isString()) {
            try {
                return new BigDecimal(asString());
            }
            catch (NumberFormatException e) {
            }
        }
        return null;
    }

    @Override
    public byte[] asBytes() {
        if (value.isBinary()) {
            return value.asBytes();
        }
        if (value.isString()) {
            return value.asString().getBytes();
        }
        return null;
    }

    @Override
    public Document asDocument() {
        return value.isDocument() ? value.asDocument() : null;
    }

    @Override
    public Array asArray() {
        return value.isArray() ? value.asArray() : null;
    }

    @Override
    public boolean isNull() {
        return value.isNull();
    }

    @Override
    public boolean isString() {
        return value.isString();
    }

    @Override
    public boolean isBoolean() {
        return value.isBoolean();
    }

    @Override
    public boolean isInteger() {
        return value.isInteger();
    }

    @Override
    public boolean isLong() {
        return value.isLong();
    }

    @Override
    public boolean isFloat() {
        return value.isFloat();
    }

    @Override
    public boolean isDouble() {
        return value.isDouble();
    }

    @Override
    public boolean isNumber() {
        return value.isNumber();
    }

    @Override
    public boolean isBigInteger() {
        return value.isBigInteger();
    }

    @Override
    public boolean isBigDecimal() {
        return value.isBigDecimal();
    }

    @Override
    public boolean isDocument() {
        return value.isDocument();
    }

    @Override
    public boolean isArray() {
        return value.isArray();
    }

    @Override
    public boolean isBinary() {
        return value.isBinary();
    }

    @Override
    public Value convert() {
        return this;
    }

    @Override
    public Value clone() {
        Value clonedValue = value.clone();
        if (clonedValue == value) {
            return this;
        }
        return new ConvertingValue(clonedValue);
    }
}
