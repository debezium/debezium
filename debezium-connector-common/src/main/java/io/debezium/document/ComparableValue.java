/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.document;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import io.debezium.annotation.Immutable;

/**
 * A specialization of {@link Value} that wraps another {@link Value} that is not comparable.
 *
 * @author Randall Hauch
 */
@Immutable
final class ComparableValue implements Value {

    private static final Map<Class<?>, Type> TYPES_BY_CLASS;

    static {
        Map<Class<?>, Type> types = new HashMap<>();
        types.put(String.class, Type.STRING);
        types.put(Boolean.class, Type.BOOLEAN);
        types.put(byte[].class, Type.BINARY);
        types.put(Integer.class, Type.INTEGER);
        types.put(Long.class, Type.LONG);
        types.put(Float.class, Type.FLOAT);
        types.put(Double.class, Type.DOUBLE);
        types.put(BigInteger.class, Type.BIG_INTEGER);
        types.put(BigDecimal.class, Type.DECIMAL);
        types.put(BasicDocument.class, Type.DOCUMENT);
        types.put(BasicArray.class, Type.ARRAY);
        TYPES_BY_CLASS = types;
    }

    static Type typeForValue(Value value) {
        assert value != null;
        if (value.isNull()) {
            return Type.NULL;
        }
        // Check by exact class ...
        Type type = TYPES_BY_CLASS.get(value.getClass());
        if (type != null) {
            return type;
        }
        // Otherwise, check using instanceof ...
        if (value.isString()) {
            return Type.STRING;
        }
        if (value.isBoolean()) {
            return Type.BOOLEAN;
        }
        if (value.isBinary()) {
            return Type.BINARY;
        }
        if (value.isInteger()) {
            return Type.INTEGER;
        }
        if (value.isLong()) {
            return Type.LONG;
        }
        if (value.isFloat()) {
            return Type.FLOAT;
        }
        if (value.isDouble()) {
            return Type.DOUBLE;
        }
        if (value.isBigInteger()) {
            return Type.BIG_INTEGER;
        }
        if (value.isBigDecimal()) {
            return Type.DECIMAL;
        }
        if (value.isDocument()) {
            return Type.DOCUMENT;
        }
        if (value.isArray()) {
            return Type.ARRAY;
        }
        if (value.isNull()) {
            return Type.NULL;
        }
        assert false;
        throw new IllegalStateException();
    }

    private final Comparable<?> value;

    ComparableValue(Comparable<?> value) {
        assert value != null;
        this.value = value;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof Value) {
            Value that = (Value) obj;
            if (this.isNumber() && that.isNumber()) {
                if (this.isLong()) {
                    return this.asLong().equals(that.asLong());
                }
                if (this.isDouble()) {
                    return this.asDouble().equals(that.asDouble());
                }
                if (this.isInteger()) {
                    return this.asInteger().equals(that.asInteger());
                }
                if (this.isFloat()) {
                    return this.asFloat().equals(that.asFloat());
                }
                if (this.isBigDecimal()) {
                    return this.asBigDecimal().equals(that.asBigDecimal());
                }
                if (this.isBigInteger()) {
                    return this.asBigInteger().equals(that.asBigInteger());
                }
            }
            return this.value.equals(that.asObject());
        }
        // Compare the value straight away ...
        return this.value.equals(obj);
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compareTo(Value that) {
        if (Value.isNull(that)) {
            return 1;
        }
        if (this.isBoolean() && that.isBoolean()) {
            return this.asBoolean().compareTo(that.asBoolean());
        }
        if (this.isNumber() && that.isNumber()) {
            if (this.isLong()) {
                return this.asLong().compareTo(that.asLong());
            }
            if (this.isDouble()) {
                return this.asDouble().compareTo(that.asDouble());
            }
            if (this.isInteger()) {
                return this.asInteger().compareTo(that.asInteger());
            }
            if (this.isFloat()) {
                return this.asFloat().compareTo(that.asFloat());
            }
            if (this.isBigDecimal()) {
                return this.asBigDecimal().compareTo(that.asBigDecimal());
            }
            return this.asBigInteger().compareTo(that.asBigInteger());
        }
        if (this.isDocument() && that.isDocument()) {
            return this.asDocument().compareTo(that.asDocument());
        }
        if (this.isArray() && that.isArray()) {
            return this.asArray().compareTo(that.asArray());
        }
        Comparable<Object> thisValue = (Comparable<Object>) this.asObject();
        Comparable<Object> thatValue = (Comparable<Object>) ((ComparableValue) that.comparable()).asObject();
        if (thisValue.getClass().isAssignableFrom(thatValue.getClass())) {
            return thisValue.compareTo(thatValue);
        }
        else if (thatValue.getClass().isAssignableFrom(thisValue.getClass())) {
            return thatValue.compareTo(thisValue) * -1; // correct for the reversed comparison
        }
        return ((Comparable<Object>) this.value).compareTo(that.asObject());
    }

    @Override
    public Type getType() {
        Type type = TYPES_BY_CLASS.get(value.getClass());
        if (type == null) {
            // Didn't match by exact class, so then figure out the extensible types by instanceof ...
            if (isDocument()) {
                return Type.DOCUMENT;
            }
            if (isArray()) {
                return Type.ARRAY;
            }
            if (isNull()) {
                return Type.NULL;
            }
        }
        assert type != null;
        return type;
    }

    @Override
    public Comparable<?> asObject() {
        return value;
    }

    @Override
    public String asString() {
        return isString() ? (String) value : null;
    }

    @Override
    public Integer asInteger() {
        if (value instanceof Integer) {
            return (Integer) value;
        }
        if (value instanceof Long) {
            long raw = ((Long) value).longValue();
            if (isValidInteger(raw)) {
                return Integer.valueOf((int) raw);
            }
        }
        return null;
    }

    private static boolean isValidInteger(long value) {
        return value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE;
    }

    private static boolean isValidFloat(double value) {
        return value >= Float.MIN_VALUE && value <= Float.MAX_VALUE;
    }

    @Override
    public Long asLong() {
        if (value instanceof Long) {
            return (Long) value;
        }
        if (value instanceof Integer) {
            return Long.valueOf(((Integer) value).longValue());
        }
        return null;
    }

    @Override
    public Boolean asBoolean() {
        return isBoolean() ? (Boolean) value : null;
    }

    @Override
    public Number asNumber() {
        return isNumber() ? (Number) value : null;
    }

    @Override
    public BigInteger asBigInteger() {
        return isBigInteger() ? (BigInteger) value : null;
    }

    @Override
    public BigDecimal asBigDecimal() {
        return isBigDecimal() ? (BigDecimal) value : null;
    }

    @Override
    public Float asFloat() {
        if (value instanceof Float) {
            return (Float) value;
        }
        if (value instanceof Double) {
            double raw = ((Double) value).doubleValue();
            if (isValidFloat(raw)) {
                return Float.valueOf((float) raw);
            }
        }
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        return null;
    }

    @Override
    public Double asDouble() {
        if (value instanceof Double) {
            return (Double) value;
        }
        if (value instanceof Float) {
            return Double.valueOf(((Float) value).doubleValue());
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return null;
    }

    @Override
    public Document asDocument() {
        return isDocument() ? (Document) value : null;
    }

    @Override
    public Array asArray() {
        return isArray() ? (Array) value : null;
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public boolean isString() {
        return value instanceof String;
    }

    @Override
    public boolean isBoolean() {
        return value instanceof Boolean;
    }

    @Override
    public boolean isInteger() {
        return value instanceof Integer || (value instanceof Long && isValidInteger(((Long) value).longValue()));
    }

    @Override
    public boolean isLong() {
        return value instanceof Long || value instanceof Integer; // all integers are longs
    }

    @Override
    public boolean isFloat() {
        return value instanceof Float || (value instanceof Double && isValidFloat(((Double) value).doubleValue()));
    }

    @Override
    public boolean isDouble() {
        return value instanceof Double || value instanceof Float; // all floats are doubles
    }

    @Override
    public boolean isNumber() {
        return value instanceof Number;
    }

    @Override
    public boolean isBigInteger() {
        return value instanceof BigInteger || value instanceof Integer || value instanceof Long;
    }

    @Override
    public boolean isBigDecimal() {
        return value instanceof BigDecimal || value instanceof Float || value instanceof Double;
    }

    @Override
    public boolean isDocument() {
        return value instanceof Document;
    }

    @Override
    public boolean isArray() {
        return value instanceof Array;
    }

    @Override
    public boolean isBinary() {
        return false;
    }

    @Override
    public byte[] asBytes() {
        return null;
    }

    @Override
    public Value convert() {
        return new ConvertingValue(this);
    }

    @Override
    public Value clone() {
        if (isArray()) {
            return new ComparableValue(asArray().clone());
        }
        if (isDocument()) {
            return new ComparableValue(asDocument().clone());
        }
        // All other values are immutable ...
        return this;
    }

}
