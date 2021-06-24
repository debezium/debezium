/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.document;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

import io.debezium.annotation.Immutable;

/**
 * A value in a {@link Document} or {@link Array}. Note that {@link Value#compareTo} might perform literal comparisons;
 * to perform semantic comparisons, use {@link #comparable()} to obtain a wrapped value with semantic comparison capability.
 *
 * @author Randall Hauch
 */
@Immutable
public interface Value extends Comparable<Value> {

    static enum Type {
        NULL,
        STRING,
        BOOLEAN,
        BINARY,
        INTEGER,
        LONG,
        FLOAT,
        DOUBLE,
        BIG_INTEGER,
        DECIMAL,
        DOCUMENT,
        ARRAY;
    }

    static boolean isNull(Value value) {
        return value == null || value.isNull();
    }

    static boolean notNull(Value value) {
        return value != null && !value.isNull();
    }

    static boolean isValid(Object value) {
        return value == null || value instanceof Value ||
                value instanceof String || value instanceof Boolean ||
                value instanceof Integer || value instanceof Long ||
                value instanceof Float || value instanceof Double ||
                value instanceof Document || value instanceof Array ||
                value instanceof BigInteger || value instanceof BigDecimal;
    }

    /**
     * Compare two {@link Value} objects, which may or may not be null.
     *
     * @param value1 the first value object, may be null
     * @param value2 the second value object, which may be null
     * @return a negative integer if the first value is less than the second, zero if the values are equivalent (including if both
     *         are null), or a positive integer if the first value is greater than the second
     */
    static int compareTo(Value value1, Value value2) {
        if (value1 == null) {
            return isNull(value2) ? 0 : -1;
        }
        return value1.compareTo(value2);
    }

    static Value create(Object value) {
        if (value instanceof Value) {
            return (Value) value;
        }
        if (!isValid(value)) {
            assert value != null;
            throw new IllegalArgumentException("Unexpected value " + value + "' of type " + value.getClass());
        }
        return value == null ? NullValue.INSTANCE : new ComparableValue((Comparable<?>) value);
    }

    static Value create(boolean value) {
        return new ComparableValue(Boolean.valueOf(value));
    }

    static Value create(int value) {
        return new ComparableValue(Integer.valueOf(value));
    }

    static Value create(long value) {
        return new ComparableValue(Long.valueOf(value));
    }

    static Value create(float value) {
        return new ComparableValue(Float.valueOf(value));
    }

    static Value create(double value) {
        return new ComparableValue(Double.valueOf(value));
    }

    static Value create(BigInteger value) {
        return value == null ? NullValue.INSTANCE : new ComparableValue(value);
    }

    static Value create(BigDecimal value) {
        return value == null ? NullValue.INSTANCE : new ComparableValue(value);
    }

    static Value create(Integer value) {
        return value == null ? NullValue.INSTANCE : new ComparableValue(value);
    }

    static Value create(Long value) {
        return value == null ? NullValue.INSTANCE : new ComparableValue(value);
    }

    static Value create(Float value) {
        return value == null ? NullValue.INSTANCE : new ComparableValue(value);
    }

    static Value create(Double value) {
        return value == null ? NullValue.INSTANCE : new ComparableValue(value);
    }

    static Value create(String value) {
        return value == null ? NullValue.INSTANCE : new ComparableValue(value);
    }

    static Value create(byte[] value) {
        return value == null ? NullValue.INSTANCE : new BinaryValue(value);
    }

    static Value create(Document value) {
        return value == null ? NullValue.INSTANCE : new ComparableValue(value);
    }

    static Value create(Array value) {
        return value == null ? NullValue.INSTANCE : new ComparableValue(value);
    }

    static Value nullValue() {
        return NullValue.INSTANCE;
    }

    default Type getType() {
        return ComparableValue.typeForValue(this);
    }

    /**
     * Get the raw value.
     *
     * @return the raw value; may be null
     */
    Object asObject();

    String asString();

    Integer asInteger();

    Long asLong();

    Boolean asBoolean();

    Number asNumber();

    BigInteger asBigInteger();

    BigDecimal asBigDecimal();

    Float asFloat();

    Double asDouble();

    Document asDocument();

    Array asArray();

    byte[] asBytes();

    boolean isNull();

    default boolean isNotNull() {
        return !isNull();
    }

    boolean isString();

    boolean isInteger();

    boolean isLong();

    boolean isBoolean();

    boolean isNumber();

    boolean isBigInteger();

    boolean isBigDecimal();

    boolean isFloat();

    boolean isDouble();

    boolean isDocument();

    boolean isArray();

    boolean isBinary();

    /**
     * Get a Value representation that will convert attempt to convert values.
     *
     * @return a value that can convert actual values to the requested format
     */
    Value convert();

    /**
     * Get a Value representation that will allow semantic comparison of values, rather than the literal comparison normally
     * performed by {@link #compareTo}.
     *
     * @return the Value that will perform semantic comparisons; never null
     */
    default Value comparable() {
        if (this instanceof ComparableValue) {
            return this;
        }
        return new ComparableValue(this);
    }

    /**
     * Obtain a clone of this value.
     *
     * @return the clone of this value; never null, but possibly the same instance if the underlying value is immutable
     *         and not a document or array
     */
    Value clone();

    /**
     * If a value is a document, invoke the specified consumer with the value, otherwise do nothing.
     *
     * @param consumer block to be executed if the value is a document
     * @return true if the block was called, or false otherwise
     */
    default boolean ifDocument(Consumer<Document> consumer) {
        if (isDocument()) {
            consumer.accept(asDocument());
            return true;
        }
        return false;
    }

    /**
     * If a value is an array, invoke the specified consumer with the value, otherwise do nothing.
     *
     * @param consumer block to be executed if the value is an array
     * @return true if the block was called, or false otherwise
     */
    default boolean ifArray(Consumer<Array> consumer) {
        if (isArray()) {
            consumer.accept(asArray());
            return true;
        }
        return false;
    }

    /**
     * If a value is a string, invoke the specified consumer with the value, otherwise do nothing.
     *
     * @param consumer block to be executed if the value is a string
     * @return true if the block was called, or false otherwise
     */
    default boolean ifString(Consumer<String> consumer) {
        if (isString()) {
            consumer.accept(asString());
            return true;
        }
        return false;
    }

    /**
     * If a value is a boolean value, invoke the specified consumer with the value, otherwise do nothing.
     *
     * @param consumer block to be executed if the value is a boolean
     * @return true if the block was called, or false otherwise
     */
    default boolean ifBoolean(Consumer<Boolean> consumer) {
        if (isBoolean()) {
            consumer.accept(asBoolean());
            return true;
        }
        return false;
    }

    /**
     * If a value is a byte array, invoke the specified consumer with the value, otherwise do nothing.
     *
     * @param consumer block to be executed if the value is a byte array
     * @return true if the block was called, or false otherwise
     */
    default boolean ifBinary(Consumer<byte[]> consumer) {
        if (isBinary()) {
            consumer.accept(asBytes());
            return true;
        }
        return false;
    }

    /**
     * If a value is an integer, invoke the specified consumer with the value, otherwise do nothing.
     *
     * @param consumer block to be executed if the value is an integer
     * @return true if the block was called, or false otherwise
     */
    default boolean ifInteger(IntConsumer consumer) {
        if (isInteger()) {
            consumer.accept(asInteger().intValue());
            return true;
        }
        return false;
    }

    /**
     * If a value is a long, invoke the specified consumer with the value, otherwise do nothing.
     *
     * @param consumer block to be executed if the value is a long
     * @return true if the block was called, or false otherwise
     */
    default boolean ifLong(LongConsumer consumer) {
        if (isLong()) {
            consumer.accept(asLong().longValue());
            return true;
        }
        return false;
    }

    /**
     * If a value is a float, invoke the specified consumer with the value, otherwise do nothing.
     *
     * @param consumer block to be executed if the value is a float
     * @return true if the block was called, or false otherwise
     */
    default boolean ifFloat(DoubleConsumer consumer) {
        if (isFloat()) {
            consumer.accept(asFloat().doubleValue());
            return true;
        }
        return false;
    }

    /**
     * If a value is a double, invoke the specified consumer with the value, otherwise do nothing.
     *
     * @param consumer block to be executed if the value is a double
     * @return true if the block was called, or false otherwise
     */
    default boolean ifDouble(DoubleConsumer consumer) {
        if (isDouble()) {
            consumer.accept(asDouble().intValue());
            return true;
        }
        return false;
    }

    /**
     * If a value is a variable-sized integer, invoke the specified consumer with the value, otherwise do nothing.
     *
     * @param consumer block to be executed if the value is a big integer
     * @return true if the block was called, or false otherwise
     */
    default boolean ifBigInteger(Consumer<BigInteger> consumer) {
        if (isBigInteger()) {
            consumer.accept(asBigInteger());
            return true;
        }
        return false;
    }

    /**
     * If a value is a variable-sized decimal, invoke the specified consumer with the value, otherwise do nothing.
     *
     * @param consumer block to be executed if the value is a big decimal
     * @return true if the block was called, or false otherwise
     */
    default boolean ifBigDecimal(Consumer<BigDecimal> consumer) {
        if (isBigDecimal()) {
            consumer.accept(asBigDecimal());
            return true;
        }
        return false;
    }

    /**
     * If a value is a variable-sized integer, invoke the specified consumer with the value, otherwise do nothing.
     *
     * @param consumer block to be executed if the value is a big integer
     * @return true if the block was called, or false otherwise
     */
    default boolean ifNull(NullHandler consumer) {
        if (isNull()) {
            consumer.call();
            return true;
        }
        return false;
    }

    @FunctionalInterface
    static interface NullHandler {
        void call();
    }

}
