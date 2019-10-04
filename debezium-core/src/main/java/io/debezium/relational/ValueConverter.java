/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

/**
 * A function that converts from a column data value into another value.
 */
@FunctionalInterface
public interface ValueConverter {

    /**
     * Convert the column's data value.
     *
     * @param data the column data value
     * @return the new data value
     */
    Object convert(Object data);

    /**
     * Adapt this converter to call the specified <em>fallback</em> converter when this converter returns {@code null} for
     * a non-null input.
     *
     * @param fallback the converter to fall back to when this converter returns {@code null} for a non-null input
     * @return the new converter, or this converter if {@code fallback} is {@code null}
     */
    default ValueConverter or(ValueConverter fallback) {
        if (fallback == null) {
            return this;
        }
        return (data) -> {
            Object result = convert(data);
            return result != null || data == null ? result : fallback.convert(data);
        };
    }

    /**
     * Return a new converter that calls this converter and passes the result to the specified converter.
     *
     * @param delegate the converter to call after this converter
     * @return the new converter, or this converter if {@code delegate} is {@code null}
     */
    default ValueConverter and(ValueConverter delegate) {
        if (delegate == null) {
            return this;
        }
        return (data) -> {
            return delegate.convert(convert(data));
        };
    }

    /**
     * Return a new converter that will call this converter only when the input is not {@code null}.
     *
     * @return the new converter; never null
     */
    default ValueConverter nullOr() {
        return (data) -> {
            if (data == null) {
                return null;
            }
            return convert(data);
        };
    }

    /**
     * Obtain a {@link ValueConverter} that passes through values.
     *
     * @return the pass-through {@link ValueConverter}; never null
     */
    public static ValueConverter passthrough() {
        return (data) -> data;
    }
}
