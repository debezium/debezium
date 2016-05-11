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
     * Obtain a {@link ValueConverter} that passes through values.
     * @return the pass-through {@link ValueConverter}; never null
     */
    public static ValueConverter passthrough() {
        return (data)->data;
    }
}