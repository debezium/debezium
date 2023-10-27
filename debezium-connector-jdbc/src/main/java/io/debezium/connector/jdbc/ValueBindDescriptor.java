/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

public class ValueBindDescriptor {

    private final int index;
    private final Object value;

    private final Integer bindableType;

    public ValueBindDescriptor(int index, Object value) {
        this.index = index;
        this.value = value;
        this.bindableType = null;
    }

    public ValueBindDescriptor(int index, Object value, Integer bindableType) {
        this.index = index;
        this.value = value;
        this.bindableType = bindableType;
    }

    public int getIndex() {
        return index;
    }

    public Object getValue() {
        return value;
    }

    public Integer getBindableType() {
        return bindableType;
    }
}
