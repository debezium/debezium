/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.valuebinding;

public class ValueBindDescriptor {

    private final int index;
    private final Object value;

    private final Integer targetSqlType;

    private final String elementTypeName;

    public ValueBindDescriptor(int index, Object value) {
        this.index = index;
        this.value = value;
        this.targetSqlType = null;
        this.elementTypeName = null;
    }

    public ValueBindDescriptor(int index, Object value, Integer targetSqlType) {
        this.index = index;
        this.value = value;
        this.targetSqlType = targetSqlType;
        this.elementTypeName = null;
    }

    public ValueBindDescriptor(int index, Object value, Integer targetSqlType, String elementTypeName) {
        this.index = index;
        this.value = value;
        this.targetSqlType = targetSqlType;
        this.elementTypeName = elementTypeName;
    }

    public int getIndex() {
        return index;
    }

    public Object getValue() {
        return value;
    }

    public Integer getTargetSqlType() {
        return targetSqlType;
    }

    public String getElementTypeName() {
        return elementTypeName;
    }
}
