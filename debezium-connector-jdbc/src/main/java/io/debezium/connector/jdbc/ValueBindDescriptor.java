/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

public class ValueBindDescriptor {

    private final int index;
    private final Object value;

    private final Integer targetSqlType;

    private final String subTypeName;

    public ValueBindDescriptor(int index, Object value) {
        this.index = index;
        this.value = value;
        this.targetSqlType = null;
        this.subTypeName = null;
    }

    public ValueBindDescriptor(int index, Object value, Integer targetSqlType) {
        this.index = index;
        this.value = value;
        this.targetSqlType = targetSqlType;
        this.subTypeName = null;
    }

    public ValueBindDescriptor(int index, Object value, Integer targetSqlType, String subTypeName) {
        this.index = index;
        this.value = value;
        this.targetSqlType = targetSqlType;
        this.subTypeName = subTypeName;
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

    public String getSubTypeName() {
        return subTypeName;
    }
}
