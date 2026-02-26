/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.ddl;

import java.sql.Types;
import java.util.Arrays;

import io.debezium.annotation.Immutable;

/**
 * An immutable representation of a data type
 *
 * @author Randall Hauch
 */
@Immutable
public final class DataType {

    /**
     * Obtain the data type for a user-defined or fully-qualified type name.
     *
     * @param qualifiedName the fully-qualified name; may not be null
     * @return the data type; never null
     */
    public static DataType userDefinedType(String qualifiedName) {
        return new DataType(qualifiedName, qualifiedName, Types.OTHER, -1, -1, null, 0);
    }

    private final String expression;
    private final String name;
    private final int jdbcType;
    private final long length;
    private final int scale;
    private final int[] arrayDimensions;

    protected DataType(String expr, String name, int jdbcType, long length, int scale, int[] arrayDimensions, int arrayDimLength) {
        this.expression = expr;
        this.name = name;
        this.jdbcType = jdbcType;
        this.length = length;
        this.scale = scale;
        if (arrayDimensions == null || arrayDimLength == 0) {
            this.arrayDimensions = null;
        }
        else {
            this.arrayDimensions = Arrays.copyOf(arrayDimensions, arrayDimLength);
        }
    }

    public String expression() {
        return expression;
    }

    public String name() {
        return name;
    }

    public int jdbcType() {
        return jdbcType;
    }

    public long length() {
        return length;
    }

    public int scale() {
        return scale;
    }

    public int[] arrayDimensions() {
        return arrayDimensions;
    }

    @Override
    public String toString() {
        return expression;
    }
}
