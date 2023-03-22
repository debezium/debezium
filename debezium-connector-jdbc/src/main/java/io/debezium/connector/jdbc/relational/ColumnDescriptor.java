/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.relational;

import java.util.Objects;

import io.debezium.annotation.Immutable;

/**
 * Describes a relational column in a relational table.
 *
 * @author Chris Cranford
 */
@Immutable
public class ColumnDescriptor {

    private final String columnName;
    private final int jdbcType;
    private final String typeName;
    private final int precision;
    private final int scale;
    private final Nullability nullability;
    private final boolean autoIncrement;
    private final boolean primaryKey;

    private ColumnDescriptor(String columnName, int jdbcType, String typeName, int precision, int scale,
                             Nullability nullability, boolean autoIncrement, boolean primaryKey) {
        this.columnName = columnName;
        this.jdbcType = jdbcType;
        this.typeName = typeName;
        this.precision = precision;
        this.scale = scale;
        this.nullability = nullability;
        this.autoIncrement = autoIncrement;
        this.primaryKey = primaryKey;
    }

    public String getColumnName() {
        return columnName;
    }

    public int getJdbcType() {
        return jdbcType;
    }

    public String getTypeName() {
        return typeName;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public Nullability getNullability() {
        return nullability;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String columnName;
        private int jdbcType;
        private String typeName;
        private int precision;
        private int scale;
        private Nullability nullability;
        private boolean autoIncrement;
        private boolean primaryKey;

        private Builder() {
            this.nullability = Nullability.NULL; // By default a column is nullable
        }

        public Builder columnName(String columnName) {
            this.columnName = columnName;
            return this;
        }

        public Builder jdbcType(int jdbcType) {
            this.jdbcType = jdbcType;
            return this;
        }

        public Builder typeName(String typeName) {
            this.typeName = typeName;
            return this;
        }

        public Builder precision(int precision) {
            this.precision = precision;
            return this;
        }

        public Builder scale(int scale) {
            this.scale = scale;
            return this;
        }

        public Builder nullable(boolean nullable) {
            this.nullability = nullable ? Nullability.NULL : Nullability.NOT_NULL;
            return this;
        }

        public Builder autoIncrement(boolean autoIncrement) {
            this.autoIncrement = autoIncrement;
            return this;
        }

        public Builder primarykey(boolean primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }

        public ColumnDescriptor build() {
            Objects.requireNonNull(columnName, "A column name is required");
            Objects.requireNonNull(typeName, "A type name is required");

            return new ColumnDescriptor(columnName, jdbcType, typeName, precision, scale, nullability, autoIncrement, primaryKey);
        }
    }

    public enum Nullability {
        NULL,
        NOT_NULL;
    }
}
