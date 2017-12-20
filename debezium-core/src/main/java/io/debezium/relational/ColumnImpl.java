/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.sql.Types;

import io.debezium.util.Strings;

final class ColumnImpl implements Column, Comparable<Column> {
    private final String name;
    private final int position;
    private final int jdbcType;
    private final int componentType;
    private final String typeName;
    private final String typeExpression;
    private final String charsetName;
    private final int length;
    private final int scale;
    private final boolean optional;
    private final boolean autoIncremented;
    private final boolean generated;

    protected ColumnImpl(String columnName, int position, int jdbcType, int componentType, String typeName, String typeExpression,
                         String charsetName, String defaultCharsetName, int columnLength, int columnScale,
                         boolean optional, boolean autoIncremented, boolean generated) {
        this.name = columnName;
        this.position = position;
        this.jdbcType = jdbcType;
        this.componentType = componentType;
        this.typeName = typeName;
        this.typeExpression = typeExpression;
        // We want to always capture the charset name for the column (if the column needs one) ...
        if ( typeUsesCharset() && (charsetName == null || "DEFAULT".equalsIgnoreCase(charsetName)) ) {
            // Use the default charset name ...
            charsetName = defaultCharsetName;
        }
        this.charsetName = charsetName;
        this.length = columnLength;
        this.scale = columnScale;
        this.optional = optional;
        this.autoIncremented = autoIncremented;
        this.generated = generated;
        assert this.scale >= -1;
        assert this.length >= -1;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public int position() {
        return position;
    }

    @Override
    public int jdbcType() {
        return jdbcType;
    }

    @Override
    public int componentType() {
        return componentType;
    }

    @Override
    public String typeName() {
        return typeName;
    }

    @Override
    public String typeExpression() {
        return typeExpression;
    }

    @Override
    public String charsetName() {
        return charsetName;
    }
    
    @Override
    public int length() {
        return length;
    }

    @Override
    public int scale() {
        return scale;
    }

    @Override
    public boolean isOptional() {
        return optional;
    }

    @Override
    public boolean isAutoIncremented() {
        return autoIncremented;
    }

    @Override
    public boolean isGenerated() {
        return generated;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj instanceof Column) {
            Column that = (Column) obj;
            return this.name().equalsIgnoreCase(that.name()) &&
                    this.typeExpression().equalsIgnoreCase(that.typeExpression()) &&
                    this.typeName().equalsIgnoreCase(that.typeName()) &&
                    this.jdbcType() == that.jdbcType() &&
                    Strings.equalsIgnoreCase(this.charsetName(),that.charsetName()) &&
                    this.position() == that.position() &&
                    this.length() == that.length() &&
                    this.scale() == that.scale() &&
                    this.isOptional() == that.isOptional() &&
                    this.isAutoIncremented() == that.isAutoIncremented() &&
                    this.isGenerated() == that.isGenerated();
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(name);
        sb.append(" ").append(typeName);
        if (length >= 0) {
            sb.append('(').append(length);
            if (scale >= 0) {
                sb.append(',').append(scale);
            }
            sb.append(')');
        }
        if (charsetName != null && !charsetName.isEmpty()) {
            sb.append(" CHARSET ").append(charsetName);
        }
        if (!optional) sb.append(" NOT NULL");
        if (autoIncremented) sb.append(" AUTO_INCREMENTED");
        if (generated) sb.append(" GENERATED");
        return sb.toString();
    }

    @Override
    public ColumnEditor edit() {
        final ColumnEditor editor = Column.editor()
                .name(name())
                .type(typeName(), typeExpression())
                .jdbcType(jdbcType())
                .charsetName(charsetName)
                .length(length())
                .scale(scale())
                .position(position())
                .optional(isOptional())
                .autoIncremented(isAutoIncremented())
                .generated(isGenerated());
        if (jdbcType() == Types.ARRAY) {
            editor.componentType(componentType());
        }
        return editor;
    }
}
