/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.debezium.util.Strings;

final class ColumnImpl implements Column, Comparable<Column> {
    private final String name;
    private final int position;
    private final int jdbcType;
    private final int nativeType;
    private final String typeName;
    private final String typeExpression;
    private final String charsetName;
    private final int length;
    private final Integer scale;
    private final boolean optional;
    private final boolean autoIncremented;
    private final boolean generated;
    private final String defaultValueExpression;
    private final boolean hasDefaultValue;
    private final List<String> enumValues;
    private final String comment;

    protected ColumnImpl(String columnName, int position, int jdbcType, int componentType, String typeName, String typeExpression,
                         String charsetName, String defaultCharsetName, int columnLength, Integer columnScale,
                         boolean optional, boolean autoIncremented, boolean generated) {
        this(columnName, position, jdbcType, componentType, typeName, typeExpression, charsetName,
                defaultCharsetName, columnLength, columnScale, null, optional, autoIncremented, generated, null, false, null);
    }

    protected ColumnImpl(String columnName, int position, int jdbcType, int nativeType, String typeName, String typeExpression,
                         String charsetName, String defaultCharsetName, int columnLength, Integer columnScale,
                         boolean optional, boolean autoIncremented, boolean generated, String defaultValueExpression, boolean hasDefaultValue) {
        this(columnName, position, jdbcType, nativeType, typeName, typeExpression, charsetName,
                defaultCharsetName, columnLength, columnScale, null, optional, autoIncremented, generated, defaultValueExpression, hasDefaultValue, null);
    }

    protected ColumnImpl(String columnName, int position, int jdbcType, int nativeType, String typeName, String typeExpression,
                         String charsetName, String defaultCharsetName, int columnLength, Integer columnScale,
                         List<String> enumValues, boolean optional, boolean autoIncremented, boolean generated,
                         String defaultValueExpression, boolean hasDefaultValue, String comment) {
        this.name = columnName;
        this.position = position;
        this.jdbcType = jdbcType;
        this.nativeType = nativeType;
        this.typeName = typeName;
        this.typeExpression = typeExpression;
        // We want to always capture the charset name for the column (if the column needs one) ...
        if (typeUsesCharset() && (charsetName == null || "DEFAULT".equalsIgnoreCase(charsetName))) {
            // Use the default charset name ...
            charsetName = defaultCharsetName;
        }
        this.charsetName = charsetName;
        this.length = columnLength;
        this.scale = columnScale;
        this.optional = optional;
        this.autoIncremented = autoIncremented;
        this.generated = generated;
        this.defaultValueExpression = defaultValueExpression;
        this.hasDefaultValue = hasDefaultValue;
        this.enumValues = enumValues == null ? new ArrayList<>() : enumValues;
        this.comment = comment;
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
    public int nativeType() {
        return nativeType;
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
    public Optional<Integer> scale() {
        return Optional.ofNullable(scale);
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
    public Optional<String> defaultValueExpression() {
        return Optional.ofNullable(defaultValueExpression);
    }

    @Override
    public boolean hasDefaultValue() {
        return hasDefaultValue;
    }

    @Override
    public List<String> enumValues() {
        return enumValues;
    }

    @Override
    public String comment() {
        return comment;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof Column) {
            Column that = (Column) obj;
            return this.name().equalsIgnoreCase(that.name()) &&
                    Strings.equalsIgnoreCase(this.typeExpression(), that.typeExpression()) &&
                    this.typeName().equalsIgnoreCase(that.typeName()) &&
                    this.jdbcType() == that.jdbcType() &&
                    Strings.equalsIgnoreCase(this.charsetName(), that.charsetName()) &&
                    this.position() == that.position() &&
                    this.length() == that.length() &&
                    this.scale().equals(that.scale()) &&
                    this.isOptional() == that.isOptional() &&
                    this.isAutoIncremented() == that.isAutoIncremented() &&
                    this.isGenerated() == that.isGenerated() &&
                    Objects.equals(this.defaultValueExpression(), that.defaultValueExpression()) &&
                    this.hasDefaultValue() == that.hasDefaultValue() &&
                    this.enumValues().equals(that.enumValues());
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(name);
        sb.append(" ").append(typeName);
        if (length >= 0) {
            sb.append('(').append(length);
            if (scale != null) {
                sb.append(", ").append(scale);
            }
            sb.append(')');
        }
        if (charsetName != null && !charsetName.isEmpty()) {
            sb.append(" CHARSET ").append(charsetName);
        }
        if (!optional) {
            sb.append(" NOT NULL");
        }
        if (autoIncremented) {
            sb.append(" AUTO_INCREMENTED");
        }
        if (generated) {
            sb.append(" GENERATED");
        }
        if (hasDefaultValue() && defaultValueExpression == null) {
            sb.append(" DEFAULT VALUE NULL");
        }
        else if (defaultValueExpression != null) {
            sb.append(" DEFAULT VALUE ").append(defaultValueExpression);
        }
        if (comment != null) {
            sb.append(" COMMENT '").append(comment).append("'");
        }
        return sb.toString();
    }

    @Override
    public ColumnEditor edit() {
        final ColumnEditor editor = Column.editor()
                .name(name())
                .type(typeName(), typeExpression())
                .jdbcType(jdbcType())
                .nativeType(nativeType)
                .charsetName(charsetName)
                .length(length())
                .scale(scale().orElse(null))
                .position(position())
                .optional(isOptional())
                .autoIncremented(isAutoIncremented())
                .generated(isGenerated())
                .enumValues(enumValues)
                .comment(comment);
        if (hasDefaultValue()) {
            editor.defaultValueExpression(defaultValueExpression().orElse(null));
        }
        return editor;
    }
}
