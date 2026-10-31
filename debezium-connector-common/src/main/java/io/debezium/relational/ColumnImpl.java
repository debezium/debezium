/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

import io.debezium.util.Interner;
import io.debezium.util.Strings;

/**
 * The default {@link Column} implementation.
 * <p>
 * A connector that needs to carry additional column metadata subclasses this class, adds its own fields,
 * and creates instances from its own {@link ColumnEditorImpl} subclass. Equality requires the same class, so
 * a ColumnImpl and a subclass instance are never equal in either direction, because {@link Tables} and
 * {@link Interner} rely on equality to detect a changed column and one must never stand in for the other.
 * A subclass that adds fields must override {@link #equals(Object)} to compare them after the base fields and
 * {@link #hashCode()} to combine the base hash code with the same fields. It should also override
 * {@link #edit()} and {@link #appendExtensions(StringBuilder)}.
 * The extra fields add metadata and never change identity, which stays with the column name.
 *
 * @author Randall Hauch
 */
public class ColumnImpl implements Column, Comparable<Column> {
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

    /**
     * Creates a column from the current state of the given editor. Subclasses call this constructor
     * and then copy their own fields from their own editor type.
     *
     * @param editor the editor whose state is copied; may not be null
     */
    protected ColumnImpl(ColumnEditor editor) {
        this.name = Interner.intern(editor.name());
        this.position = editor.position();
        this.jdbcType = editor.jdbcType();
        this.nativeType = editor.nativeType();
        this.typeName = Interner.intern(editor.typeName());
        this.typeExpression = Interner.intern(editor.typeExpression());
        // We want to always capture the charset name for the column (if the column needs one) ...
        String charsetName = editor.charsetName();
        if (typeUsesCharset() && (charsetName == null || "DEFAULT".equalsIgnoreCase(charsetName))) {
            // Use the default charset name ...
            charsetName = editor.charsetNameOfTable();
        }
        this.charsetName = Interner.intern(charsetName);
        this.length = editor.length();
        this.scale = editor.scale().orElse(null);
        this.optional = editor.isOptional();
        this.autoIncremented = editor.isAutoIncremented();
        this.generated = editor.isGenerated();
        this.defaultValueExpression = Interner.intern(editor.defaultValueExpression().orElse(null));
        this.hasDefaultValue = editor.hasDefaultValue();
        final List<String> enumValues = editor.enumValues();
        this.enumValues = enumValues == null ? new ArrayList<>() : Interner.intern(Collections.unmodifiableList(enumValues));
        this.comment = Interner.intern(editor.comment());
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
        return name.toLowerCase(Locale.ROOT).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        // Only the same class is equal, so equality stays symmetric with subclasses and an interned
        // ColumnImpl and a subclass instance never stand in for each other
        if (obj != null && obj.getClass() == getClass()) {
            final Column that = (Column) obj;
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
        appendExtensions(sb);
        return sb.toString();
    }

    /**
     * Appends any subclass-specific state to the {@link #toString()} output. The default appends nothing.
     *
     * @param sb the builder holding the base column rendering; never null
     */
    protected void appendExtensions(StringBuilder sb) {
    }

    @Override
    public ColumnEditor edit() {
        return copyTo(new ColumnEditorImpl());
    }

    /**
     * Copies the state of this column into the given editor and returns that editor. Subclasses call this
     * from their own {@link #edit()} with their own editor type and then set their additional fields.
     *
     * @param editor the editor to populate; may not be null
     * @return the given editor
     * @param <E> the editor type
     */
    protected <E extends ColumnEditor> E copyTo(E editor) {
        editor.name(name)
                .type(typeName, typeExpression)
                .jdbcType(jdbcType)
                .nativeType(nativeType)
                .charsetName(charsetName)
                .length(length)
                .scale(scale)
                .position(position)
                .optional(optional)
                .autoIncremented(autoIncremented)
                .generated(generated)
                .enumValues(enumValues)
                .comment(comment);
        if (hasDefaultValue) {
            editor.defaultValueExpression(defaultValueExpression);
        }
        return editor;
    }
}
