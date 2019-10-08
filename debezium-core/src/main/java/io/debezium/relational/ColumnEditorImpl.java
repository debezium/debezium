/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.sql.Types;
import java.util.List;
import java.util.Optional;

final class ColumnEditorImpl implements ColumnEditor {

    private String name;
    private int jdbcType = Types.INTEGER;
    private int nativeType = Column.UNSET_INT_VALUE;
    private String typeName;
    private String typeExpression;
    private String charsetName;
    private String tableCharsetName;
    private int length = Column.UNSET_INT_VALUE;
    private Integer scale;
    private int position = 1;
    private boolean optional = true;
    private boolean autoIncremented = false;
    private boolean generated = false;
    private Object defaultValue = null;
    private boolean hasDefaultValue = false;
    private List<String> enumValues;

    protected ColumnEditorImpl() {
    }

    @Override
    public String name() {
        return name;
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
    public int jdbcType() {
        return jdbcType;
    }

    @Override
    public int nativeType() {
        return nativeType;
    }

    @Override
    public String charsetName() {
        return charsetName;
    }

    @Override
    public String charsetNameOfTable() {
        return tableCharsetName;
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
    public int position() {
        return position;
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
    public Object defaultValue() {
        return defaultValue;
    }

    @Override
    public boolean hasDefaultValue() {
        return hasDefaultValue;
    }

    @Override
    public ColumnEditorImpl name(String name) {
        this.name = name;
        return this;
    }

    @Override
    public List<String> enumValues() {
        return enumValues;
    }

    @Override
    public ColumnEditorImpl type(String typeName) {
        this.typeName = typeName;
        this.typeExpression = typeName;
        return this;
    }

    @Override
    public ColumnEditor type(String typeName, String typeExpression) {
        this.typeName = typeName;
        this.typeExpression = typeExpression != null ? typeExpression : typeName;
        return this;
    }

    @Override
    public ColumnEditorImpl jdbcType(int jdbcType) {
        this.jdbcType = jdbcType;
        return this;
    }

    @Override
    public ColumnEditorImpl nativeType(int nativeType) {
        this.nativeType = nativeType;
        return this;
    }

    @Override
    public ColumnEditor charsetName(String charsetName) {
        this.charsetName = charsetName;
        return this;
    }

    @Override
    public ColumnEditor charsetNameOfTable(String charsetName) {
        this.tableCharsetName = charsetName;
        return this;
    }

    @Override
    public ColumnEditorImpl length(int length) {
        assert length >= -1;
        this.length = length;
        return this;
    }

    @Override
    public ColumnEditorImpl scale(Integer scale) {
        this.scale = scale;
        return this;
    }

    @Override
    public ColumnEditorImpl optional(boolean optional) {
        this.optional = optional;
        if (optional && !hasDefaultValue()) {
            // Optional columns have implicit NULL default value
            defaultValue(null);
        }
        return this;
    }

    @Override
    public ColumnEditorImpl autoIncremented(boolean autoIncremented) {
        this.autoIncremented = autoIncremented;
        return this;
    }

    @Override
    public ColumnEditorImpl generated(boolean generated) {
        this.generated = generated;
        return this;
    }

    @Override
    public ColumnEditorImpl position(int position) {
        this.position = position;
        return this;
    }

    @Override
    public ColumnEditor defaultValue(final Object defaultValue) {
        this.hasDefaultValue = true;
        this.defaultValue = defaultValue;
        return this;
    }

    @Override
    public ColumnEditor unsetDefaultValue() {
        this.hasDefaultValue = false;
        this.defaultValue = null;
        return this;
    }

    @Override
    public ColumnEditor enumValues(List<String> enumValues) {
        this.enumValues = enumValues;
        return this;
    }

    @Override
    public Column create() {
        return new ColumnImpl(name, position, jdbcType, nativeType, typeName, typeExpression, charsetName, tableCharsetName,
                length, scale, enumValues, optional, autoIncremented, generated, defaultValue, hasDefaultValue);
    }

    @Override
    public String toString() {
        return create().toString();
    }
}
