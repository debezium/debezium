/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

final class NoOpTableEditorImpl implements TableEditor {

    private TableId id;
    private boolean uniqueValues = false;
    private String defaultCharsetName;
    private String comment;

    protected NoOpTableEditorImpl() {
    }

    @Override
    public TableId tableId() {
        return id;
    }

    @Override
    public NoOpTableEditorImpl tableId(TableId id) {
        this.id = id;
        return this;
    }

    @Override
    public List<Column> columns() {
        return Collections.emptyList();
    }

    @Override
    public Column columnWithName(String name) {
        return null;
    }

    protected boolean hasColumnWithName(String name) {
        return false;
    }

    @Override
    public List<String> primaryKeyColumnNames() {
        return Collections.emptyList();
    }

    @Override
    public NoOpTableEditorImpl addColumns(Column... columns) {
        return this;
    }

    @Override
    public NoOpTableEditorImpl addColumns(Iterable<Column> columns) {
        return this;
    }

    @Override
    public NoOpTableEditorImpl setColumns(Column... columns) {
        return this;
    }

    @Override
    public NoOpTableEditorImpl setColumns(Iterable<Column> columns) {
        return this;
    }

    @Override
    public NoOpTableEditorImpl setPrimaryKeyNames(String... pkColumnNames) {
        return this;
    }

    @Override
    public NoOpTableEditorImpl setPrimaryKeyNames(List<String> pkColumnNames) {
        return this;
    }

    @Override
    public NoOpTableEditorImpl setUniqueValues() {
        this.uniqueValues = true;
        return this;
    }

    @Override
    public boolean hasUniqueValues() {
        return uniqueValues;
    }

    @Override
    public NoOpTableEditorImpl setDefaultCharsetName(String charsetName) {
        this.defaultCharsetName = charsetName;
        return this;
    }

    @Override
    public NoOpTableEditorImpl setComment(String comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public String defaultCharsetName() {
        return defaultCharsetName;
    }

    @Override
    public String comment() {
        return comment;
    }

    @Override
    public boolean hasDefaultCharsetName() {
        return this.defaultCharsetName != null && !this.defaultCharsetName.trim().isEmpty();
    }

    @Override
    public boolean hasComment() {
        return this.comment != null && !this.comment.trim().isEmpty();
    }

    @Override
    public NoOpTableEditorImpl removeColumn(String columnName) {
        return this;
    }

    @Override
    public NoOpTableEditorImpl updateColumn(Column column) {
        return this;
    }

    @Override
    public NoOpTableEditorImpl reorderColumn(String columnName, String afterColumnName) {
        return this;
    }

    @Override
    public NoOpTableEditorImpl renameColumn(String existingName, String newName) {
        return this;
    }

    @Override
    public List<Attribute> attributes() {
        return Collections.emptyList();
    }

    @Override
    public Attribute attributeWithName(String attributeName) {
        return null;
    }

    @Override
    public NoOpTableEditorImpl addAttribute(Attribute attribute) {
        return this;
    }

    @Override
    public NoOpTableEditorImpl addAttributes(List<Attribute> attributes) {
        return this;
    }

    @Override
    public NoOpTableEditorImpl removeAttribute(String attributeName) {
        return this;
    }

    @Override
    public String toString() {
        return create().toString();
    }

    @Override
    public Table create() {
        if (id == null) {
            throw new IllegalStateException("Unable to create a table from an editor that has no table ID");
        }
        List<Column> columns = new ArrayList<>();
        List<Attribute> attributes = new ArrayList<>();
        return new TableImpl(id, columns, primaryKeyColumnNames(), defaultCharsetName, comment, attributes);
    }
}
