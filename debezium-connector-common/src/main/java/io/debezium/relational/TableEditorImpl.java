/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import io.debezium.util.Interner;

/**
 * The default {@link TableEditor}, which creates {@link TableImpl} instances.
 * <p>
 * A connector that needs to carry additional table metadata subclasses this class, adds its own
 * setters, and overrides {@link #create()} to return its own {@link TableImpl} subclass, calling
 * {@link #checkCreatable()} first.
 *
 * @author Randall Hauch
 */
public class TableEditorImpl implements TableEditor {

    private TableId id;
    private LinkedHashMap<String, Column> sortedColumns = new LinkedHashMap<>();
    private final List<String> pkColumnNames = new ArrayList<>();
    private boolean uniqueValues = false;
    private String defaultCharsetName;
    private String comment;
    private LinkedHashMap<String, Attribute> attributes = new LinkedHashMap<>();

    protected TableEditorImpl() {
    }

    /**
     * Verifies that a table can be created from the current state of this editor.
     *
     * @throws IllegalStateException if no table identifier has been set
     * @throws IllegalArgumentException if a primary key column name does not match a column
     */
    protected void checkCreatable() {
        if (id == null) {
            throw new IllegalStateException("Unable to create a table from an editor that has no table ID");
        }
        updatePrimaryKeys();
    }

    @Override
    public TableId tableId() {
        return id;
    }

    @Override
    public TableEditorImpl tableId(TableId id) {
        this.id = id;
        return this;
    }

    @Override
    public List<Column> columns() {
        return Collections.unmodifiableList(new ArrayList<>(sortedColumns.values()));
    }

    @Override
    public Column columnWithName(String name) {
        return sortedColumns.get(name.toLowerCase());
    }

    protected boolean hasColumnWithName(String name) {
        return columnWithName(name) != null;
    }

    @Override
    public List<String> primaryKeyColumnNames() {
        return uniqueValues ? columnNames() : Collections.unmodifiableList(pkColumnNames);
    }

    @Override
    public TableEditorImpl addColumns(Column... columns) {
        for (Column column : columns) {
            add(column);
        }
        assert positionsAreValid();
        return this;
    }

    @Override
    public TableEditorImpl addColumns(Iterable<Column> columns) {
        columns.forEach(this::add);
        assert positionsAreValid();
        return this;
    }

    protected void add(Column defn) {
        if (defn != null) {
            Column existing = columnWithName(defn.name());
            int position = existing != null ? existing.position() : sortedColumns.size() + 1;
            sortedColumns.put(defn.name().toLowerCase(), defn.edit().position(position).create());
        }
        assert positionsAreValid();
    }

    @Override
    public TableEditorImpl setColumns(Column... columns) {
        sortedColumns.clear();
        addColumns(columns);
        assert positionsAreValid();
        return this;
    }

    @Override
    public TableEditorImpl setColumns(Iterable<Column> columns) {
        sortedColumns.clear();
        addColumns(columns);
        assert positionsAreValid();
        return this;
    }

    protected void updatePrimaryKeys() {
        if (!uniqueValues) {
            // table does have any primary key --> we need to remove it
            this.pkColumnNames.removeIf(pkColumnName -> {
                final boolean pkColumnDoesNotExists = !hasColumnWithName(pkColumnName);
                if (pkColumnDoesNotExists) {
                    throw new IllegalArgumentException(
                            "The column \"" + pkColumnName + "\" is referenced as PRIMARY KEY, but a matching column is not defined in table \"" + tableId() + "\"!");
                }
                return pkColumnDoesNotExists;
            });
        }
    }

    @Override
    public TableEditorImpl setPrimaryKeyNames(String... pkColumnNames) {
        return setPrimaryKeyNames(Arrays.asList(pkColumnNames));
    }

    @Override
    public TableEditorImpl setPrimaryKeyNames(List<String> pkColumnNames) {
        this.pkColumnNames.clear();
        this.pkColumnNames.addAll(pkColumnNames);
        uniqueValues = false;
        return this;
    }

    @Override
    public TableEditorImpl setUniqueValues() {
        pkColumnNames.clear();
        uniqueValues = true;
        return this;
    }

    @Override
    public boolean hasUniqueValues() {
        return uniqueValues;
    }

    @Override
    public TableEditorImpl setDefaultCharsetName(String charsetName) {
        this.defaultCharsetName = charsetName;
        return this;
    }

    @Override
    public TableEditorImpl setComment(String comment) {
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
    public TableEditorImpl removeColumn(String columnName) {
        Column existing = sortedColumns.remove(columnName.toLowerCase());
        if (existing != null) {
            updatePositions();
            columnName = existing.name();
        }
        assert positionsAreValid();
        pkColumnNames.remove(columnName);
        return this;
    }

    @Override
    public TableEditorImpl updateColumn(Column newColumn) {
        setColumns(columns().stream()
                .map(c -> c.name().equals(newColumn.name()) ? newColumn : c)
                .collect(Collectors.toList()));
        return this;
    }

    @Override
    public TableEditorImpl reorderColumn(String columnName, String afterColumnName) {
        Column columnToMove = columnWithName(columnName);
        if (columnToMove == null) {
            throw new IllegalArgumentException("No column with name '" + columnName + "'");
        }
        Column afterColumn = afterColumnName == null ? null : columnWithName(afterColumnName);
        if (afterColumn != null && afterColumn.position() == sortedColumns.size()) {
            // Just append ...
            sortedColumns.remove(columnName);
            sortedColumns.put(columnName, columnToMove);
        }
        else {
            LinkedHashMap<String, Column> newColumns = new LinkedHashMap<>();
            sortedColumns.remove(columnName.toLowerCase());
            if (afterColumn == null) {
                // Then the column to move comes first ...
                newColumns.put(columnToMove.name().toLowerCase(), columnToMove);
            }
            sortedColumns.forEach((key, defn) -> {
                newColumns.put(key, defn);
                if (defn == afterColumn) {
                    // We just added the column that came before, so add our column here ...
                    newColumns.put(columnToMove.name().toLowerCase(), columnToMove);
                }
            });
            sortedColumns = newColumns;
        }
        updatePositions();
        return this;
    }

    @Override
    public TableEditorImpl renameColumn(String existingName, String newName) {
        final Column existing = columnWithName(existingName);
        if (existing == null) {
            throw new IllegalArgumentException("No column with name '" + existingName + "'");
        }
        Column newColumn = existing.edit().name(newName).create();
        // Determine the primary key names ...
        List<String> newPkNames = null;
        if (!hasUniqueValues() && primaryKeyColumnNames().contains(existing.name())) {
            newPkNames = new ArrayList<>(primaryKeyColumnNames());
            newPkNames.replaceAll(name -> existing.name().equals(name) ? newName : name);
        }
        // Add the new column, move it before the existing column, and remove the old column ...
        if (!existingName.equalsIgnoreCase(newName)) {
            addColumn(newColumn);
            reorderColumn(newColumn.name(), existing.name());
            removeColumn(existing.name());
        }
        else {
            sortedColumns.replace(existingName.toLowerCase(), existing, newColumn);
        }
        if (newPkNames != null) {
            setPrimaryKeyNames(newPkNames);
        }
        return this;
    }

    @Override
    public List<Attribute> attributes() {
        return Collections.unmodifiableList(new ArrayList<>(attributes.values()));
    }

    @Override
    public Attribute attributeWithName(String attributeName) {
        return attributes.get(attributeName.toLowerCase());
    }

    @Override
    public TableEditorImpl addAttribute(Attribute attribute) {
        if (attribute != null) {
            attributes.put(attribute.name().toLowerCase(), attribute);
        }
        return this;
    }

    @Override
    public TableEditorImpl addAttributes(List<Attribute> attributes) {
        for (Attribute attribute : attributes) {
            addAttribute(attribute);
        }
        return this;
    }

    @Override
    public TableEditorImpl removeAttribute(String attributeName) {
        if (attributeName != null) {
            attributes.remove(attributeName.toLowerCase());
        }
        return this;
    }

    protected void updatePositions() {
        AtomicInteger position = new AtomicInteger(1);
        sortedColumns.replaceAll((name, defn) -> {
            // Decrement the position ...
            int nextPosition = position.getAndIncrement();
            if (defn.position() != nextPosition) {
                return defn.edit().position(nextPosition).create();
            }
            return defn;
        });
    }

    protected boolean positionsAreValid() {
        AtomicInteger position = new AtomicInteger(1);
        return sortedColumns.values().stream().allMatch(defn -> defn.position() >= position.getAndSet(defn.position() + 1));
    }

    @Override
    public String toString() {
        return create().toString();
    }

    @Override
    public Table create() {
        checkCreatable();
        return Interner.intern(new TableImpl(this));
    }
}
