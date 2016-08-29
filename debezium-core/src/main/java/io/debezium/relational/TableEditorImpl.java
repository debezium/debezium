/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

final class TableEditorImpl implements TableEditor {

    private TableId id;
    private LinkedHashMap<String, Column> sortedColumns = new LinkedHashMap<>();
    private final List<String> pkColumnNames = new ArrayList<>();
    private boolean uniqueValues = false;
    private String defaultCharsetName;

    protected TableEditorImpl() {
    }

    @Override
    public TableId tableId() {
        return id;
    }

    @Override
    public TableEditor tableId(TableId id) {
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
    public TableEditor addColumns(Column... columns) {
        for (Column column : columns) {
            add(column);
        }
        assert positionsAreValid();
        return this;
    }

    @Override
    public TableEditor addColumns(Iterable<Column> columns) {
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
    public TableEditor setColumns(Column... columns) {
        sortedColumns.clear();
        addColumns(columns);
        updatePrimaryKeys();
        assert positionsAreValid();
        return this;
    }

    @Override
    public TableEditor setColumns(Iterable<Column> columns) {
        sortedColumns.clear();
        addColumns(columns);
        updatePrimaryKeys();
        assert positionsAreValid();
        return this;
    }

    protected void updatePrimaryKeys() {
        Iterator<String> nameIter = primaryKeyColumnNames().iterator();
        while (nameIter.hasNext()) {
            String pkColumnName = nameIter.next();
            if (!hasColumnWithName(pkColumnName)) nameIter.remove();
        }
    }

    @Override
    public TableEditor setPrimaryKeyNames(String... pkColumnNames) {
        for (String pkColumnName : pkColumnNames) {
            if (!hasColumnWithName(pkColumnName)) {
                throw new IllegalArgumentException("The primary key cannot reference a non-existant column'" + pkColumnName + "'");
            }
        }
        uniqueValues = false;
        this.pkColumnNames.clear();
        for (String pkColumnName : pkColumnNames) {
            this.pkColumnNames.add(pkColumnName);
        }
        return this;
    }
    
    @Override
    public TableEditor setPrimaryKeyNames(List<String> pkColumnNames) {
        for (String pkColumnName : pkColumnNames) {
            if (!hasColumnWithName(pkColumnName)) {
                throw new IllegalArgumentException("The primary key cannot reference a non-existant column'" + pkColumnName + "'");
            }
        }
        this.pkColumnNames.clear();
        this.pkColumnNames.addAll(pkColumnNames);
        uniqueValues = false;
        return this;
    }

    @Override
    public TableEditor setUniqueValues() {
        pkColumnNames.clear();
        uniqueValues = true;
        return this;
    }
    
    @Override
    public boolean hasUniqueValues() {
        return uniqueValues;
    }
    
    @Override
    public TableEditor setDefaultCharsetName(String charsetName) {
        this.defaultCharsetName = charsetName;
        return this;
    }
    
    @Override
    public boolean hasDefaultCharsetName() {
        return this.defaultCharsetName != null && !this.defaultCharsetName.trim().isEmpty();
    }

    @Override
    public TableEditor removeColumn(String columnName) {
        Column existing = sortedColumns.remove(columnName.toLowerCase());
        if (existing != null) updatePositions();
        assert positionsAreValid();
        return this;
    }

    @Override
    public TableEditor reorderColumn(String columnName, String afterColumnName) {
        Column columnToMove = columnWithName(columnName);
        if (columnToMove == null) throw new IllegalArgumentException("No column with name '" + columnName + "'");
        Column afterColumn = afterColumnName == null ? null : columnWithName(afterColumnName);
        if (afterColumn != null && (afterColumn.position() + 1) == columnToMove.position()) {
            // nothing to do ...
        } else if ( afterColumn == null && columnToMove.position() == 1) {
            // nothing to do ...
        }
        if (afterColumn != null && afterColumn.position() == sortedColumns.size()) {
            // Just append ...
            sortedColumns.remove(columnName);
            sortedColumns.put(columnName, columnToMove);
        } else {
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
    public TableEditor renameColumn(String existingName, String newName) {
        final Column existing = columnWithName(existingName);
        if (existing == null) throw new IllegalArgumentException("No column with name '" + existingName + "'");
        Column newColumn = existing.edit().name(newName).create();
        // Determine the primary key names ...
        List<String> newPkNames = null;
        if ( !hasUniqueValues() && primaryKeyColumnNames().contains(existing.name())) {
            newPkNames = new ArrayList<>(primaryKeyColumnNames());
            newPkNames.replaceAll(name->existing.name().equals(name) ? newName : name);
        }
        // Add the new column, move it before the existing column, and remove the old column ...
        addColumn(newColumn);
        reorderColumn(newColumn.name(), existing.name());
        removeColumn(existing.name());
        if (newPkNames != null) {
            setPrimaryKeyNames(newPkNames);
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
        return sortedColumns.values().stream().allMatch(defn -> defn.position() == position.getAndIncrement());
    }

    @Override
    public String toString() {
        return create().toString();
    }

    @Override
    public Table create() {
        if (id == null) throw new IllegalStateException("Unable to create a table from an editor that has no table ID");
        List<Column> columns = new ArrayList<>();
        sortedColumns.values().forEach(column->{
            column = column.edit().charsetNameOfTable(defaultCharsetName).create();
            columns.add(column);
        });
        return new TableImpl(id, columns, primaryKeyColumnNames(), defaultCharsetName);
    }
}
