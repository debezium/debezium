/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.debezium.annotation.PackagePrivate;
import io.debezium.util.Strings;

final class TableImpl implements Table {

    private final TableId id;
    private final List<Column> columnDefs;
    private final List<String> pkColumnNames;
    private final Map<String, Column> columnsByLowercaseName;
    private final String defaultCharsetName;

    @PackagePrivate
    TableImpl(Table table) {
        this(table.id(), table.columns(), table.primaryKeyColumnNames(), table.defaultCharsetName());
    }

    @PackagePrivate
    TableImpl(TableId id, List<Column> sortedColumns, List<String> pkColumnNames, String defaultCharsetName) {
        this.id = id;
        this.columnDefs = Collections.unmodifiableList(sortedColumns);
        this.pkColumnNames = pkColumnNames == null ? Collections.emptyList() : Collections.unmodifiableList(pkColumnNames);
        Map<String, Column> defsByLowercaseName = new LinkedHashMap<>();
        for (Column def : this.columnDefs) {
            defsByLowercaseName.put(def.name().toLowerCase(), def);
        }
        this.columnsByLowercaseName = Collections.unmodifiableMap(defsByLowercaseName);
        this.defaultCharsetName = defaultCharsetName;
    }

    @Override
    public TableId id() {
        return id;
    }

    @Override
    public List<String> primaryKeyColumnNames() {
        return pkColumnNames;
    }

    @Override
    public List<Column> columns() {
        return columnDefs;
    }

    @Override
    public List<String> retrieveColumnNames() {
        return columnDefs.stream()
                .map(Column::name)
                .collect(Collectors.toList());
    }

    @Override
    public Column columnWithName(String name) {
        return columnsByLowercaseName.get(name.toLowerCase());
    }

    @Override
    public String defaultCharsetName() {
        return defaultCharsetName;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof Table) {
            Table that = (Table) obj;
            return this.id().equals(that.id())
                    && this.columns().equals(that.columns())
                    && this.primaryKeyColumnNames().equals(that.primaryKeyColumnNames())
                    && Strings.equalsIgnoreCase(this.defaultCharsetName(), that.defaultCharsetName());
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        toString(sb, "");
        return sb.toString();
    }

    public void toString(StringBuilder sb, String prefix) {
        if (prefix == null) {
            prefix = "";
        }
        sb.append(prefix).append("columns: {").append(System.lineSeparator());
        for (Column defn : columnDefs) {
            sb.append(prefix).append("  ").append(defn).append(System.lineSeparator());
        }
        sb.append(prefix).append("}").append(System.lineSeparator());
        sb.append(prefix).append("primary key: ").append(primaryKeyColumnNames()).append(System.lineSeparator());
        sb.append(prefix).append("default charset: ").append(defaultCharsetName()).append(System.lineSeparator());
    }

    @Override
    public TableEditor edit() {
        return new TableEditorImpl().tableId(id)
                .setColumns(columnDefs)
                .setPrimaryKeyNames(pkColumnNames)
                .setDefaultCharsetName(defaultCharsetName);
    }
}
