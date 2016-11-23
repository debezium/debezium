/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.kafka.connect.data.Schema;

import io.debezium.annotation.ThreadSafe;
import io.debezium.util.Collect;
import io.debezium.util.FunctionalReadWriteLock;

/**
 * Structural definitions for a set of tables in a JDBC database.
 * 
 * @author Randall Hauch
 */
@ThreadSafe
public final class Tables {

    /**
     * Create a {@link TableNameFilter} for the given {@link Predicate Predicate<TableId>}.
     * @param predicate the {@link TableId} predicate filter;  may be null
     * @return the TableNameFilter; never null
     */
    public static TableNameFilter filterFor( Predicate<TableId> predicate) {
        if ( predicate == null ) return (catalogName, schemaName, tableName)->true;
        return (catalogName, schemaName, tableName)->{
            TableId id = new TableId(catalogName, schemaName, tableName);
            return predicate.test(id);
        };
    }

    /**
     * A filter for tables.
     */
    @FunctionalInterface
    public static interface TableNameFilter {
        /**
         * Determine whether the named table should be included.
         * 
         * @param catalogName the name of the database catalog that contains the table; may be null if the JDBC driver does not
         *            show a schema for this table
         * @param schemaName the name of the database schema that contains the table; may be null if the JDBC driver does not
         *            show a schema for this table
         * @param tableName the name of the table
         * @return {@code true} if the table should be included, or {@code false} if the table should be excluded
         */
        boolean matches(String catalogName, String schemaName, String tableName);
    }

    /**
     * A filter for columns.
     */
    @FunctionalInterface
    public static interface ColumnNameFilter {
        /**
         * Determine whether the named column should be included in the table's {@link Schema} definition.
         * 
         * @param catalogName the name of the database catalog that contains the table; may be null if the JDBC driver does not
         *            show a schema for this table
         * @param schemaName the name of the database schema that contains the table; may be null if the JDBC driver does not
         *            show a schema for this table
         * @param tableName the name of the table
         * @param columnName the name of the column
         * @return {@code true} if the table should be included, or {@code false} if the table should be excluded
         */
        boolean matches(String catalogName, String schemaName, String tableName, String columnName);
    }

    private final FunctionalReadWriteLock lock = FunctionalReadWriteLock.reentrant();
    private final Map<TableId, TableImpl> tablesByTableId = new ConcurrentHashMap<>();
    private final Set<TableId> changes = new HashSet<>();

    /**
     * Create an empty set of definitions.
     */
    public Tables() {
    }
    
    protected Tables( Tables other) {
        this.tablesByTableId.putAll(other.tablesByTableId);
    }
    
    @Override
    public Tables clone() {
        return new Tables(this);
    }

    /**
     * Get the number of tables that are in this object.
     * 
     * @return the table count
     */
    public int size() {
        return lock.read(tablesByTableId::size);
    }

    public Set<TableId> drainChanges() {
        return lock.write(() -> {
            Set<TableId> result = new HashSet<>(changes);
            changes.clear();
            return result;
        });
    }

    /**
     * Add or update the definition for the identified table.
     * 
     * @param tableId the identifier of the table
     * @param columnDefs the list of column definitions; may not be null or empty
     * @param primaryKeyColumnNames the list of the column names that make up the primary key; may be null or empty
     * @param defaultCharsetName the name of the character set that should be used by default
     * @return the previous table definition, or null if there was no prior table definition
     */
    public Table overwriteTable(TableId tableId, List<Column> columnDefs, List<String> primaryKeyColumnNames,
                                String defaultCharsetName) {
        return lock.write(() -> {
            TableImpl updated = new TableImpl(tableId, columnDefs, primaryKeyColumnNames, defaultCharsetName);
            TableImpl existing = tablesByTableId.get(tableId);
            if ( existing == null || !existing.equals(updated) ) {
                // Our understanding of the table has changed ...
                changes.add(tableId);
                tablesByTableId.put(tableId,updated);
            }
            return tablesByTableId.get(tableId);
        });
    }

    /**
     * Add or update the definition for the identified table.
     * 
     * @param table the definition for the table; may not be null
     * @return the previous table definition, or null if there was no prior table definition
     */
    public Table overwriteTable(Table table) {
        return lock.write(() -> {
            TableImpl updated = new TableImpl(table);
            try {
                return tablesByTableId.put(updated.id(), updated);
            } finally {
                changes.add(updated.id());
            }
        });
    }

    /**
     * Rename an existing table.
     * 
     * @param existingTableId the identifier of the existing table to be renamed; may not be null
     * @param newTableId the new identifier for the table; may not be null
     * @return the previous table definition, or null if there was no prior table definition
     */
    public Table renameTable(TableId existingTableId, TableId newTableId) {
        return lock.write(() -> {
            Table existing = forTable(existingTableId);
            if (existing == null) return null;
            tablesByTableId.remove(existing);
            TableImpl updated = new TableImpl(newTableId, existing.columns(),
                                              existing.primaryKeyColumnNames(), existing.defaultCharsetName());
            try {
                return tablesByTableId.put(updated.id(), updated);
            } finally {
                changes.add(existingTableId);
                changes.add(updated.id());
            }
        });
    }

    /**
     * Add or update the definition for the identified table.
     * 
     * @param tableId the identifier of the table
     * @param changer the function that accepts the current {@link Table} and returns either the same or an updated
     *            {@link Table}; may not be null
     * @return the previous table definition, or null if there was no prior table definition
     */
    public Table updateTable(TableId tableId, Function<Table, Table> changer) {
        return lock.write(() -> {
            TableImpl existing = tablesByTableId.get(tableId);
            Table updated = changer.apply(existing);
            if (updated != existing) {
                tablesByTableId.put(tableId, new TableImpl(tableId, updated.columns(),
                                                           updated.primaryKeyColumnNames(), updated.defaultCharsetName()));
            }
            changes.add(tableId);
            return existing;
        });
    }

    /**
     * Add or update the definition for the identified table.
     * 
     * @param tableId the identifier of the table
     * @param changer the function that accepts and changes the mutable ordered list of column definitions and the mutable set of
     *            column names that make up the primary key; may not be null
     * @return the previous table definition, or null if there was no prior table definition
     */
    public Table updateTable(TableId tableId, TableChanger changer) {
        return lock.write(() -> {
            TableImpl existing = tablesByTableId.get(tableId);
            List<Column> columns = new ArrayList<>(existing.columns());
            List<String> pkColumnNames = new ArrayList<>(existing.primaryKeyColumnNames());
            changer.rewrite(columns, pkColumnNames);
            TableImpl updated = new TableImpl(tableId, columns, pkColumnNames, existing.defaultCharsetName());
            tablesByTableId.put(tableId, updated);
            changes.add(tableId);
            return existing;
        });
    }

    public static interface TableChanger {
        void rewrite(List<Column> columnDefinitions, List<String> primaryKeyNames);
    }

    /**
     * Remove the definition of the identified table.
     * 
     * @param tableId the identifier of the table
     * @return the existing table definition that was removed, or null if there was no prior table definition
     */
    public Table removeTable(TableId tableId) {
        return lock.write(() -> {
            changes.add(tableId);
            return tablesByTableId.remove(tableId);
        });
    }

    /**
     * Obtain the definition of the identified table.
     * 
     * @param tableId the identifier of the table
     * @return the table definition, or null if there was no definition for the identified table
     */
    public Table forTable(TableId tableId) {
        return lock.read(() -> tablesByTableId.get(tableId));
    }

    /**
     * Obtain the definition of the identified table.
     * 
     * @param catalogName the name of the database catalog that contains the table; may be null if the JDBC driver does not
     *            show a schema for this table
     * @param schemaName the name of the database schema that contains the table; may be null if the JDBC driver does not
     *            show a schema for this table
     * @param tableName the name of the table
     * @return the table definition, or null if there was no definition for the identified table
     */
    public Table forTable(String catalogName, String schemaName, String tableName) {
        return forTable(new TableId(catalogName, schemaName, tableName));
    }

    /**
     * Get the set of {@link TableId}s for which there is a {@link Schema}.
     * 
     * @return the immutable set of table identifiers; never null
     */
    public Set<TableId> tableIds() {
        return lock.read(() -> Collect.unmodifiableSet(tablesByTableId.keySet()));
    }

    /**
     * Obtain an editor for the table with the given ID. This method does not lock the set of table definitions, so use
     * with caution. The resulting editor can be used to modify the table definition, but when completed the new {@link Table}
     * needs to be added back to this object via {@link #overwriteTable(Table)}.
     * 
     * @param tableId the identifier of the table
     * @return the editor for the table, or null if there is no table with the specified ID
     */
    public TableEditor editTable(TableId tableId) {
        Table table = forTable(tableId);
        return table == null ? null : table.edit();
    }

    /**
     * Obtain an editor for the identified table. This method does not lock the set of table definitions, so use
     * with caution. The resulting editor can be used to modify the table definition, but when completed the new {@link Table}
     * needs to be added back to this object via {@link #overwriteTable(Table)}.
     * 
     * @param catalogName the name of the database catalog that contains the table; may be null if the JDBC driver does not
     *            show a schema for this table
     * @param schemaName the name of the database schema that contains the table; may be null if the JDBC driver does not
     *            show a schema for this table
     * @param tableName the name of the table
     * @return the editor for the table, or null if there is no table with the specified ID
     */
    public TableEditor editTable(String catalogName, String schemaName, String tableName) {
        return editTable(new TableId(catalogName, schemaName, tableName));
    }

    /**
     * Obtain an editor for the table with the given ID. This method does not lock or modify the set of table definitions, so use
     * with caution. The resulting editor can be used to modify the table definition, but when completed the new {@link Table}
     * needs to be added back to this object via {@link #overwriteTable(Table)}.
     * 
     * @param tableId the identifier of the table
     * @return the editor for the table, or null if there is no table with the specified ID
     */
    public TableEditor editOrCreateTable(TableId tableId) {
        Table table = forTable(tableId);
        return table == null ? Table.editor().tableId(tableId) : table.edit();
    }

    /**
     * Obtain an editor for the identified table or, if there is no such table, create an editor with the specified ID.
     * This method does not lock or modify the set of table definitions, so use with caution. The resulting editor can be used to
     * modify the table definition, but when completed the new {@link Table} needs to be added back to this object via
     * {@link #overwriteTable(Table)}.
     * 
     * @param catalogName the name of the database catalog that contains the table; may be null if the JDBC driver does not
     *            show a schema for this table
     * @param schemaName the name of the database schema that contains the table; may be null if the JDBC driver does not
     *            show a schema for this table
     * @param tableName the name of the table
     * @return the editor for the table, or null if there is no table with the specified ID
     */
    public TableEditor editOrCreateTable(String catalogName, String schemaName, String tableName) {
        return editOrCreateTable(new TableId(catalogName, schemaName, tableName));
    }

    @Override
    public int hashCode() {
        return tablesByTableId.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj instanceof Tables) {
            Tables that = (Tables) obj;
            return this.tablesByTableId.equals(that.tablesByTableId);
        }
        return false;
    }

    public Tables subset(Predicate<TableId> filter) {
        if (filter == null) return this;
        return lock.read(() -> {
            Tables result = new Tables();
            tablesByTableId.forEach((tableId, table) -> {
                if (filter.test(tableId)) {
                    result.overwriteTable(table);
                }
            });
            return result;
        });
    }

    @Override
    public String toString() {
        return lock.read(() -> {
            StringBuilder sb = new StringBuilder();
            sb.append("Tables {");
            if (!tablesByTableId.isEmpty()) {
                sb.append(System.lineSeparator());
                tablesByTableId.forEach((tableId, table) -> {
                    sb.append("  ").append(tableId).append(": {").append(System.lineSeparator());
                    table.toString(sb, "    ");
                    sb.append("  }").append(System.lineSeparator());
                });
            }
            sb.append("}");
            return sb.toString();
        });
    }
}
