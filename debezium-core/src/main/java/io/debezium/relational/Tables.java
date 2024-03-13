/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.kafka.connect.data.Schema;

import io.debezium.annotation.ThreadSafe;
import io.debezium.function.Predicates;
import io.debezium.schema.DataCollectionFilters.DataCollectionFilter;
import io.debezium.schema.DatabaseSchema;
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
     * A filter for tables.
     */
    @FunctionalInterface
    public interface TableFilter extends DataCollectionFilter<TableId> {

        /**
         * Determines whether the given table should be included in the current {@link DatabaseSchema}.
         */
        @Override
        boolean isIncluded(TableId tableId);

        /**
         * Creates a {@link TableFilter} from the given predicate.
         */
        static TableFilter fromPredicate(Predicate<TableId> predicate) {
            return t -> predicate.test(t);
        }

        /**
         * Creates a {@link TableFilter} that includes all tables.
         */
        static TableFilter includeAll() {
            return t -> true;
        }
    }

    public static class ColumnNameFilterFactory {

        /**
         * Build the {@link ColumnNameFilter} that determines whether a column identified by a given {@link ColumnId} is to be included,
         * using the given comma-separated regular expression patterns defining which columns (if any) should be <i>excluded</i>.
         * <p>
         * Note that this predicate is completely independent of the table selection predicate, so it is expected that this predicate
         * be used only <i>after</i> the table selection predicate determined the table containing the column(s) is to be used.
         *
         * @param fullyQualifiedColumnNames the comma-separated list of fully-qualified column names to exclude; may be null or
         * @return a column name filter; never null
         */
        public static ColumnNameFilter createExcludeListFilter(String fullyQualifiedColumnNames, ColumnFilterMode columnFilterMode) {
            Predicate<ColumnId> delegate = Predicates.excludes(fullyQualifiedColumnNames, ColumnId::toString);
            return (catalogName, schemaName, tableName, columnName) -> delegate
                    .test(new ColumnId(columnFilterMode.getTableIdForFilter(catalogName, schemaName, tableName), columnName));
        }

        /**
         * Build the {@link ColumnNameFilter} that determines whether a column identified by a given {@link ColumnId} is to be included,
         * using the given comma-separated regular expression patterns defining which columns (if any) should be <i>included</i>.
         * <p>
         * Note that this predicate is completely independent of the table selection predicate, so it is expected that this predicate
         * be used only <i>after</i> the table selection predicate determined the table containing the column(s) is to be used.
         *
         * @param fullyQualifiedColumnNames the comma-separated list of fully-qualified column names to  include; may be null or
         * @return a column name filter; never null
         */
        public static ColumnNameFilter createIncludeListFilter(String fullyQualifiedColumnNames, ColumnFilterMode columnFilterMode) {
            Predicate<ColumnId> delegate = Predicates.includes(fullyQualifiedColumnNames, ColumnId::toString);
            return (catalogName, schemaName, tableName, columnName) -> delegate
                    .test(new ColumnId(columnFilterMode.getTableIdForFilter(catalogName, schemaName, tableName), columnName));
        }
    }

    /**
     * A filter for columns.
     */
    @FunctionalInterface
    public interface ColumnNameFilter {

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
    private final TablesById tablesByTableId;
    private final TableIds changes;
    private final boolean tableIdCaseInsensitive;

    /**
     * Create an empty set of definitions.
     *
     * @param tableIdCaseInsensitive - true if lookup is case insensitive (typical for MySQL on Windows)
     */
    public Tables(boolean tableIdCaseInsensitive) {
        this.tableIdCaseInsensitive = tableIdCaseInsensitive;
        this.tablesByTableId = new TablesById(tableIdCaseInsensitive);
        this.changes = new TableIds(tableIdCaseInsensitive);
    }

    /**
     * Create case sensitive empty set of definitions.
     */
    public Tables() {
        this(false);
    }

    protected Tables(Tables other, boolean tableIdCaseInsensitive) {
        this(tableIdCaseInsensitive);
        this.tablesByTableId.putAll(other.tablesByTableId);
    }

    public void clear() {
        lock.write(() -> {
            tablesByTableId.clear();
            changes.clear();
        });
    }

    @Override
    public Tables clone() {
        return new Tables(this, tableIdCaseInsensitive);
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
            Set<TableId> result = changes.toSet();
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
     * @param attributes the list of attribute definitions; may not be null or empty
     * @return the previous table definition, or null if there was no prior table definition
     */
    public Table overwriteTable(TableId tableId, List<Column> columnDefs, List<String> primaryKeyColumnNames,
                                String defaultCharsetName, List<Attribute> attributes) {
        return lock.write(() -> {
            Table updated = Table.editor()
                    .tableId(tableId)
                    .addColumns(columnDefs)
                    .setPrimaryKeyNames(primaryKeyColumnNames)
                    .setDefaultCharsetName(defaultCharsetName)
                    .addAttributes(attributes)
                    .create();

            Table existing = tablesByTableId.get(tableId);
            if (existing == null || !existing.equals(updated)) {
                // Our understanding of the table has changed ...
                changes.add(tableId);
                tablesByTableId.put(tableId, updated);
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
            }
            finally {
                changes.add(updated.id());
            }
        });
    }

    public void removeTablesForDatabase(String schemaName) {
        removeTablesForDatabase(schemaName, null);
    }

    public void removeTablesForDatabase(String catalogName, String schemaName) {
        lock.write(() -> {
            tablesByTableId.entrySet().removeIf(tableIdTableEntry -> {
                TableId tableId = tableIdTableEntry.getKey();

                boolean equalCatalog = Objects.equals(catalogName, tableId.catalog());
                boolean equalSchema = Objects.equals(schemaName, tableId.schema());

                return equalSchema && equalCatalog;
            });
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
            if (existing == null) {
                return null;
            }
            tablesByTableId.remove(existing.id());
            TableImpl updated = new TableImpl(newTableId, existing.columns(),
                    existing.primaryKeyColumnNames(), existing.defaultCharsetName(), existing.comment(), existing.attributes());
            try {
                return tablesByTableId.put(updated.id(), updated);
            }
            finally {
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
            Table existing = tablesByTableId.get(tableId);
            Table updated = changer.apply(existing);
            if (updated != existing) {
                tablesByTableId.put(tableId, new TableImpl(tableId, updated.columns(),
                        updated.primaryKeyColumnNames(), updated.defaultCharsetName(), updated.comment(), updated.attributes()));
            }
            changes.add(tableId);
            return existing;
        });
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
     * Obtain the table id of the oracle object id.
     *
     * @param objectId the id of the oracle table object
     * @return the table id, or null if there was no definition for the oracle table object
     */
    public TableId forTableId(long objectId) {
        return lock.read(() -> tablesByTableId.getByObjectId(objectId));
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
        return lock.read(() -> Collect.unmodifiableSet(tablesByTableId.ids()));
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

    @Override
    public int hashCode() {
        return tablesByTableId.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof Tables) {
            Tables that = (Tables) obj;
            return this.tablesByTableId.equals(that.tablesByTableId);
        }
        return false;
    }

    public Tables subset(TableFilter filter) {
        if (filter == null) {
            return this;
        }
        return lock.read(() -> {
            Tables result = new Tables(tableIdCaseInsensitive);
            tablesByTableId.forEach((tableId, table) -> {
                if (filter.isIncluded(tableId)) {
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
                    if (table instanceof TableImpl) {
                        ((TableImpl) table).toString(sb, "    ");
                    }
                    else {
                        sb.append(table.toString());
                    }
                    sb.append("  }").append(System.lineSeparator());
                });
            }
            sb.append("}");
            return sb.toString();
        });
    }

    /**
     * A map of tables by id. Table names are stored lower-case if required as per the config.
     */
    private static class TablesById {

        private final boolean tableIdCaseInsensitive;
        private final ConcurrentMap<TableId, Table> values;
        private final ConcurrentMap<Long, TableId> tableIdMap;

        TablesById(boolean tableIdCaseInsensitive) {
            this.tableIdCaseInsensitive = tableIdCaseInsensitive;
            this.values = new ConcurrentHashMap<>();
            this.tableIdMap = new ConcurrentHashMap<>();
        }

        public Set<TableId> ids() {
            return values.keySet();
        }

        boolean isEmpty() {
            return values.isEmpty();
        }

        public void putAll(TablesById tablesByTableId) {
            if (tableIdCaseInsensitive) {
                tablesByTableId.values.entrySet()
                        .forEach(e -> put(e.getKey().toLowercase(), e.getValue()));
            }
            else {
                values.putAll(tablesByTableId.values);
                tablesByTableId.values.values().stream()
                        .map(Table::id)
                        .filter(tableId -> tableId.objectId() != null && tableId.objectId() > 0)
                        .forEach(tableId -> tableIdMap.put(tableId.objectId(), tableId));
            }
        }

        public Table remove(TableId tableId) {
            tableId = toLowerCaseIfNeeded(tableId);
            tableIdMap.remove(tableId.objectId());
            return values.remove(tableId);
        }

        public Table get(TableId tableId) {
            return values.get(toLowerCaseIfNeeded(tableId));
        }

        public TableId getByObjectId(long objectId) {
            return tableIdMap.get(objectId);
        }

        public Table put(TableId tableId, Table updated) {
            tableId = toLowerCaseIfNeeded(tableId);
            if (null != tableId.objectId() && tableId.objectId() > 0) {
                tableIdMap.put(tableId.objectId(), tableId);
            }
            return values.put(tableId, updated);
        }

        int size() {
            return values.size();
        }

        void forEach(BiConsumer<? super TableId, ? super Table> action) {
            values.forEach(action);
        }

        Set<Map.Entry<TableId, Table>> entrySet() {
            return values.entrySet();
        }

        void clear() {
            values.clear();
        }

        private TableId toLowerCaseIfNeeded(TableId tableId) {
            return tableIdCaseInsensitive ? tableId.toLowercase() : tableId;
        }

        @Override
        public int hashCode() {
            return values.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            TablesById other = (TablesById) obj;

            return values.equals(other.values);
        }
    }

    /**
     * A set of table ids. Table names are stored lower-case if required as per the config.
     */
    private static class TableIds {

        private final boolean tableIdCaseInsensitive;
        private final Set<TableId> values;

        TableIds(boolean tableIdCaseInsensitive) {
            this.tableIdCaseInsensitive = tableIdCaseInsensitive;
            this.values = new HashSet<>();
        }

        public void add(TableId tableId) {
            values.add(toLowerCaseIfNeeded(tableId));
        }

        public Set<TableId> toSet() {
            return new HashSet<>(values);
        }

        public void clear() {
            values.clear();
        }

        private TableId toLowerCaseIfNeeded(TableId tableId) {
            return tableIdCaseInsensitive ? tableId.toLowercase() : tableId;
        }
    }
}
