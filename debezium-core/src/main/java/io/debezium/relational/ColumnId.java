/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import io.debezium.annotation.Immutable;
import io.debezium.util.Strings;

/**
 * Unique identifier for a column in a database table.
 *
 * @author Randall Hauch
 */
@Immutable
public final class ColumnId implements Comparable<ColumnId> {

    private static final Pattern IDENTIFIER_SEPARATOR_PATTERN = Pattern.compile("\\.");

    /**
     * Create the map of predicate functions that specify which columns are to be included.
     * <p>
     * Qualified column names are comma-separated strings that are each {@link #parse(String) parsed} into {@link ColumnId} objects.
     *
     * @param columnExcludeList the comma-separated string listing the qualified names of the columns to be explicitly disallowed;
     *            may be null
     * @return the predicate function; never null
     */
    public static Map<TableId, Predicate<Column>> filter(String columnExcludeList) {
        Set<ColumnId> columnExclusions = columnExcludeList == null ? null : Strings.setOf(columnExcludeList, ColumnId::parse);
        Map<TableId, Set<String>> excludedColumnNamesByTable = new HashMap<>();
        if (null != columnExclusions) {
            columnExclusions.forEach(columnId -> {
                excludedColumnNamesByTable.compute(columnId.tableId(), (tableId, columns) -> {
                    if (columns == null) {
                        columns = new HashSet<>();
                    }
                    columns.add(columnId.columnName().toLowerCase());
                    return columns;
                });
            });
        }
        Map<TableId, Predicate<Column>> exclusionFilterByTable = new HashMap<>();
        excludedColumnNamesByTable.forEach((tableId, excludedColumnNames) -> {
            exclusionFilterByTable.put(tableId, (col) -> !excludedColumnNames.contains(col.name().toLowerCase()));
        });
        return exclusionFilterByTable;
    }

    /**
     * Parse the supplied string delimited with a period ({@code .}) character, extracting the last segment into a column name
     * and the prior segments into the TableID.
     *
     * @param str the input string
     * @return the column ID, or null if it could not be parsed
     */
    public static ColumnId parse(String str) {
        return parse(str, true);
    }

    /**
     * Parse the supplied string delimited with the specified delimiter character, extracting the last segment into a column name
     * and the prior segments into the TableID.
     *
     * @param str the input string
     * @param useCatalogBeforeSchema {@code true} if the parsed string contains only 2 items and the first should be used as
     *            the catalog and the second as the table name, or {@code false} if the first should be used as the schema and the
     *            second
     *            as the table name
     * @return the column ID, or null if it could not be parsed
     */
    private static ColumnId parse(String str, boolean useCatalogBeforeSchema) {
        String[] parts = IDENTIFIER_SEPARATOR_PATTERN.split(str);
        if (parts.length < 2) {
            return null;
        }
        TableId tableId = TableId.parse(parts, parts.length - 1, useCatalogBeforeSchema);
        if (tableId == null) {
            return null;
        }
        return new ColumnId(tableId, parts[parts.length - 1]);
    }

    private final TableId tableId;
    private final String columnName;
    private final String id;

    /**
     * Create a new column identifier.
     *
     * @param tableId the identifier of the table; may not be null
     * @param columnName the name of the column; may not be null
     */
    public ColumnId(TableId tableId, String columnName) {
        this.tableId = tableId;
        this.columnName = columnName;
        assert this.tableId != null;
        assert this.columnName != null;
        this.id = columnId(this.tableId, this.columnName);
    }

    /**
     * Create a new column identifier.
     *
     * @param catalogName the name of the database catalog that contains the table; may be null if the JDBC driver does not
     *            show a schema for this table
     * @param schemaName the name of the database schema that contains the table; may be null if the JDBC driver does not
     *            show a schema for this table
     * @param tableName the name of the table; may not be null
     * @param columnName the name of the column; may not be null
     */
    public ColumnId(String catalogName, String schemaName, String tableName, String columnName) {
        this(new TableId(catalogName, schemaName, tableName), columnName);
    }

    /**
     * Get the identifier for the table that owns this column.
     *
     * @return the table identifier; never null
     */
    public TableId tableId() {
        return tableId;
    }

    /**
     * Get the name of the JDBC catalog.
     *
     * @return the catalog name, or null if the table does not belong to a catalog
     */
    public String catalog() {
        return tableId.catalog();
    }

    /**
     * Get the name of the JDBC schema.
     *
     * @return the JDBC schema name, or null if the table does not belong to a JDBC schema
     */
    public String schema() {
        return tableId.schema();
    }

    /**
     * Get the name of the table.
     *
     * @return the table name; never null
     */
    public String table() {
        return tableId.table();
    }

    /**
     * Get the name of the table.
     *
     * @return the table name; never null
     */
    public String columnName() {
        return columnName;
    }

    @Override
    public int compareTo(ColumnId that) {
        if (this == that) {
            return 0;
        }
        return this.id.compareTo(that.id);
    }

    public int compareToIgnoreCase(ColumnId that) {
        if (this == that) {
            return 0;
        }
        return this.id.compareToIgnoreCase(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ColumnId) {
            return this.compareTo((ColumnId) obj) == 0;
        }
        return false;
    }

    @Override
    public String toString() {
        return id;
    }

    private static String columnId(TableId tableId, String columnName) {
        return tableId.toString() + "." + columnName;
    }
}
