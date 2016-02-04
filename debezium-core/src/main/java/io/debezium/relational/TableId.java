/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.function.Predicate;

import io.debezium.annotation.Immutable;
import io.debezium.function.Predicates;

/**
 * Unique identifier for a database table.
 * 
 * @author Randall Hauch
 */
@Immutable
public final class TableId implements Comparable<TableId> {

    /**
     * Create a predicate function that allows only those {@link TableId}s that are allowed by the database whitelist (or
     * not disallowed by the database blacklist) and allowed by the table whitelist (or not disallowed by the table blacklist).
     * Therefore, blacklists are only used if there is no corresponding whitelist.
     * <p>
     * Qualified table names are comma-separated strings that are each {@link #parse(String) parsed} into {@link TableId} objects.
     * 
     * @param dbWhitelist the comma-separated string listing the names of the databases to be explicitly allowed;
     *            may be null
     * @param dbBlacklist the comma-separated string listing the names of the databases to be explicitly disallowed;
     *            may be null
     * @param tableWhitelist the comma-separated string listing the qualified names of the tables to be explicitly allowed;
     *            may be null
     * @param tableBlacklist the comma-separated string listing the qualified names of the tables to be explicitly disallowed;
     *            may be null
     * @return the predicate function; never null
     */
    public static Predicate<TableId> filter(String dbWhitelist, String dbBlacklist, String tableWhitelist, String tableBlacklist) {
        Predicate<TableId> tableExclusions = tableBlacklist == null ? null : Predicates.blacklist(tableBlacklist, TableId::parse);
        Predicate<TableId> tableInclusions = tableWhitelist == null ? null : Predicates.whitelist(tableWhitelist, TableId::parse);
        Predicate<TableId> tableFilter = tableInclusions != null ? tableInclusions : tableExclusions;
        Predicate<String> dbExclusions = dbBlacklist == null ? null : Predicates.blacklist(dbBlacklist);
        Predicate<String> dbInclusions = dbWhitelist == null ? null : Predicates.whitelist(dbWhitelist);
        Predicate<String> dbFilter = dbInclusions != null ? dbInclusions : dbExclusions;
        if (dbFilter != null) {
            if (tableFilter != null) {
                return (id) -> dbFilter.test(id.catalog()) && tableFilter.test(id);
            }
            return (id) -> dbFilter.test(id.catalog());
        }
        if (tableFilter != null) {
            return tableFilter;
        }
        return (id) -> true;
    }

    /**
     * Parse the supplied string delimited with a period ({@code .}) character, extracting up to the first 3 parts into a TableID.
     * If the input contains only two parts, then the first part will be used as the catalog name and the second as the table
     * name.
     * 
     * @param str the input string
     * @return the table ID, or null if it could not be parsed
     */
    public static TableId parse(String str) {
        return parse(str, '.', true);
    }

    /**
     * Parse the supplied string, extracting up to the first 3 parts into a TableID.
     * 
     * @param str the input string
     * @param delimiter the delimiter between parts
     * @param useCatalogBeforeSchema {@code true} if the parsed string contains only 2 items and the first should be used as
     *            the catalog and the second as the table name, or {@code false} if the first should be used as the schema and the
     *            second
     *            as the table name
     * @return the table ID, or null if it could not be parsed
     */
    public static TableId parse(String str, char delimiter, boolean useCatalogBeforeSchema) {
        String[] parts = str.split("[\\" + delimiter + "]");
        if (parts.length == 0) return null;
        if (parts.length == 1) return new TableId(null, null, parts[0]); // table only
        if (parts.length == 2) {
            if (useCatalogBeforeSchema) return new TableId(parts[0], null, parts[1]); // catalog & table only
            return new TableId(null, parts[0], parts[1]); // catalog & table only
        }
        return new TableId(parts[0], parts[1], parts[2]); // catalog & table only
    }

    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final String id;

    /**
     * Create a new table identifier.
     * 
     * @param catalogName the name of the database catalog that contains the table; may be null if the JDBC driver does not
     *            show a schema for this table
     * @param schemaName the name of the database schema that contains the table; may be null if the JDBC driver does not
     *            show a schema for this table
     * @param tableName the name of the table; may not be null
     */
    public TableId(String catalogName, String schemaName, String tableName) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        assert this.tableName != null;
        this.id = tableId(this.catalogName, this.schemaName, this.tableName);
    }

    /**
     * Get the name of the JDBC catalog.
     * 
     * @return the catalog name, or null if the table does not belong to a catalog
     */
    public String catalog() {
        return catalogName;
    }

    /**
     * Get the name of the JDBC schema.
     * 
     * @return the JDBC schema name, or null if the table does not belong to a JDBC schema
     */
    public String schema() {
        return schemaName;
    }

    /**
     * Get the name of the table.
     * 
     * @return the table name; never null
     */
    public String table() {
        return tableName;
    }

    @Override
    public int compareTo(TableId that) {
        if (this == that) return 0;
        return this.id.compareTo(that.id);
    }

    public int compareToIgnoreCase(TableId that) {
        if (this == that) return 0;
        return this.id.compareToIgnoreCase(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TableId) {
            return this.compareTo((TableId) obj) == 0;
        }
        return false;
    }

    @Override
    public String toString() {
        return id;
    }

    private static String tableId(String catalog, String schema, String table) {
        if (catalog == null || catalog.length() == 0) {
            if (schema == null || schema.length() == 0) {
                return table;
            }
            return schema + "." + table;
        }
        if (schema == null || schema.length() == 0) {
            return catalog + "." + table;
        }
        return catalog + "." + schema + "." + table;
    }
}
