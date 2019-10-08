/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import io.debezium.annotation.Immutable;
import io.debezium.schema.DataCollectionId;

/**
 * Unique identifier for a database table.
 *
 * @author Randall Hauch
 */
@Immutable
public final class TableId implements DataCollectionId, Comparable<TableId> {

    /**
     * Parse the supplied string, extracting up to the first 3 parts into a TableID.
     *
     * @param str the string representation of the table identifier; may not be null
     * @return the table ID, or null if it could not be parsed
     */
    public static TableId parse(String str) {
        return parse(str, true);
    }

    /**
     * Parse the supplied string, extracting up to the first 3 parts into a TableID.
     *
     * @param str the string representation of the table identifier; may not be null
     * @param useCatalogBeforeSchema {@code true} if the parsed string contains only 2 items and the first should be used as
     *            the catalog and the second as the table name, or {@code false} if the first should be used as the schema and the
     *            second as the table name
     * @return the table ID, or null if it could not be parsed
     */
    public static TableId parse(String str, boolean useCatalogBeforeSchema) {
        String[] parts = TableIdParser.parse(str).stream()
                .toArray(String[]::new);

        return TableId.parse(parts, parts.length, useCatalogBeforeSchema);
    }

    /**
     * Parse the supplied string, extracting up to the first 3 parts into a TableID.
     *
     * @param parts the parts of the identifier; may not be null
     * @param numParts the number of parts to use for the table identifier
     * @param useCatalogBeforeSchema {@code true} if the parsed string contains only 2 items and the first should be used as
     *            the catalog and the second as the table name, or {@code false} if the first should be used as the schema and the
     *            second as the table name
     * @return the table ID, or null if it could not be parsed
     */
    protected static TableId parse(String[] parts, int numParts, boolean useCatalogBeforeSchema) {
        if (numParts == 0) {
            return null;
        }
        if (numParts == 1) {
            return new TableId(null, null, parts[0]); // table only
        }
        if (numParts == 2) {
            if (useCatalogBeforeSchema) {
                return new TableId(parts[0], null, parts[1]); // catalog & table only
            }
            return new TableId(null, parts[0], parts[1]); // schema & table only
        }
        return new TableId(parts[0], parts[1], parts[2]); // catalog, schema & table
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
        if (this == that) {
            return 0;
        }
        return this.id.compareTo(that.id);
    }

    public int compareToIgnoreCase(TableId that) {
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
        if (obj instanceof TableId) {
            return this.compareTo((TableId) obj) == 0;
        }
        return false;
    }

    @Override
    public String toString() {
        return id;
    }

    /**
     * Returns a dot-separated String representation of this identifier, quoting all
     * name parts with the {@code "} char.
     */
    public String toDoubleQuotedString() {
        return toQuotedString('"');
    }

    /**
     * Returns a dot-separated String representation of this identifier, quoting all
     * name parts with the given quoting char.
     */
    public String toQuotedString(char quotingChar) {
        StringBuilder quoted = new StringBuilder();

        if (catalogName != null && !catalogName.isEmpty()) {
            quoted.append(quote(catalogName, quotingChar)).append(".");
        }

        if (schemaName != null && !schemaName.isEmpty()) {
            quoted.append(quote(schemaName, quotingChar)).append(".");
        }

        quoted.append(quote(tableName, quotingChar));

        return quoted.toString();
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

    /**
     * Quotes the given identifier part, e.g. schema or table name.
     */
    private static String quote(String identifierPart, char quotingChar) {
        if (identifierPart == null) {
            return null;
        }

        if (identifierPart.isEmpty()) {
            return new StringBuilder().append(quotingChar).append(quotingChar).toString();
        }

        if (identifierPart.charAt(0) != quotingChar && identifierPart.charAt(identifierPart.length() - 1) != quotingChar) {
            identifierPart = identifierPart.replace(quotingChar + "", repeat(quotingChar));
            identifierPart = quotingChar + identifierPart + quotingChar;
        }

        return identifierPart;
    }

    private static String repeat(char quotingChar) {
        return new StringBuilder().append(quotingChar).append(quotingChar).toString();
    }

    public TableId toLowercase() {
        return new TableId(catalogName, schemaName, tableName.toLowerCase());
    }
}
