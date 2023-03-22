/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.relational;

import java.util.Objects;

/**
 * Describes a relational table's identifier.
 *
 * @author Chris Cranford
 */
public class TableId {

    private final String catalogName;
    private final String schemaName;
    private final String tableName;

    public TableId(String catalogName, String schemaName, String tableName) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableId tableId = (TableId) o;
        return Objects.equals(catalogName, tableId.catalogName)
                && Objects.equals(schemaName, tableId.schemaName)
                && Objects.equals(tableName, tableId.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName, schemaName, tableName);
    }
}
