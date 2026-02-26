/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

/**
 * Modes for column name filters, either including a catalog (database) or schema name.
 *
 * @author Gunnar Morling
 */
public enum ColumnFilterMode {

    CATALOG {
        @Override
        public TableId getTableIdForFilter(String catalog, String schema, String table) {
            return new TableId(catalog, null, table);
        }
    },
    SCHEMA {
        @Override
        public TableId getTableIdForFilter(String catalog, String schema, String table) {
            return new TableId(null, schema, table);
        }
    };

    public abstract TableId getTableIdForFilter(String catalog, String schema, String table);
}
