/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import java.util.Objects;

import io.debezium.relational.Table;

public class SchemaChangeEvent {

    private final String ddl;
    private final Table table;
    private final SchemaChangeEventType type;

    public SchemaChangeEvent(String ddl, Table table, SchemaChangeEventType type) {
        this.ddl = Objects.requireNonNull(ddl, "ddl must not be null");
        this.table = Objects.requireNonNull(table, "table must not be null");
        this.type = Objects.requireNonNull(type, "type must not be null");
    }

    public String getDdl() {
        return ddl;
    }

    public Table getTable() {
        return table;
    }

    public SchemaChangeEventType getType() {
        return type;
    }

    @Override
    public String toString() {
        return "SchemaChangeEvent [ddl=" + ddl + ", table=" + table + ", type=" + type + "]";
    }

    public static enum SchemaChangeEventType {
        CREATE,
        ALTER,
        DROP;
    }
}
