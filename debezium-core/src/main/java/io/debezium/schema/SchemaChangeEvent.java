/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.debezium.relational.Table;

public class SchemaChangeEvent {

    private final String database;
    private final String schema;
    private final String ddl;
    private final Set<Table> tables;
    private final SchemaChangeEventType type;
    private final Map<String, ?> partition;
    private final Map<String, ?> offset;
    private final boolean isFromSnapshot;

    public SchemaChangeEvent(Map<String, ?> partition, Map<String, ?> offset, String database, String schema, String ddl, Table table, SchemaChangeEventType type, boolean isFromSnapshot) {
        this(partition, offset, database, schema, ddl, table != null ? Collections.singleton(table) : null, type, isFromSnapshot);
    }

    public SchemaChangeEvent(Map<String, ?> partition, Map<String, ?> offset, String database, String schema, String ddl, Set<Table> tables, SchemaChangeEventType type, boolean isFromSnapshot) {
        this.partition = Objects.requireNonNull(partition, "partition must not be null");
        this.offset = Objects.requireNonNull(offset, "offset must not be null");
        this.database = Objects.requireNonNull(database, "database must not be null");
        this.schema = Objects.requireNonNull(schema, "schema must not be null");
        this.ddl = Objects.requireNonNull(ddl, "ddl must not be null");
        this.tables = Objects.requireNonNull(tables, "tables must not be null");
        this.type = Objects.requireNonNull(type, "type must not be null");
        this.isFromSnapshot = isFromSnapshot;
    }

    public Map<String, ?> getPartition() {
        return partition;
    }

    public Map<String, ?> getOffset() {
        return offset;
    }

    public String getDatabase() {
        return database;
    }

    public String getSchema() {
        return schema;
    }

    public String getDdl() {
        return ddl;
    }

    public Set<Table> getTables() {
        return tables;
    }

    public SchemaChangeEventType getType() {
        return type;
    }

    public boolean isFromSnapshot() {
        return isFromSnapshot;
    }

    @Override
    public String toString() {
        return "SchemaChangeEvent [ddl=" + ddl + ", tables=" + tables + ", type=" + type + "]";
    }

    public static enum SchemaChangeEventType {
        CREATE,
        ALTER,
        DROP;
    }
}
