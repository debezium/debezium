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

import org.apache.kafka.connect.data.Struct;

import io.debezium.relational.Table;
import io.debezium.relational.history.TableChanges;

/**
 * Represents a structural change to a database schema.
 *
 * @author Gunnar Morling
 */
public class SchemaChangeEvent {

    private final String database;
    private final String schema;
    private final String ddl;
    private final Set<Table> tables;
    private final SchemaChangeEventType type;
    private final Map<String, ?> partition;
    private final Map<String, ?> offset;
    private final Struct source;
    private final boolean isFromSnapshot;
    private TableChanges tableChanges = new TableChanges();

    public SchemaChangeEvent(Map<String, ?> partition, Map<String, ?> offset, Struct source, String database, String schema, String ddl, Table table,
                             SchemaChangeEventType type,
                             boolean isFromSnapshot) {
        this(partition, offset, source, database, schema, ddl, table != null ? Collections.singleton(table) : Collections.emptySet(), type, isFromSnapshot);
    }

    public SchemaChangeEvent(Map<String, ?> partition, Map<String, ?> offset, Struct source, String database, String schema, String ddl, Set<Table> tables,
                             SchemaChangeEventType type,
                             boolean isFromSnapshot) {
        this.partition = Objects.requireNonNull(partition, "partition must not be null");
        this.offset = Objects.requireNonNull(offset, "offset must not be null");
        this.source = Objects.requireNonNull(source, "source must not be null");
        this.database = Objects.requireNonNull(database, "database must not be null");
        // schema is not mandatory for all databases
        this.schema = schema;
        // DDL is not mandatory
        this.ddl = ddl;
        this.tables = Objects.requireNonNull(tables, "tables must not be null");
        this.type = Objects.requireNonNull(type, "type must not be null");
        this.isFromSnapshot = isFromSnapshot;
        switch (type) {
            case CREATE:
                tables.forEach(tableChanges::create);
                break;
            case ALTER:
                tables.forEach(tableChanges::alter);
                break;
            case DROP:
                tables.forEach(tableChanges::drop);
                break;
            case DATABASE:
                break;
        }
    }

    public Map<String, ?> getPartition() {
        return partition;
    }

    public Map<String, ?> getOffset() {
        return offset;
    }

    public Struct getSource() {
        return source;
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

    public TableChanges getTableChanges() {
        return tableChanges;
    }

    @Override
    public String toString() {
        return "SchemaChangeEvent [database=" + database + ", schema=" + schema + ", ddl=" + ddl + ", tables=" + tables
                + ", type=" + type + "]";
    }

    /**
     * Type describing the content of the event.
     * CREATE, ALTER, DROP - corresponds to table operations
     * DATABASE - an event common to the database, like CREATE/DROP DATABASE or SET...
     */
    public static enum SchemaChangeEventType {
        CREATE,
        ALTER,
        DROP,
        DATABASE;
    }
}
