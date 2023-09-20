/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.connect.data.Struct;

import io.debezium.DebeziumException;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import io.debezium.util.Clock;

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
    private final Instant timestamp;
    private final TableChanges tableChanges = new TableChanges();

    private SchemaChangeEvent(Map<String, ?> partition, Map<String, ?> offset, Struct source, String database, String schema, String ddl, Table table,
                              SchemaChangeEventType type, boolean isFromSnapshot, TableId previousTableId) {
        this(partition, offset, source, database, schema, ddl, table != null ? Collections.singleton(table) : Collections.emptySet(),
                type, isFromSnapshot, Clock.SYSTEM.currentTimeAsInstant(), previousTableId);
    }

    private SchemaChangeEvent(Map<String, ?> partition, Map<String, ?> offset, Struct source, String database, String schema, String ddl, Set<Table> tables,
                              SchemaChangeEventType type, boolean isFromSnapshot, Instant timestamp, TableId previousTableId) {
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
        this.timestamp = timestamp;
        switch (type) {
            case CREATE:
                tables.forEach(tableChanges::create);
                break;
            case ALTER:
                if (previousTableId == null) {
                    tables.forEach(tableChanges::alter);
                }
                else {
                    // there is only ever 1 table within the set, so it's safe to apply the previousTableId like this
                    tables.forEach(t -> tableChanges.rename(t, previousTableId));
                }
                break;
            case DROP:
                tables.stream().map(Table::id).forEach(tableChanges::drop);
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

    public Instant getTimestamp() {
        return timestamp;
    }

    public TableChanges getTableChanges() {
        return tableChanges;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SchemaChangeEvent that = (SchemaChangeEvent) o;
        return Objects.equals(database, that.database) && Objects.equals(schema, that.schema) && Objects.equals(ddl,
                that.ddl) && type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, schema, ddl, type);
    }

    @Override
    public String toString() {
        return "SchemaChangeEvent [database=" + database + ", schema=" + schema + ", ddl=" + ddl + ", tables=" + tables
                + ", type=" + type + ", ts_ms=" + timestamp.toEpochMilli() + "]";
    }

    /**
     * Type describing the content of the event.
     * CREATE, ALTER, DROP, TRUNCATE - corresponds to table operations
     * DATABASE - an event common to the database, like CREATE/DROP DATABASE or SET...
     */
    public enum SchemaChangeEventType {
        CREATE,
        ALTER,
        DROP,
        TRUNCATE,
        DATABASE
    }

    /**
     * Create a schema change event for any event type that does not involve a table rename.
     *
     * @param type the schema change event type
     * @param partition the partition
     * @param offsetContext the offset context
     * @param databaseName the database name
     * @param schemaName the schema name
     * @param ddl the schema change DDL statement
     * @param table the affected relational table
     * @param isFromSnapshot flag indicating whether the change is from snapshot
     * @return the schema change event
     */
    public static SchemaChangeEvent of(SchemaChangeEventType type, Partition partition, OffsetContext offsetContext,
                                       String databaseName, String schemaName, String ddl, Table table, boolean isFromSnapshot) {
        return new SchemaChangeEvent(
                partition.getSourcePartition(),
                offsetContext.getOffset(),
                offsetContext.getSourceInfo(),
                databaseName,
                schemaName,
                ddl,
                table,
                type,
                isFromSnapshot,
                null);
    }

    /**
     * Create a schema change event for a {@link io.debezium.relational.history.TableChanges.TableChange}.
     *
     * @param change the table change event
     * @param partition the partition
     * @param offset the offsets
     * @param source the source
     * @param databaseName the database name
     * @param schemaName the schema name
     * @return the schema change event
     */
    public static SchemaChangeEvent ofTableChange(TableChanges.TableChange change, Map<String, ?> partition, Map<String, ?> offset,
                                                  Struct source, String databaseName, String schemaName) {
        return new SchemaChangeEvent(
                partition,
                offset,
                source,
                databaseName,
                schemaName,
                null,
                change.getTable(),
                toSchemaChangeEventType(change.getType()),
                false,
                change.getPreviousId());
    }

    /**
     * Create a schema change event for a database-specific DDL operation.
     *
     * @param partition the partition
     * @param offsetContext the offset context
     * @param databaseName the database name
     * @param ddl the schema change DDL statement
     * @param isFromSnapshot flag indicating whether the change is from snapshot
     * @return the schema change event
     */
    public static SchemaChangeEvent ofDatabase(Partition partition, OffsetContext offsetContext, String databaseName,
                                               String ddl, boolean isFromSnapshot) {
        return of(
                SchemaChangeEventType.DATABASE,
                partition,
                offsetContext,
                databaseName,
                null,
                ddl,
                (Table) null,
                isFromSnapshot);
    }

    /**
     * Create a schema change event for a {@code CREATE TABLE} statement without DDL from snapshot phase.
     *
     * @param partition the partition
     * @param offsetContext the offset context
     * @param databaseName the database name
     * @param table the affected relational table
     * @return the schema change event
     */
    public static SchemaChangeEvent ofSnapshotCreate(Partition partition, OffsetContext offsetContext, String databaseName,
                                                     Table table) {
        return ofCreate(partition, offsetContext, databaseName, table.id().schema(), null, table, true);
    }

    /**
     * Create a schema change event for a {@code CREATE TABLE} statement with DDL.
     *
     * @param partition the partition
     * @param offsetContext the offset context
     * @param databaseName the database name
     * @param schemaName the schema name
     * @param ddl the schema change DDL statement
     * @param table the affected relational table
     * @param isFromSnapshot flag indicating whether the change is from snapshot
     * @return the schema change event
     */
    public static SchemaChangeEvent ofCreate(Partition partition, OffsetContext offsetContext, String databaseName,
                                             String schemaName, String ddl, Table table, boolean isFromSnapshot) {
        return of(
                SchemaChangeEventType.CREATE,
                partition,
                offsetContext,
                databaseName,
                schemaName,
                ddl,
                table,
                isFromSnapshot);
    }

    /**
     * Create a schema change event for a {@code ALTER TABLE} event.
     *
     * @param partition the partition
     * @param offsetContext the offset context
     * @param databaseName the database name
     * @param schemaName the schema name
     * @param ddl the schema change DDL statement
     * @param table the affected relational table
     * @return the schema change event
     */
    public static SchemaChangeEvent ofAlter(Partition partition, OffsetContext offsetContext, String databaseName,
                                            String schemaName, String ddl, Table table) {
        return of(
                SchemaChangeEventType.ALTER,
                partition,
                offsetContext,
                databaseName,
                schemaName,
                ddl,
                table,
                false);
    }

    /**
     * Create a schema change event for a {@code ALTER TABLE RENAME} event.
     *
     * @param partition the partition
     * @param offsetContext the offset context
     * @param databaseName the database name
     * @param schemaName the schema name
     * @param ddl the schema change DDL statement
     * @param table the affected relational table
     * @param previousTableId the old, previous relational table identifier
     * @return the schema change event
     */
    public static SchemaChangeEvent ofRename(Partition partition, OffsetContext offsetContext, String databaseName,
                                             String schemaName, String ddl, Table table, TableId previousTableId) {
        return new SchemaChangeEvent(
                partition.getSourcePartition(),
                offsetContext.getOffset(),
                offsetContext.getSourceInfo(),
                databaseName,
                schemaName,
                ddl,
                table,
                SchemaChangeEventType.ALTER,
                false,
                previousTableId);
    }

    /**
     * Create a schema change event for a {@code DROP TABLE} event.
     *
     * @param partition the partition
     * @param offsetContext the offset context
     * @param databaseName the database name
     * @param schemaName the schema name
     * @param ddl the schema change DDL statement
     * @param table the affected relational table
     * @return the schema change event
     */
    public static SchemaChangeEvent ofDrop(Partition partition, OffsetContext offsetContext, String databaseName,
                                           String schemaName, String ddl, Table table) {
        return of(
                SchemaChangeEventType.DROP,
                partition,
                offsetContext,
                databaseName,
                schemaName,
                ddl,
                table,
                false);
    }

    /**
     * Create a schema change event for a {@code TRUNCATE TABLE} event.
     *
     * @param partition the partition
     * @param offsetContext the offset context
     * @param databaseName the database name
     * @param schemaName the schema name
     * @param ddl the schema change DDL statement
     * @param table the affected relational table
     * @return the schema change event
     */
    public static SchemaChangeEvent ofTruncate(Partition partition, OffsetContext offsetContext, String databaseName,
                                               String schemaName, String ddl, Table table) {
        return of(
                SchemaChangeEventType.TRUNCATE,
                partition,
                offsetContext,
                databaseName,
                schemaName,
                ddl,
                table,
                false);
    }

    private static SchemaChangeEvent.SchemaChangeEventType toSchemaChangeEventType(TableChanges.TableChangeType type) {
        switch (type) {
            case CREATE:
                return SchemaChangeEventType.CREATE;
            case ALTER:
                return SchemaChangeEventType.ALTER;
            case DROP:
                return SchemaChangeEventType.DROP;
        }
        throw new DebeziumException("Unknown table change event type " + type);
    }
}
