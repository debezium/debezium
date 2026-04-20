/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi.snapshot;

/**
 * Wrapper class to pass Table, TableSchema, and OffsetContext to snapshot completion handlers.
 *
 * <p>Handlers may need:
 * - Table (relational structure with columns, keys) for creating destination tables
 * - TableSchema (Kafka Connect schemas) for creating Debezium change events
 * - OffsetContext (source metadata with LSN, txId, etc.) for proper source struct creation
 *
 * @author Ivan Senyk
 */
public class SnapshotTableMetadata {

    private final Object table;
    private final Object tableSchema;
    private final Object offsetContext;

    public SnapshotTableMetadata(Object table, Object tableSchema) {
        this(table, tableSchema, null);
    }

    public SnapshotTableMetadata(Object table, Object tableSchema, Object offsetContext) {
        this.table = table;
        this.tableSchema = tableSchema;
        this.offsetContext = offsetContext;
    }

    /**
     * Gets the relational Table (io.debezium.relational.Table).
     */
    public Object getTable() {
        return table;
    }

    /**
     * Gets the Debezium TableSchema (io.debezium.relational.TableSchema).
     */
    public Object getTableSchema() {
        return tableSchema;
    }

    /**
     * Gets the OffsetContext (io.debezium.pipeline.spi.OffsetContext).
     * May be null for handlers that don't need it.
     */
    public Object getOffsetContext() {
        return offsetContext;
    }
}
