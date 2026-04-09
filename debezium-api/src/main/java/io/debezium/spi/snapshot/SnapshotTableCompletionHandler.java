/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi.snapshot;

import java.util.List;

/**
 * SPI interface for handling table snapshot completion events.
 *
 * <p>Implementations can register via {@link java.util.ServiceLoader} to receive
 * notifications when an incremental snapshot worker completes processing a table.
 *
 * <p>This allows sink implementations (e.g., Iceberg, BigQuery) to perform custom
 * actions like immediate flushing, without coupling debezium-core to specific sinks.
 *
 * @author Debezium Community
 */
public interface SnapshotTableCompletionHandler {

    /**
     * Called when a snapshot worker completes processing an entire table.
     *
     * @param tableName The fully qualified table name (e.g., "schema.table")
     * @param rows The list of rows read from the table during snapshot (in Object[] format)
     * @param tableSchema The table schema metadata (can be null if not available)
     */
    void onTableSnapshotCompleted(String tableName, List<Object[]> rows, Object tableSchema);

    /**
     * Determines if this handler should process the given table.
     *
     * @param tableName The table name to check
     * @return true if this handler should process the table, false otherwise
     */
    default boolean shouldHandle(String tableName) {
        return true;
    }
}
