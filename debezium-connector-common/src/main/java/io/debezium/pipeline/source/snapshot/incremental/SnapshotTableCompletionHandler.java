/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.util.List;

import org.apache.kafka.connect.source.SourceRecord;

/**
 * SPI interface for handling table snapshot completion events.
 *
 * <p>Implementations receive pre-transformed SourceRecords — the configured SMT chain
 * has already been applied by the snapshot worker. This matches the CDC path where
 * the engine applies transforms before delivering records to the consumer.
 *
 * <p>Implementations can register via {@link java.util.ServiceLoader} to receive
 * notifications when an incremental snapshot worker completes processing a table.
 *
 * @author Ivan Senyk
 */
public interface SnapshotTableCompletionHandler {

    /**
     * Called for each chunk of pre-transformed records during snapshot processing.
     *
     * @param tableName The fully qualified table name (e.g., "schema.table")
     * @param records Pre-transformed SourceRecords ready for sink consumption
     */
    void onTableSnapshotCompleted(String tableName, List<SourceRecord> records);

    /**
     * Determines if this handler should process the given table.
     */
    default boolean shouldHandle(String tableName) {
        return true;
    }

    /**
     * Called when the snapshot worker has finished processing ALL chunks of a table.
     * This signals that no more records will arrive for this table.
     */
    default void onTableSnapshotFinished(String tableName) {
    }
}
