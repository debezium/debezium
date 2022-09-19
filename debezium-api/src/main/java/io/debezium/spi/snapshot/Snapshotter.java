/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi.snapshot;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

import io.debezium.common.annotation.Incubating;
import io.debezium.spi.schema.DataCollectionId;

/**
 * This interface is used to determine details about the snapshot process:
 *
 * Namely:
 * - Should a snapshot occur at all
 * - Should streaming occur
 * - What queries should be used to snapshot
 *
 * While many default snapshot modes are provided with debezium (see documentation for details)
 * a custom implementation of this interface can be provided by the implementor which
 * can provide more advanced functionality, such as partial snapshots
 *
 * @param <O> OffsetContext
 */
@Incubating
public interface Snapshotter<O> {

    void configure(Properties props, O offsetContext);

    /**
     * @return true if the snapshotter should take a snapshot
     */
    boolean shouldSnapshot();

    /**
     * Whether this snapshotting mode should include the schema.
     */
    boolean includeSchema();

    /**
     * Whether this snapshotting mode should include the actual data or just the
     * schema of captured tables.
     */
    boolean includeData();

    /**
     * @return true if the snapshotter should stream after taking a snapshot
     */
    boolean shouldStream();

    /**
     * Whether the schema can be recovered if database schema history is corrupted.
     */
    boolean shouldSnapshotOnSchemaError();

    /**
     * Whether the snapshot should be re-executed when there is a gap in data stream.
     */
    boolean shouldSnapshotOnDataError();

    /**
     *
     * @return true if streaming should resume from the start of the snapshot
     * transaction, or false for when a connector resumes and takes a snapshot,
     * streaming should resume from where streaming previously left off.
     */
    default boolean shouldStreamEventsStartingFromSnapshot() {
        return true;
    }

    /**
     * Generate a valid postgres query string for the specified table, or an empty {@link Optional}
     * to skip snapshotting this table (but that table will still be streamed from)
     *
     * @param id the table to generate a query for
     * @param snapshotSelectColumns the columns to be used in the snapshot select based on the column
     *                              include/exclude filters
     * @return a valid query string, or none to skip snapshotting this table
     */
    Optional<String> buildSnapshotQuery(DataCollectionId id, List<String> snapshotSelectColumns, char quotingChar);

    /**
     * Lifecycle hook called once the snapshot phase is finished.
     */
    default void snapshotCompleted() {
        // no operation
    }
}
