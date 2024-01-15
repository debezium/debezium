/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi.snapshot;

import io.debezium.common.annotation.Incubating;

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
 * Implementor's must return true for either {@link #shouldSnapshot()} or {@link #shouldStream()}
 * or true for both.
 */
@Incubating
public interface Snapshotter {

    /**
     * @return true if the snapshotter should take a snapshot
     */
    boolean shouldSnapshot();

    /**
     * @return true if the snapshotter should take a snapshot
     */
    boolean shouldSnapshotSchema();

    /**
     * @return true if the snapshotter should stream after taking a snapshot
     */
    boolean shouldStream();

    /**
     * @return true whether the schema can be recovered if database schema history is corrupted.
     */
    boolean shouldSnapshotOnSchemaError();

    /**
     * @return true whether the snapshot should be re-executed when there is a gap in data stream.
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
     * Lifecycle hook called once the snapshot phase is successful.
     */
    default void snapshotCompleted() {
        // no operation
    }

    /**
     * Lifecycle hook called once the snapshot phase is aborted.
     */
    default void snapshotAborted() {
        // no operation
    }
}
