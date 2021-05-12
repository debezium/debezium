/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi.snapshot;

import java.util.Optional;
import java.util.Set;

import io.debezium.common.annotation.Incubating;

/**
 * This interface is used to determine details about the snapshot process:
 *
 * Namely:
 * - Should a snapshot occur at all
 * - Should streaming occur
 * - What queries should be used to snapshot
 *
 * While many default snapshot modes are provided with Debezium (see documentation for details)
 * a custom implementation of this interface can be provided by the implementor which
 * can provide more advanced functionality, such as partial snapshots
 *
 * Implementor's must return true for either {@link #shouldSnapshot()} or {@link #shouldStream()}
 * or true for both.
 * 
 * @param <T> connector configuration
 * @param <U> state provided for snapshot execution
 * @param <V> the data collection identifier
 */
@Incubating
public interface Snapshotter<T, U, V> {

    void init(T config, U snapshotState);

    /**
     * @return true if the snapshotter should take a snapshot
     */
    boolean shouldSnapshot();

    /**
     * @return true if the snapshotter should stream after taking a snapshot
     */
    boolean shouldStream();

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
     * Generate a valid query string for the specified data collection, or an empty {@link Optional}
     * to skip snapshotting this data collection (but that it will still be streamed from)
     *
     * @param dataCollectionId the data collection to generate a query for
     * @return a valid query string, or none to skip snapshotting this data collection
     */
    Optional<String> buildSnapshotQuery(V dataCollectionId);

    /**
     * Return a new string that set up the transaction for snapshotting
     */
    String snapshotTransactionIsolationLevelStatement();

    /**
     * Returns a SQL statement for locking the given data collection during snapshotting, if required by the specific snapshotter
     * implementation.
     */
    Optional<String> snapshotDataCollectionLockingStatement(Set<V> dataCollectionIds);

    /**
     * Lifecycle hook called once the snapshot phase is finished.
     */
    default void snapshotCompleted() {
        // no operation
    }
}
