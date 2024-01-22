/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi.snapshot;

import io.debezium.DebeziumException;
import io.debezium.common.annotation.Incubating;
import io.debezium.spi.common.Configurable;

/**
 * {@link Snapshotter} is used to determine details about the snapshot process:
 * <p>
 * Namely: <br>
 * - Should a snapshot occur at all <br>
 * - Should streaming occur <br>
 * - Should snapshot schema (if supported) <br>
 * - Should snapshot data/schema on error
 * <p>
 * While many default snapshot modes are provided with debezium (see documentation for details)
 * a custom implementation of this interface can be provided by the implementor which
 * can provide more advanced functionality, such as partial snapshots
 *
 *
 * @author Mario Fiore Vitale
 */
@Incubating
public interface Snapshotter extends Configurable {

    /**
     * @return the name of the snapshotter.
     *
     *
     */
    String name();

    /**
     * Validate the snapshotter compatibility with the current connector configuration.
     * Throws a {@link DebeziumException} in case it is not compatible.
     *
     * @param offsetContextExists is {@code true} when the connector has an offset context (i.e. restarted)
     * @param isSnapshotInProgress is {@code true} when the connector is started but there was already a snapshot in progress
     */
    void validate(boolean offsetContextExists, boolean isSnapshotInProgress);

    /**
     * @return {@code true} if the snapshotter should take a snapshot
     */
    boolean shouldSnapshot();

    /**
     * @return {@code true} if the snapshotter should take a snapshot
     */
    boolean shouldSnapshotSchema();

    /**
     * @return {@code true} if the snapshotter should stream after taking a snapshot
     */
    boolean shouldStream();

    /**
     * @return {@code true} whether the schema can be recovered if database schema history is corrupted.
     */
    boolean shouldSnapshotOnSchemaError();

    /**
     * @return {@code true} whether the snapshot should be re-executed when there is a gap in data stream.
     */
    boolean shouldSnapshotOnDataError();

    /**
     *
     * @return {@code true} if streaming should resume from the start of the snapshot
     * transaction, or {@code false} for when a connector resumes and takes a snapshot,
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
