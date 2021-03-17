/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.spi;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;

import io.debezium.common.annotation.Incubating;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.relational.TableId;

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

    void init(PostgresConnectorConfig config, OffsetState sourceInfo,
              SlotState slotState);

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
     * @return true if when creating a slot, a snapshot should be exported, which
     * can be used as an alternative to taking a lock
     */
    default boolean exportSnapshot() {
        return false;
    }

    /**
     * Generate a valid postgres query string for the specified table, or an empty {@link Optional}
     * to skip snapshotting this table (but that table will still be streamed from)
     *
     * @param tableId the table to generate a query for
     * @return a valid query string, or none to skip snapshotting this table
     */
    Optional<String> buildSnapshotQuery(TableId tableId);

    /**
     * Return a new string that set up the transaction for snapshotting
     *
     * @param newSlotInfo if a new slow was created for snapshotting, this contains information from
     *                    the `create_replication_slot` command
     */
    default String snapshotTransactionIsolationLevelStatement(SlotCreationResult newSlotInfo) {
        // we're using the same isolation level that pg_backup uses
        return "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE;";
    }

    /**
     * Returns a SQL statement for locking the given tables during snapshotting, if required by the specific snapshotter
     * implementation.
     */
    default Optional<String> snapshotTableLockingStatement(Duration lockTimeout, Set<TableId> tableIds) {
        String lineSeparator = System.lineSeparator();
        StringBuilder statements = new StringBuilder();
        statements.append("SET lock_timeout = ").append(lockTimeout.toMillis()).append(";").append(lineSeparator);
        // we're locking in ACCESS SHARE MODE to avoid concurrent schema changes while we're taking the snapshot
        // this does not prevent writes to the table, but prevents changes to the table's schema....
        // DBZ-298 Quoting name in case it has been quoted originally; it doesn't do harm if it hasn't been quoted
        tableIds.forEach(tableId -> statements.append("LOCK TABLE ")
                .append(tableId.toDoubleQuotedString())
                .append(" IN ACCESS SHARE MODE;")
                .append(lineSeparator));
        return Optional.of(statements.toString());
    }

    /**
     * Lifecycle hook called once the snapshot phase is finished.
     */
    default void snapshotCompleted() {
        // no operation
    }
}
