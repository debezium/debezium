/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.spi;

import io.debezium.annotation.Incubating;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.relational.TableId;

import java.util.Optional;

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
     * Generate a valid postgres query string for the specified table, or an empty {@link Optional}
     * to skip snapshotting this table (but that table will still be streamed from)
     *
     * @param tableId the table to generate a query for
     * @return a valid query string, or none to skip snapshotting this table
     */
    Optional<String> buildSnapshotQuery(TableId tableId);
}
