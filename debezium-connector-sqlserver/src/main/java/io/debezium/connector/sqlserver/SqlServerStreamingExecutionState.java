/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Represents the state of an {@link SqlServerStreamingChangeEventSource} object between calls to the
 * SqlServerStreamingChangeEventSource#execute method.
 *
 * @author Jacob Gminder
 *
 */
public class SqlServerStreamingExecutionState {
    private final Queue<SqlServerChangeTable> schemaChangeCheckpoints;
    private final AtomicReference<SqlServerChangeTable[]> tablesSlot;
    private final TxLogPosition lastProcessedPositionOnStart;
    private final long lastProcessedEventSerialNoOnStart;
    private final TxLogPosition lastProcessedPosition;
    private final AtomicBoolean changesStoppedBeingMonotonic;
    private final boolean shouldIncreaseFromLsn;
    private final StreamingResultStatus status;

    public StreamingResultStatus getStatus() {
        return status;
    }

    SqlServerStreamingExecutionState(Queue<SqlServerChangeTable> schemaChangeCheckpoints,
                                     AtomicReference<SqlServerChangeTable[]> tablesSlot,
                                     TxLogPosition lastProcessedPositionOnStart,
                                     long lastProcessedEventSerialNoOnStart,
                                     TxLogPosition lastProcessedPosition,
                                     AtomicBoolean changesStoppedBeingMonotonic,
                                     boolean shouldIncreaseFromLsn,
                                     StreamingResultStatus status) {
        this.schemaChangeCheckpoints = schemaChangeCheckpoints;
        this.tablesSlot = tablesSlot;
        this.lastProcessedEventSerialNoOnStart = lastProcessedEventSerialNoOnStart;
        this.lastProcessedPositionOnStart = lastProcessedPositionOnStart;
        this.changesStoppedBeingMonotonic = changesStoppedBeingMonotonic;
        this.shouldIncreaseFromLsn = shouldIncreaseFromLsn;
        this.lastProcessedPosition = lastProcessedPosition;
        this.status = status;
    }

    public Queue<SqlServerChangeTable> getSchemaChangeCheckpoints() {
        return schemaChangeCheckpoints;
    }

    public AtomicReference<SqlServerChangeTable[]> getTablesSlot() {
        return tablesSlot;
    }

    public TxLogPosition getLastProcessedPositionOnStart() {
        return lastProcessedPositionOnStart;
    }

    public long getLastProcessedEventSerialNoOnStart() {
        return lastProcessedEventSerialNoOnStart;
    }

    public TxLogPosition getLastProcessedPosition() {
        return lastProcessedPosition;
    }

    public AtomicBoolean getChangesStoppedBeingMonotonic() {
        return changesStoppedBeingMonotonic;
    }

    public boolean getShouldIncreaseFromLsn() {
        return shouldIncreaseFromLsn;
    }

    public enum StreamingResultStatus {
        STREAMING_NOT_ENABLED,
        NO_CHANGES_IN_DATABASE,
        NO_MAXIMUM_LSN_RECORDED,
        CHANGES_IN_DATABASE
    }
}
