/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.events;

import java.time.Instant;

import io.debezium.connector.oracle.Scn;
import io.debezium.relational.TableId;

/**
 * A LogMiner event that represents a rollback to a savepoint.
 *
 * @author Sergei Nikolaev
 */
public class RollbackToSavepointEvent extends LogMinerEvent {
    public RollbackToSavepointEvent(LogMinerEventRow row) {
        super(row);
    }

    public RollbackToSavepointEvent(EventType eventType, Scn scn, TableId tableId, String rowId, String rsId, Instant changeTime) {
        super(eventType, scn, tableId, rowId, rsId, changeTime);
    }
}
