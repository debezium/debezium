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
 * A LogMiner event that represents a {@code LOB_WRITE} operation.
 *
 * @author Chris Cranford
 */
public class LobWriteEvent extends LogMinerEvent {

    private final String data;

    public LobWriteEvent(LogMinerEventRow row, String data) {
        super(row);
        this.data = data;
    }

    public LobWriteEvent(EventType eventType, Scn scn, TableId tableId, String rowId, String rsId, Instant changeTime, String data) {
        super(eventType, scn, tableId, rowId, rsId, changeTime);
        this.data = data;
    }

    public String getData() {
        return data;
    }

    @Override
    public String toString() {
        return "LobWriteEvent{" +
                "data='" + data + '\'' +
                "} " + super.toString();
    }
}
