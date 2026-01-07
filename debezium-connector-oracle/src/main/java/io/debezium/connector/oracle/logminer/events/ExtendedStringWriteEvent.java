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
 * Represents a {@code 32_WRITE} LogMiner event.
 *
 * @author Chris Cranford
 */
public class ExtendedStringWriteEvent extends LogMinerEvent {

    private final String data;

    public ExtendedStringWriteEvent(LogMinerEventRow row, String data) {
        super(row);
        this.data = data;
    }

    public ExtendedStringWriteEvent(EventType eventType, Scn scn, TableId tableId, String rowId, String rsId, Instant changeTime, String data) {
        super(eventType, scn, tableId, rowId, rsId, changeTime);
        this.data = data;
    }

    public String getData() {
        return data;
    }

    @Override
    public String toString() {
        return "ExtendedStringWriteEvent{" +
                "data='" + data + '\'' +
                "} " + super.toString();
    }
}
