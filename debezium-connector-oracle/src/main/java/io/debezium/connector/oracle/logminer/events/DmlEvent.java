/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.events;

import java.time.Instant;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.relational.TableId;

/**
 * An event that represents a data modification (DML).
 *
 * @author Chris Cranford
 */
public class DmlEvent extends LogMinerEvent {

    private final LogMinerDmlEntry dmlEntry;

    public DmlEvent(LogMinerEventRow row, LogMinerDmlEntry dmlEntry) {
        super(row);
        this.dmlEntry = dmlEntry;
    }

    public DmlEvent(EventType eventType, Scn scn, TableId tableId, String rowId, String rsId, Instant changeTime, LogMinerDmlEntry dmlEntry) {
        super(eventType, scn, tableId, rowId, rsId, changeTime);
        this.dmlEntry = dmlEntry;
    }

    public LogMinerDmlEntry getDmlEntry() {
        return dmlEntry;
    }

    @Override
    public String toString() {
        return "DmlEvent{" +
                "dmlEntry=" + dmlEntry +
                "} " + super.toString();
    }
}
