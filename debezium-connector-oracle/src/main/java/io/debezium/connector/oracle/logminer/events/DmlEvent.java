/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.events;

import java.time.Instant;
import java.util.Arrays;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.relational.TableId;

/**
 * An event that represents a data modification (DML).
 *
 * @author Chris Cranford
 */
public class DmlEvent extends LogMinerEvent {

    private final Object[] oldValues;
    private final Object[] newValues;

    public DmlEvent(LogMinerEventRow row, LogMinerDmlEntry dmlEntry) {
        super(row);
        this.oldValues = dmlEntry.getOldValues();
        this.newValues = dmlEntry.getNewValues();
    }

    public DmlEvent(EventType eventType, Scn scn, TableId tableId, String rowId, String rsId, Instant changeTime,
                    Object[] oldValues, Object[] newValues) {
        super(eventType, scn, tableId, rowId, rsId, changeTime);
        this.oldValues = oldValues;
        this.newValues = newValues;
    }

    public Object[] getOldValues() {
        return oldValues;
    }

    public Object[] getNewValues() {
        return newValues;
    }

    @Override
    public String toString() {
        return "DmlEvent{" +
                "oldValues=" + Arrays.toString(oldValues) +
                ",newValues=" + Arrays.toString(newValues) +
                "} " + super.toString();
    }
}
