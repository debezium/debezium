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
 * A data modification event that represents a {@code SEL_LOB_LOCATOR} event type.
 *
 * @author Chris Cranford
 */
public class SelectLobLocatorEvent extends DmlEvent {

    private final String columnName;
    private final boolean binary;

    public SelectLobLocatorEvent(LogMinerEventRow row, LogMinerDmlEntry dmlEntry, String columnName, boolean binary) {
        super(row, dmlEntry);
        this.columnName = columnName;
        this.binary = binary;
    }

    public SelectLobLocatorEvent(EventType eventType, Scn scn, TableId tableId, String rowId, String rsId, Instant changeTime,
                                 LogMinerDmlEntry dmlEntry, String columnName, boolean binary) {
        super(eventType, scn, tableId, rowId, rsId, changeTime, dmlEntry);
        this.columnName = columnName;
        this.binary = binary;
    }

    public String getColumnName() {
        return columnName;
    }

    public boolean isBinary() {
        return binary;
    }

    @Override
    public String toString() {
        return "SelectLobLocatorEvent{" +
                "columnName='" + columnName + '\'' +
                ", binary=" + binary +
                "} " + super.toString();
    }
}
