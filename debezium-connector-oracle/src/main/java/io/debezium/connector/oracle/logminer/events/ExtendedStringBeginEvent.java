/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.events;

import java.time.Instant;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntryImpl;
import io.debezium.relational.TableId;

/**
 * Represents a {@code 32K_BEGIN} LogMiner event.
 *
 * @author Chris Cranford
 */
public class ExtendedStringBeginEvent extends DmlEvent {

    private final String columnName;

    public ExtendedStringBeginEvent(LogMinerEventRow row, LogMinerDmlEntry dmlEntry, String columnName) {
        super(row, dmlEntry);
        this.columnName = columnName;
    }

    public ExtendedStringBeginEvent(EventType eventType, Scn scn, TableId tableId, String rowId, String rsId, Instant changeTime, LogMinerDmlEntryImpl entry,
                                    String columnName) {
        super(eventType, scn, tableId, rowId, rsId, changeTime, entry);
        this.columnName = columnName;
    }

    public String getColumnName() {
        return columnName;
    }

    @Override
    public String toString() {
        return "ExtendedStringBeginEvent{" +
                "columnName='" + columnName + '\'' +
                "} " + super.toString();
    }
}
