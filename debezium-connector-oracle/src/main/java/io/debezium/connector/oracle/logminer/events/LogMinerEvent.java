/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.events;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

import io.debezium.connector.oracle.Scn;
import io.debezium.relational.TableId;

/**
 * Base event class for all events read from Oracle LogMiner
 *
 * @author Chris Cranford
 */
public class LogMinerEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private final EventType eventType;
    private final Scn scn;
    private final TableId tableId;
    private final String rowId;
    private final String rsId;
    private final Instant changeTime;

    public LogMinerEvent(LogMinerEventRow row) {
        this(row.getEventType(), row.getScn(), row.getTableId(), row.getRowId(), row.getRsId(), row.getChangeTime());
    }

    public LogMinerEvent(EventType eventType, Scn scn, TableId tableId, String rowId, String rsId, Instant changeTime) {
        this.eventType = eventType;
        this.scn = scn;
        this.tableId = tableId;
        this.rowId = rowId;
        this.rsId = rsId;
        this.changeTime = changeTime;
    }

    public EventType getEventType() {
        return eventType;
    }

    public Scn getScn() {
        return scn;
    }

    public TableId getTableId() {
        return tableId;
    }

    public String getRowId() {
        return rowId;
    }

    public String getRsId() {
        return rsId;
    }

    public Instant getChangeTime() {
        return changeTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogMinerEvent that = (LogMinerEvent) o;
        return eventType == that.eventType &&
                Objects.equals(scn, that.scn) &&
                Objects.equals(tableId, that.tableId) &&
                Objects.equals(rowId, that.rowId) &&
                Objects.equals(rsId, that.rsId) &&
                Objects.equals(changeTime, that.changeTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventType, scn, tableId, rowId, rsId, changeTime);
    }

    @Override
    public String toString() {
        return "LogMinerEvent{" +
                "eventType=" + eventType +
                ", scn=" + scn +
                ", tableId=" + tableId +
                ", rowId='" + rowId + '\'' +
                ", rsId=" + rsId +
                ", changeTime=" + changeTime +
                '}';
    }
}
