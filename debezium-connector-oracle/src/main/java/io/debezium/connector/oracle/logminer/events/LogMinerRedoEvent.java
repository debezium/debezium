/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.events;

import java.time.Instant;
import java.util.Objects;

import io.debezium.connector.oracle.Scn;
import io.debezium.relational.TableId;

/**
 * The mined row as the transaction buffer stores it: the row's
 * metadata plus its redo SQL, with no parsed values. It is parsed into a {@link LogMinerEvent} when its
 * transaction commits, through the row view that {@link LogMinerEventRow#fromRedoEvent} rebuilds.
 * <p>
 * Undo markers keep their row's ordinary event type (the reversing operation) and set {@link #isRollback()},
 * which is what the caches read to attribute the partial rollback.
 *
 * @author Chris Cranford
 */
public final class LogMinerRedoEvent implements CachedEvent {

    private final EventType eventType;
    private final boolean rollback;
    private final Scn scn;
    private final TableId tableId;
    private final RowIdCodec.Packed rowId;
    private final String rsId;
    private final long changeTime;
    private final String redoSql;
    private final int status;
    private final String info;
    private final long transactionSequence;
    private final long objectId;
    private final long dataObjectId;

    /**
     * Stores the row with its redo SQL.
     */
    public LogMinerRedoEvent(LogMinerEventRow row) {
        this(row, true);
    }

    /**
     * @param row the mined row
     * @param withRedoSql {@code false} for rows whose redo text is never read at commit, such as undo markers,
     *        INTERNAL, LOB_ERASE and XML_END rows, so that it is not stored
     */
    public LogMinerRedoEvent(LogMinerEventRow row, boolean withRedoSql) {
        this(row.getEventType(),
                row.isRollbackFlag(),
                row.getScn(),
                row.getTableId(),
                row.getRowId(),
                row.getRsId(),
                row.getChangeTime(),
                withRedoSql ? row.getRedoSql() : null,
                row.getStatus(),
                row.getInfo(),
                row.getTransactionSequence() == null ? 0L : row.getTransactionSequence(),
                row.getObjectId(),
                row.getDataObjectId());
    }

    public LogMinerRedoEvent(EventType eventType, boolean rollback, Scn scn, TableId tableId, String rowId, String rsId, Instant changeTime,
                             String redoSql, int status, String info, long transactionSequence, long objectId, long dataObjectId) {
        this(eventType, rollback, scn, tableId, RowIdCodec.encode(rowId), rsId, changeTime.toEpochMilli(), redoSql, status, info,
                transactionSequence, objectId, dataObjectId);
    }

    /**
     * Rebuilds an event from fields a store keeps in their packed form.
     *
     * @param rowId the packed row id, should not be {@code null}
     * @param changeTime the change time in milliseconds since the epoch
     */
    public LogMinerRedoEvent(EventType eventType, boolean rollback, Scn scn, TableId tableId, RowIdCodec.Packed rowId, String rsId, long changeTime,
                             String redoSql, int status, String info, long transactionSequence, long objectId, long dataObjectId) {
        this.eventType = eventType;
        this.rollback = rollback;
        this.scn = scn;
        this.tableId = tableId;
        this.rowId = rowId;
        this.rsId = rsId;
        this.changeTime = changeTime;
        this.redoSql = redoSql;
        this.status = status;
        this.info = info;
        this.transactionSequence = transactionSequence;
        this.objectId = objectId;
        this.dataObjectId = dataObjectId;
    }

    @Override
    public EventType getEventType() {
        return eventType;
    }

    @Override
    public boolean isRollback() {
        return rollback;
    }

    @Override
    public Scn getScn() {
        return scn;
    }

    @Override
    public TableId getTableId() {
        return tableId;
    }

    @Override
    public RowIdCodec.Packed getRowId() {
        return rowId;
    }

    @Override
    public String getRsId() {
        return rsId;
    }

    public Instant getChangeTime() {
        return Instant.ofEpochMilli(changeTime);
    }

    /**
     * @return the change time in milliseconds since the epoch, the form the event keeps it in
     */
    public long getChangeTimeMillis() {
        return changeTime;
    }

    /**
     * @return the row's redo SQL with any continuation rows joined, or {@code null} when it was not stored
     */
    public String getRedoSql() {
        return redoSql;
    }

    /**
     * @return the row's {@code STATUS}; {@code 2} marks redo SQL that LogMiner could not fully reconstruct
     */
    public int getStatus() {
        return status;
    }

    /**
     * @return the row's {@code INFO} column, which among other things says whether XML is stored as binary
     */
    public String getInfo() {
        return info;
    }

    @Override
    public long getTransactionSequence() {
        return transactionSequence;
    }

    public long getObjectId() {
        return objectId;
    }

    public long getDataObjectId() {
        return dataObjectId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final LogMinerRedoEvent that = (LogMinerRedoEvent) o;
        return eventType == that.eventType
                && rollback == that.rollback
                && changeTime == that.changeTime
                && status == that.status
                && transactionSequence == that.transactionSequence
                && objectId == that.objectId
                && dataObjectId == that.dataObjectId
                && Objects.equals(scn, that.scn)
                && Objects.equals(tableId, that.tableId)
                && Objects.equals(rowId, that.rowId)
                && Objects.equals(rsId, that.rsId)
                && Objects.equals(redoSql, that.redoSql)
                && Objects.equals(info, that.info);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventType, rollback, scn, tableId, rowId, rsId, changeTime, redoSql, status, info, transactionSequence, objectId, dataObjectId);
    }

    @Override
    public String toString() {
        return "LogMinerRedoEvent{" +
                "eventType=" + eventType +
                ", rollback=" + rollback +
                ", scn=" + scn +
                ", tableId=" + tableId +
                ", rowId='" + RowIdCodec.decode(rowId) + '\'' +
                ", rsId=" + rsId +
                ", changeTime=" + getChangeTime() +
                ", redoSql=" + redoSql +
                ", status=" + status +
                ", info=" + info +
                ", transactionSequence=" + transactionSequence +
                ", objectId=" + objectId +
                ", dataObjectId=" + dataObjectId +
                '}';
    }
}
