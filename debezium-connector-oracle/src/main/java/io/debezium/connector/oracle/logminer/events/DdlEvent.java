/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.events;

/**
 * A schema change that the buffered adapter applies when its transaction commits.
 * <p>
 * It exists only between the transaction buffer and the commit consumer: the buffer stores the
 * mined row as a {@link LogMinerRedoEvent}, and this is the form the commit consumer dispatches.
 *
 * @author Chris Cranford
 */
public class DdlEvent extends LogMinerEvent {

    private final String redoSql;
    private final long objectId;
    private final long dataObjectId;

    public DdlEvent(LogMinerEventRow row) {
        super(row);
        this.redoSql = row.getRedoSql();
        this.objectId = row.getObjectId();
        this.dataObjectId = row.getDataObjectId();
    }

    /**
     * @return the DDL statement, never {@code null}
     */
    public String getRedoSql() {
        return redoSql;
    }

    public long getObjectId() {
        return objectId;
    }

    public long getDataObjectId() {
        return dataObjectId;
    }

    @Override
    public String toString() {
        return "DdlEvent{" +
                "redoSql='" + redoSql + '\'' +
                ", objectId=" + objectId +
                ", dataObjectId=" + dataObjectId +
                "} " + super.toString();
    }
}
