/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.Objects;

/**
 * A logical representation of a change table containing changes for a given source table.
 * There is usually one change table for each source table.  When the schema of the source table is changed,
 * then two change tables could be present.
 *
 * @author Jiri Pechanec
 * @author Chris Cranford
 */
public class ChangeTable {

    private final String captureInstance;
    private final TableId sourceTableId;
    private final TableId changeTableId;
    private final int changeTableObjectId;

    /**
     * The table from which the changes are captured
     */
    private Table sourceTable;

    /**
     * Creates an object that represents a source table's change table.
     *
     * @param captureInstance the logical name of the change capture process
     * @param sourceTableId the table from which the changes are captured
     * @param changeTableId the table that contains the changes for the source table
     * @param changeTableObjectId the numeric identifier for the change table in the source database
     */
    public ChangeTable(String captureInstance, TableId sourceTableId, TableId changeTableId, int changeTableObjectId) {
        this.captureInstance = captureInstance;
        this.sourceTableId = sourceTableId;
        this.changeTableId = changeTableId;
        this.changeTableObjectId = changeTableObjectId;
    }

    public String getCaptureInstance() {
        return captureInstance;
    }

    public TableId getSourceTableId() {
        return sourceTableId;
    }

    public TableId getChangeTableId() {
        return changeTableId;
    }

    public int getChangeTableObjectId() {
        return changeTableObjectId;
    }

    public Table getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(Table sourceTable) {
        this.sourceTable = sourceTable;
    }

    @Override
    public int hashCode() {
        return Objects.hash(captureInstance, sourceTableId, changeTableId, changeTableObjectId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ChangeTable) {
            ChangeTable that = (ChangeTable) obj;
            return captureInstance.equals(that.captureInstance)
                    && sourceTableId.equals(that.sourceTableId)
                    && changeTableId.equals(that.changeTableId)
                    && changeTableObjectId == that.changeTableObjectId;
        }
        return false;
    }

    @Override
    public String toString() {
        return "ChangeTable{" +
                "captureInstance='" + captureInstance + '\'' +
                ", sourceTableId=" + sourceTableId +
                ", changeTableId=" + changeTableId +
                ", changeTableObjectId=" + changeTableObjectId +
                '}';
    }
}
