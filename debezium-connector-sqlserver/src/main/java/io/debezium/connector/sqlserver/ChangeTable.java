/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import io.debezium.relational.TableId;

/**
 * A logical representation of change table containing changes for a given source table.
 * There is usually one change table for each source table. When the schema of the source table
 * is changed then two change tables could be present.
 *
 * @author Jiri Pechanec
 *
 */
public class ChangeTable {
    private static final String CDC_SCHEMA = "cdc";

    /**
     * The logical name of the change capture process
     */
    private final String captureInstance;

    /**
     * The table from which the changes are captured
     */
    private final TableId sourceTableId;

    /**
     * The table that contains the changes for the source table
     */
    private final TableId changeTableId;

    /**
     * A LSN from which the data in the change table are relevant
     */
    private final Lsn startLsn;

    /**
     * A LSN to which the data in the change table are relevant
     */
    private Lsn stopLsn;

    /**
     * Numeric identifier of change table in SQL Server schema
     */
    private final int changeTableObjectId;

    public ChangeTable(TableId sourceTableId, String captureInstance, int changeTableObjectId, Lsn startLsn, Lsn stopLsn) {
        super();
        this.sourceTableId = sourceTableId;
        this.captureInstance = captureInstance;
        this.changeTableObjectId = changeTableObjectId;
        this.startLsn = startLsn;
        this.stopLsn = stopLsn;
        this.changeTableId = sourceTableId != null ? new TableId(sourceTableId.catalog(), CDC_SCHEMA, captureInstance + "_CT") : null;
    }

    public ChangeTable(String captureInstance, int changeTableObjectId, Lsn startLsn, Lsn stopLsn) {
        this(null, captureInstance, changeTableObjectId, startLsn, stopLsn);
    }

    public String getCaptureInstance() {
        return captureInstance;
    }
    public Lsn getStartLsn() {
        return startLsn;
    }
    public Lsn getStopLsn() {
        return stopLsn;
    }

    public void setStopLsn(Lsn stopLsn) {
        this.stopLsn = stopLsn;
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

    @Override
    public String toString() {
        return "ChangeTable [captureInstance=" + captureInstance + ", sourceTableId=" + sourceTableId
                + ", changeTableId=" + changeTableId + ", startLsn=" + startLsn + ", changeTableObjectId="
                + changeTableObjectId + ", stopLsn=" + stopLsn + "]";
    }
}
