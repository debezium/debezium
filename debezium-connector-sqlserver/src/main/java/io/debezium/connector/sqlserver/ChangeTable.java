/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import io.debezium.relational.TableId;

public class ChangeTable {
    private static final String CDC_SCHEMA = "cdc";

    private final String captureInstance;
    private final TableId sourceTableId;
    private final TableId changeTableId;
    private final Lsn startLsn;
    private final int changeTableObjectId;
    private Lsn stopLsn;

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
