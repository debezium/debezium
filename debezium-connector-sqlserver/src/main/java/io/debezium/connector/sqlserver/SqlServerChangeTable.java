/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import io.debezium.relational.ChangeTable;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * A logical representation of change table containing changes for a given source table.
 * There is usually one change table for each source table. When the schema of the source table
 * is changed then two change tables could be present.
 *
 * @author Jiri Pechanec
 *
 */
public class SqlServerChangeTable extends ChangeTable {

    private static final String CDC_SCHEMA = "cdc";

    /**
     * A LSN from which the data in the change table are relevant
     */
    private final Lsn startLsn;

    /**
     * A LSN to which the data in the change table are relevant
     */
    private Lsn stopLsn;

    /**
     * The table from which the changes are captured
     */
    private Table sourceTable;

    public SqlServerChangeTable(TableId sourceTableId, String captureInstance, int changeTableObjectId, Lsn startLsn, Lsn stopLsn) {
        super(captureInstance, sourceTableId, resolveChangeTableId(sourceTableId, captureInstance), changeTableObjectId);
        this.startLsn = startLsn;
        this.stopLsn = stopLsn;
    }

    public SqlServerChangeTable(String captureInstance, int changeTableObjectId, Lsn startLsn, Lsn stopLsn) {
        this(null, captureInstance, changeTableObjectId, startLsn, stopLsn);
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

    public Table getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(Table sourceTable) {
        this.sourceTable = sourceTable;
    }

    @Override
    public String toString() {
        return "Capture instance \"" + getCaptureInstance() + "\" [sourceTableId=" + getSourceTableId()
                + ", changeTableId=" + getChangeTableId() + ", startLsn=" + startLsn + ", changeTableObjectId="
                + getChangeTableObjectId() + ", stopLsn=" + stopLsn + "]";
    }

    private static TableId resolveChangeTableId(TableId sourceTableId, String captureInstance) {
        return sourceTableId != null ? new TableId(sourceTableId.catalog(), CDC_SCHEMA, captureInstance + "_CT") : null;
    }
}
