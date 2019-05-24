/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.time.Instant;

import org.apache.kafka.connect.data.Struct;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.relational.TableId;

/**
 * Coordinates from the database log to establish the relation between the change streamed and the source log position.
 * Maps to {@code source} field in {@code Envelope}.
 *
 * @author Jiri Pechanec
 *
 */
@NotThreadSafe
public class SourceInfo extends AbstractSourceInfo {

    public static final String CHANGE_LSN_KEY = "change_lsn";
    public static final String COMMIT_LSN_KEY = "commit_lsn";

    private Lsn changeLsn;
    private Lsn commitLsn;
    private boolean snapshot;
    private Instant sourceTime;
    private TableId tableId;

    protected SourceInfo(SqlServerConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    /**
     * @param lsn - LSN of the change in the database log
     */
    public void setChangeLsn(Lsn lsn) {
        changeLsn = lsn;
    }

    public Lsn getChangeLsn() {
        return changeLsn;
    }

    public Lsn getCommitLsn() {
        return commitLsn;
    }

    /**
     * @param commitLsn - LSN of the {@code COMMIT} of the transaction whose part the change is
     */
    public void setCommitLsn(Lsn commitLsn) {
        this.commitLsn = commitLsn;
    }

    /**
     * @param instant a time at which the transaction commit was executed
     */
    public void setSourceTime(Instant instant) {
        sourceTime = instant;
    }

    public Instant getSourceTime() {
        return sourceTime;
    }

    public boolean isSnapshot() {
        return snapshot;
    }

    /**
     * @param snapshot - true if the source of even is snapshot phase, not the database log
     */
    public void setSnapshot(boolean snapshot) {
        this.snapshot = snapshot;
    }

    public TableId getTableId() {
        return tableId;
    }

    /**
     * @param tableId - source table of the event
     */
    public void setTableId(TableId tableId) {
        this.tableId = tableId;
    }

    /**
     * @return the coordinates encoded as a {@code Struct}
     */
    public Struct struct() {
        return structMaker().struct(this);
    }

    @Override
    public String toString() {
        return "SourceInfo [" +
                "serverName=" + serverName() +
                ", changeLsn=" + changeLsn +
                ", commitLsn=" + commitLsn +
                ", snapshot=" + snapshot +
                ", sourceTime=" + sourceTime +
                "]";
    }

    @Override
    protected long timestamp() {
        return sourceTime.toEpochMilli();
    }

    @Override
    protected boolean snapshot() {
        return isSnapshot();
    }

    @Override
    protected String database() {
        return tableId.catalog();
    }
}
