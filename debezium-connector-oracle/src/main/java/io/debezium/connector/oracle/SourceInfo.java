/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.time.Instant;

import org.apache.kafka.connect.data.Struct;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.relational.TableId;

@NotThreadSafe
public class SourceInfo extends AbstractSourceInfo {

    public static final String TXID_KEY = "txId";
    public static final String SCN_KEY = "scn";
    public static final String LCR_POSITION_KEY = "lcr_position";
    public static final String SNAPSHOT_KEY = "snapshot";

    private long scn;
    private LcrPosition lcrPosition;
    private String transactionId;
    private Instant sourceTime;
    private boolean snapshot;
    private TableId tableId;

    protected SourceInfo(OracleConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    /**
     * @return the coordinates encoded as a {@code Struct}
     */
    public Struct struct() {
        return structMaker().struct(this);
    }

    public long getScn() {
        return scn;
    }

    public void setScn(long scn) {
        this.scn = scn;
    }

    public LcrPosition getLcrPosition() {
        return lcrPosition;
    }

    public void setLcrPosition(LcrPosition lcrPosition) {
        this.lcrPosition = lcrPosition;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public Instant getSourceTime() {
        return sourceTime;
    }

    public void setSourceTime(Instant sourceTime) {
        this.sourceTime = sourceTime;
    }

    public void setSnapshot(boolean snapshot) {
        this.snapshot = snapshot;
    }

    public boolean isSnapshot() {
        return snapshot;
    }

    public TableId getTableId() {
        return tableId;
    }

    public void setTableId(TableId tableId) {
        this.tableId = tableId;
    }

    @Override
    protected Instant timestamp() {
        return sourceTime;
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
