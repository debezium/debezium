package io.debezium.connector.sqlserver;

import io.debezium.relational.TableId;

public class ChangeTable {
    private final String captureInstance;
    private final TableId tableId;
    private final Lsn startLsn;
    private final Lsn stopLsn;

    public ChangeTable(TableId tableId, String captureInstance, Lsn startLsn, Lsn stopLsn) {
        super();
        this.captureInstance = captureInstance;
        this.startLsn = startLsn;
        this.stopLsn = stopLsn;
        this.tableId = tableId;
    }

    public ChangeTable(String captureInstance, Lsn startLsn, Lsn stopLsn) {
        this(null, captureInstance, startLsn, stopLsn);
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

    public TableId getTableId() {
        return tableId;
    }

    @Override
    public String toString() {
        return "ChangeTable [captureInstance=" + captureInstance + ", tableId=" + tableId + ", startLsn=" + startLsn
                + ", stopLsn=" + stopLsn + "]";
    }
}
