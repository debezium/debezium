/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.relational.TableId;

@NotThreadSafe
public class SourceInfo extends BaseSourceInfo {

    public static final String TXID_KEY = "txId";
    public static final String SCN_KEY = "scn";
    public static final String EVENT_SCN_KEY = "scn";
    public static final String COMMIT_SCN_KEY = "commit_scn";
    public static final String COMMIT_TIMESTAMP_KEY = "commit_ts_ms";
    public static final String LCR_POSITION_KEY = "lcr_position";
    public static final String SNAPSHOT_KEY = "snapshot";
    public static final String USERNAME_KEY = "user_name";
    public static final String SCN_INDEX_KEY = "scn_idx";
    public static final String REDO_SQL = "redo_sql";
    public static final String ROW_ID = "row_id";
    public static final String START_SCN_KEY = "start_scn";
    public static final String START_TIMESTAMP_KEY = "start_ts_ms";

    // Tracks thread-specific values when using multiple threads during snapshot
    private final ThreadLocal<String> rowId = new ThreadLocal<>();

    private Scn scn;
    private CommitScn commitScn;
    private Scn eventScn;
    private Scn startScn;
    private String lcrPosition;
    private String transactionId;
    private String userName;
    private Instant sourceTime;
    private Instant commitTime;
    private Instant startTime;
    private Set<TableId> tableIds;
    private Integer redoThread;
    private String rsId;
    private long ssn;
    private Long scnIndex;
    private String redoSql;

    protected SourceInfo(OracleConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    public Scn getScn() {
        return scn;
    }

    public Scn getStartScn() {
        return startScn;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public CommitScn getCommitScn() {
        return commitScn;
    }

    public Instant getCommitTime() {
        return commitTime;
    }

    public Scn getEventScn() {
        return eventScn;
    }

    public void setScn(Scn scn) {
        this.scn = scn;
    }

    public void setStartScn(Scn startScn) {
        this.startScn = startScn;
    }

    public void setStartTime(Instant startTime) {
        this.startTime = startTime;
    }

    public void setCommitScn(CommitScn commitScn) {
        this.commitScn = commitScn;
    }

    public void setCommitTime(Instant commitTime) {
        this.commitTime = commitTime;
    }

    public void setEventScn(Scn eventScn) {
        this.eventScn = eventScn;
    }

    public String getLcrPosition() {
        return lcrPosition;
    }

    public void setLcrPosition(String lcrPosition) {
        this.lcrPosition = lcrPosition;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getRsId() {
        return rsId;
    }

    public void setRsId(String rsId) {
        this.rsId = rsId;
    }

    public long getSsn() {
        return ssn;
    }

    public void setSsn(long ssn) {
        this.ssn = ssn;
    }

    public Instant getSourceTime() {
        return sourceTime;
    }

    public void setSourceTime(Instant sourceTime) {
        this.sourceTime = sourceTime;
    }

    public String getRedoSql() {
        return redoSql;
    }

    public void setRedoSql(String redoSql) {
        this.redoSql = redoSql;
    }

    public String getRowId() {
        return rowId.get();
    }

    public void setRowId(String rowId) {
        this.rowId.set(rowId);
    }

    public String tableSchema() {
        return (tableIds == null || tableIds.isEmpty()) ? null
                : tableIds.stream()
                        .filter(x -> x != null)
                        .map(TableId::schema)
                        .distinct()
                        .collect(Collectors.joining(","));
    }

    public String table() {
        return (tableIds == null || tableIds.isEmpty()) ? null
                : tableIds.stream()
                        .filter(x -> x != null)
                        .map(TableId::table)
                        .collect(Collectors.joining(","));
    }

    public void tableEvent(Set<TableId> tableIds) {
        this.tableIds = new LinkedHashSet<>(tableIds);
    }

    public void tableEvent(TableId tableId) {
        this.tableIds = Collections.singleton(tableId);
    }

    public Integer getRedoThread() {
        return redoThread;
    }

    public void setRedoThread(Integer redoThread) {
        this.redoThread = redoThread;
    }

    public Long getScnIndex() {
        return scnIndex;
    }

    public void setScnIndex(Long scnIndex) {
        this.scnIndex = scnIndex;
    }

    @Override
    protected Instant timestamp() {
        return sourceTime;
    }

    @Override
    protected String database() {
        return (tableIds != null) ? tableIds.iterator().next().catalog() : null;
    }
}
