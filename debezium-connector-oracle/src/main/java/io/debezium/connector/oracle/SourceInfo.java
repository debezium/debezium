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
    public static final String LCR_POSITION_KEY = "lcr_position";
    public static final String SNAPSHOT_KEY = "snapshot";
    public static final String USERNAME_KEY = "user_name";

    private Scn scn;
    private CommitScn commitScn;
    private Scn eventScn;
    private String lcrPosition;
    private String transactionId;
    private String userName;
    private Instant sourceTime;
    private Set<TableId> tableIds;
    private Integer redoThread;
    private String rsId;
    private long ssn;

    protected SourceInfo(OracleConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    public Scn getScn() {
        return scn;
    }

    public CommitScn getCommitScn() {
        return commitScn;
    }

    public Scn getEventScn() {
        return eventScn;
    }

    public void setScn(Scn scn) {
        this.scn = scn;
    }

    public void setCommitScn(CommitScn commitScn) {
        this.commitScn = commitScn;
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

    public String tableSchema() {
        return tableIds.isEmpty() ? null
                : tableIds.stream()
                        .filter(x -> x != null)
                        .map(TableId::schema)
                        .distinct()
                        .collect(Collectors.joining(","));
    }

    public String table() {
        return tableIds.isEmpty() ? null
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

    @Override
    protected Instant timestamp() {
        return sourceTime;
    }

    @Override
    protected String database() {
        return tableIds.iterator().next().catalog();
    }
}
