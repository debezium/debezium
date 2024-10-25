/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.SnapshotType;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;

public class OracleOffsetContext extends CommonOffsetContext<SourceInfo> {

    public static final String SNAPSHOT_PENDING_TRANSACTIONS_KEY = "snapshot_pending_tx";
    public static final String SNAPSHOT_SCN_KEY = "snapshot_scn";

    private final Schema sourceInfoSchema;

    private final TransactionContext transactionContext;
    private final IncrementalSnapshotContext<TableId> incrementalSnapshotContext;

    /**
     * SCN that was used for the initial consistent snapshot.
     *
     * We keep track of this field because it's a cutoff for emitting DDL statements,
     * in case we start mining _before_ the snapshot SCN to cover transactions that were
     * ongoing at the time the snapshot was taken.
     */
    private final Scn snapshotScn;

    /**
     * Map of (txid, start SCN) for all transactions in progress at the time the
     * snapshot was taken.
     */
    private Map<String, Scn> snapshotPendingTransactions;

    public OracleOffsetContext(OracleConnectorConfig connectorConfig, Scn scn, Long scnIndex, CommitScn commitScn, String lcrPosition,
                               Scn snapshotScn, Map<String, Scn> snapshotPendingTransactions,
                               SnapshotType snapshot, boolean snapshotCompleted, TransactionContext transactionContext,
                               IncrementalSnapshotContext<TableId> incrementalSnapshotContext) {
        this(connectorConfig, scn, scnIndex, lcrPosition, snapshotScn, snapshotPendingTransactions, snapshot, snapshotCompleted, transactionContext,
                incrementalSnapshotContext);
        sourceInfo.setCommitScn(commitScn);
    }

    public OracleOffsetContext(OracleConnectorConfig connectorConfig, Scn scn, Long scnIndex, String lcrPosition,
                               Scn snapshotScn, Map<String, Scn> snapshotPendingTransactions,
                               SnapshotType snapshot, boolean snapshotCompleted, TransactionContext transactionContext,
                               IncrementalSnapshotContext<TableId> incrementalSnapshotContext) {
        super(new SourceInfo(connectorConfig), snapshotCompleted);
        sourceInfo.setScn(scn);
        sourceInfo.setScnIndex(scnIndex);
        // It is safe to set this value to the supplied SCN, specifically for snapshots.
        // During streaming this value will be updated by the current event handler.
        sourceInfo.setEventScn(scn);
        sourceInfo.setLcrPosition(lcrPosition);
        sourceInfo.setCommitScn(CommitScn.valueOf((String) null));
        sourceInfoSchema = sourceInfo.schema();

        // Snapshot SCN is a new field and may be null in cases where the offsets are being read from
        // and older version of Debezium. In this case, we need to explicitly enforce Scn#NULL usage
        // when the value is null.
        this.snapshotScn = snapshotScn == null ? Scn.NULL : snapshotScn;
        this.snapshotPendingTransactions = snapshotPendingTransactions;

        this.transactionContext = transactionContext;
        this.incrementalSnapshotContext = incrementalSnapshotContext;

        if (this.snapshotCompleted) {
            postSnapshotCompletion();
        }
        else {
            setSnapshot(snapshot);
            sourceInfo.setSnapshot(snapshot != null ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }
    }

    public static class Builder {

        private OracleConnectorConfig connectorConfig;
        private Scn scn;
        private Long scnIndex;
        private String lcrPosition;
        private SnapshotType snapshot;
        private boolean snapshotCompleted;
        private TransactionContext transactionContext;
        private IncrementalSnapshotContext<TableId> incrementalSnapshotContext;
        private Map<String, Scn> snapshotPendingTransactions;
        private Scn snapshotScn;

        public Builder logicalName(OracleConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
            return this;
        }

        public Builder scn(Scn scn) {
            this.scn = scn;
            return this;
        }

        public Builder scnIndex(Long scnIndex) {
            this.scnIndex = scnIndex;
            return this;
        }

        public Builder lcrPosition(String lcrPosition) {
            this.lcrPosition = lcrPosition;
            return this;
        }

        public Builder snapshot(SnapshotType snapshot) {
            this.snapshot = snapshot;
            return this;
        }

        public Builder snapshotCompleted(boolean snapshotCompleted) {
            this.snapshotCompleted = snapshotCompleted;
            return this;
        }

        public Builder transactionContext(TransactionContext transactionContext) {
            this.transactionContext = transactionContext;
            return this;
        }

        public Builder incrementalSnapshotContext(IncrementalSnapshotContext<TableId> incrementalSnapshotContext) {
            this.incrementalSnapshotContext = incrementalSnapshotContext;
            return this;
        }

        public Builder snapshotPendingTransactions(Map<String, Scn> snapshotPendingTransactions) {
            this.snapshotPendingTransactions = snapshotPendingTransactions;
            return this;
        }

        public Builder snapshotScn(Scn scn) {
            this.snapshotScn = scn;
            return this;
        }

        public OracleOffsetContext build() {
            return new OracleOffsetContext(connectorConfig, scn, scnIndex, lcrPosition, snapshotScn,
                    snapshotPendingTransactions, snapshot, snapshotCompleted, transactionContext,
                    incrementalSnapshotContext);
        }
    }

    public static Builder create() {
        return new Builder();
    }

    @Override
    public Map<String, ?> getOffset() {
        if (getSnapshot().isPresent()) {
            Map<String, Object> offset = new HashMap<>();

            final Scn scn = sourceInfo.getScn();
            offset.put(SourceInfo.SCN_KEY, scn != null ? scn.toString() : scn);
            offset.put(AbstractSourceInfo.SNAPSHOT_KEY, getSnapshot().get().toString());
            offset.put(SNAPSHOT_COMPLETED_KEY, snapshotCompleted);

            if (snapshotPendingTransactions != null && !snapshotPendingTransactions.isEmpty()) {
                String encoded = snapshotPendingTransactions.entrySet().stream()
                        .map(e -> e.getKey() + ":" + e.getValue().toString())
                        .collect(Collectors.joining(","));
                offset.put(SNAPSHOT_PENDING_TRANSACTIONS_KEY, encoded);
            }
            offset.put(SNAPSHOT_SCN_KEY, snapshotScn != null ? snapshotScn.isNull() ? null : snapshotScn.toString() : null);

            return offset;
        }
        else {
            final Map<String, Object> offset = new HashMap<>();
            if (sourceInfo.getLcrPosition() != null) {
                offset.put(SourceInfo.LCR_POSITION_KEY, sourceInfo.getLcrPosition());
            }
            else {
                final Scn scn = sourceInfo.getScn();
                offset.put(SourceInfo.SCN_KEY, scn != null ? scn.toString() : null);
                if (sourceInfo.getScnIndex() != null) {
                    offset.put(SourceInfo.SCN_INDEX_KEY, sourceInfo.getScnIndex());
                }
                sourceInfo.getCommitScn().store(offset);
            }
            if (snapshotPendingTransactions != null && !snapshotPendingTransactions.isEmpty()) {
                String encoded = snapshotPendingTransactions.entrySet().stream()
                        .map(e -> e.getKey() + ":" + e.getValue().toString())
                        .collect(Collectors.joining(","));
                offset.put(SNAPSHOT_PENDING_TRANSACTIONS_KEY, encoded);
            }
            offset.put(SNAPSHOT_SCN_KEY, snapshotScn != null ? snapshotScn.isNull() ? null : snapshotScn.toString() : null);

            return incrementalSnapshotContext.store(transactionContext.store(offset));
        }
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    public void setScn(Scn scn) {
        sourceInfo.setScn(scn);
    }

    public void setScnIndex(Long scnIndex) {
        sourceInfo.setScnIndex(scnIndex);
    }

    public void setEventScn(Scn eventScn) {
        sourceInfo.setEventScn(eventScn);
    }

    public Scn getScn() {
        return sourceInfo.getScn();
    }

    public Long getScnIndex() {
        return sourceInfo.getScnIndex();
    }

    public CommitScn getCommitScn() {
        return sourceInfo.getCommitScn();
    }

    public Scn getEventScn() {
        return sourceInfo.getEventScn();
    }

    public void setLcrPosition(String lcrPosition) {
        sourceInfo.setLcrPosition(lcrPosition);
    }

    public String getLcrPosition() {
        return sourceInfo.getLcrPosition();
    }

    public Scn getSnapshotScn() {
        return snapshotScn;
    }

    public Map<String, Scn> getSnapshotPendingTransactions() {
        return snapshotPendingTransactions;
    }

    public void setSnapshotPendingTransactions(Map<String, Scn> snapshotPendingTransactions) {
        this.snapshotPendingTransactions = snapshotPendingTransactions;
    }

    public void setTransactionId(String transactionId) {
        sourceInfo.setTransactionId(transactionId);
    }

    public void setUserName(String userName) {
        sourceInfo.setUserName(userName);
    }

    public void setSourceTime(Instant instant) {
        sourceInfo.setSourceTime(instant);
    }

    public void setTableId(TableId tableId) {
        sourceInfo.tableEvent(tableId);
    }

    public Integer getRedoThread() {
        return sourceInfo.getRedoThread();
    }

    public void setRedoThread(Integer redoThread) {
        sourceInfo.setRedoThread(redoThread);
    }

    public void setRsId(String rsId) {
        sourceInfo.setRsId(rsId);
    }

    public void setSsn(long ssn) {
        sourceInfo.setSsn(ssn);
    }

    public String getRedoSql() {
        return sourceInfo.getRedoSql();
    }

    public void setRedoSql(String redoSql) {
        sourceInfo.setRedoSql(redoSql);
    }

    public String getRowId() {
        return sourceInfo.getRowId();
    }

    public void setRowId(String rowId) {
        sourceInfo.setRowId(rowId);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("OracleOffsetContext [scn=").append(getScn());

        if (getSnapshot().isPresent()) {
            sb.append(", snapshot=").append(getSnapshot().get());
            sb.append(", snapshot_completed=").append(snapshotCompleted);
        }
        else if (getScnIndex() != null) {
            sb.append(", scnIndex=").append(getScnIndex());
        }

        sb.append(", commit_scn=").append(sourceInfo.getCommitScn().toLoggableFormat());
        sb.append(", lcr_position=").append(sourceInfo.getLcrPosition());

        sb.append("]");

        return sb.toString();
    }

    @Override
    public void event(DataCollectionId tableId, Instant timestamp) {
        sourceInfo.tableEvent((TableId) tableId);
        sourceInfo.setSourceTime(timestamp);
    }

    public void tableEvent(TableId tableId, Instant timestamp) {
        sourceInfo.setSourceTime(timestamp);
        sourceInfo.tableEvent(tableId);
    }

    public void tableEvent(Set<TableId> tableIds, Instant timestamp) {
        sourceInfo.setSourceTime(timestamp);
        sourceInfo.tableEvent(tableIds);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return incrementalSnapshotContext;
    }

    /**
     * Helper method to resolve a {@link Scn} by key from the offset map.
     *
     * @param offset the offset map
     * @param key the entry key, either {@link SourceInfo#SCN_KEY} or {@link SourceInfo#COMMIT_SCN_KEY}.
     * @return the {@link Scn} or null if not found
     */
    public static Scn getScnFromOffsetMapByKey(Map<String, ?> offset, String key) {
        Object scn = offset.get(key);
        if (scn instanceof String) {
            return Scn.valueOf((String) scn);
        }
        else if (scn != null) {
            return Scn.valueOf((Long) scn);
        }
        return null;
    }

    /**
     * Helper method to read the in-progress transaction map from the offset map.
     *
     * @param offset the offset map
     * @return the in-progress transaction map
     */
    public static Map<String, Scn> loadSnapshotPendingTransactions(Map<String, ?> offset) {
        Map<String, Scn> snapshotPendingTransactions = new HashMap<>();
        String encoded = (String) offset.get(SNAPSHOT_PENDING_TRANSACTIONS_KEY);
        if (encoded != null) {
            Arrays.stream(encoded.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .forEach(e -> {
                        String[] parts = e.split(":", 2);
                        String txid = parts[0];
                        Scn startScn = Scn.valueOf(parts[1]);
                        snapshotPendingTransactions.put(txid, startScn);
                    });
        }
        return snapshotPendingTransactions;
    }

    /**
     * Helper method to read the snapshot SCN from the offset map.
     *
     * @param offset the offset map
     * @return the snapshot SCN
     */
    public static Scn loadSnapshotScn(Map<String, ?> offset) {
        return getScnFromOffsetMapByKey(offset, SNAPSHOT_SCN_KEY);
    }
}
