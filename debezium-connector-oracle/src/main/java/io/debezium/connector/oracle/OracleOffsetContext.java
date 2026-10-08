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

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.SnapshotType;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;

/**
 * The Oracle connector offset context.
 * <p>
 * The {@code scn} offset value is always the position where streaming resumes. For LogMiner, it is
 * the exclusive lower bound of the next mining session. The initial snapshot already sets it just
 * before the start of the oldest transaction that was in progress when the snapshot was taken, so
 * that such transactions are mined in full, and keeps the SCN the snapshot is taken at as the
 * {@link #getSnapshotCommitScn() snapshot commit SCN} to discard what the snapshot already captured.
 */
public class OracleOffsetContext extends CommonOffsetContext<SourceInfo> {

    public static final String SNAPSHOT_COMMIT_SCN_KEY = "snapshot_commit_scn";
    public static final String WINDOW_ADVANCE_ENABLED_KEY = "window_advance_enabled";

    /**
     * Written by older versions as the SCN the snapshot was taken at, which was also the offset SCN
     * until streaming persisted its first position. It is only read to migrate such offsets.
     */
    private static final String LEGACY_SNAPSHOT_SCN_KEY = "snapshot_scn";

    /**
     * Written by older versions to track the transactions in progress at the snapshot boundary.
     * It is only read to resolve where mining starts for such offsets.
     */
    private static final String LEGACY_SNAPSHOT_PENDING_TRANSACTIONS_KEY = "snapshot_pending_tx";

    private final Schema sourceInfoSchema;

    private boolean windowAdvanceEnabled;

    private final TransactionContext transactionContext;
    private final IncrementalSnapshotContext<TableId> incrementalSnapshotContext;

    /**
     * SCN the initial snapshot was taken at, LogMiner only.
     * <p>
     * Every transaction that committed at or before this SCN is already part of the snapshot and is
     * discarded by streaming, which resumes before the snapshot to mine the transactions that were
     * in progress at that time in full. It is persisted until every redo thread has committed past
     * it, after which no such transaction can be mined again.
     */
    private Scn snapshotCommitScn;

    private OracleOffsetContext(OracleConnectorConfig connectorConfig, Scn scn, Long scnIndex, CommitScn commitScn, String lcrPosition,
                                Scn snapshotCommitScn, SnapshotType snapshot,
                                boolean snapshotCompleted, TransactionContext transactionContext,
                                IncrementalSnapshotContext<TableId> incrementalSnapshotContext,
                                String transactionId, Long transactionSequence, boolean windowAdvanceEnabled) {
        super(new SourceInfo(connectorConfig), snapshotCompleted);
        sourceInfo.setScn(scn);
        sourceInfo.setScnIndex(scnIndex);
        sourceInfo.setTransactionId(transactionId);
        sourceInfo.setTransactionSequence(transactionSequence);
        this.windowAdvanceEnabled = windowAdvanceEnabled;
        sourceInfo.setLcrPosition(lcrPosition);
        sourceInfo.setCommitScn(commitScn);
        sourceInfoSchema = sourceInfo.schema();

        // The snapshot commit SCN may be null in cases where the offsets are being read from an older
        // version of Debezium, after it has been retired, or for adapters that do not track it. In this
        // case, we need to explicitly enforce Scn#NULL usage when the value is null.
        this.snapshotCommitScn = snapshotCommitScn == null ? Scn.NULL : snapshotCommitScn;

        // Snapshot records are read as of the SCN the snapshot is taken at, which is their event SCN.
        // During streaming this value will be updated by the current event handler.
        sourceInfo.setEventScn(getSnapshotAsOfScn());

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
        private Scn snapshotCommitScn;
        private String transactionId;
        private Long transactionSequence;
        private CommitScn commitScn = CommitScn.empty();
        private boolean windowAdvanceEnabled;

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

        public Builder snapshotCommitScn(Scn scn) {
            this.snapshotCommitScn = scn;
            return this;
        }

        public Builder transactionId(String transactionId) {
            this.transactionId = transactionId;
            return this;
        }

        public Builder transactionSequence(Long transactionSequence) {
            this.transactionSequence = transactionSequence;
            return this;
        }

        public Builder commitScn(CommitScn commitScn) {
            this.commitScn = commitScn;
            return this;
        }

        public Builder windowAdvanceEnabled(boolean windowAdvanceEnabled) {
            this.windowAdvanceEnabled = windowAdvanceEnabled;
            return this;
        }

        public OracleOffsetContext build() {
            return new OracleOffsetContext(connectorConfig, scn, scnIndex, commitScn, lcrPosition,
                    snapshotCommitScn, snapshot, snapshotCompleted, transactionContext,
                    incrementalSnapshotContext, transactionId, transactionSequence, windowAdvanceEnabled);
        }
    }

    public static Builder create() {
        return new Builder();
    }

    @Override
    public Map<String, ?> getOffset() {
        final Map<String, Object> result = new HashMap<>();

        if (getSnapshot().isPresent()) {
            result.put(SourceInfo.SNAPSHOT_KEY, getSnapshot().get().toString());
            result.put(SNAPSHOT_COMPLETED_KEY, snapshotCompleted);
        }

        if (sourceInfo.getLcrPosition() != null) {
            // XStream
            result.put(SourceInfo.LCR_POSITION_KEY, sourceInfo.getLcrPosition());
        }
        else {
            // Non-XStream
            if (sourceInfo.getScn() != null) {
                result.put(SourceInfo.SCN_KEY, sourceInfo.getScn().toString());
            }
            if (sourceInfo.getScnIndex() != null) {
                result.put(SourceInfo.SCN_INDEX_KEY, sourceInfo.getScnIndex());
            }
        }

        if (!snapshotCommitScn.isNull()) {
            if (isSnapshotCommitScnRetired()) {
                // Every redo thread has committed past the snapshot, so nothing mined from now on can be
                // part of it.
                retireSnapshotCommitScn();
            }
            else {
                result.put(SNAPSHOT_COMMIT_SCN_KEY, snapshotCommitScn.toString());
            }
        }

        if (sourceInfo.getCommitScn() != null) {
            sourceInfo.getCommitScn().store(result);
        }

        if (sourceInfo.getTransactionId() != null) {
            result.put(SourceInfo.TXID_KEY, sourceInfo.getTransactionId());
            if (sourceInfo.getTransactionSequence() != null) {
                result.put(SourceInfo.TXSEQ_KEY, sourceInfo.getTransactionSequence());
            }
        }

        if (windowAdvanceEnabled) {
            result.put(WINDOW_ADVANCE_ENABLED_KEY, true);
        }

        return sourceInfo.isSnapshot() ? result : incrementalSnapshotContext.store(transactionContext.store(result));
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

    public void setEventCommitScn(Scn eventCommitScn) {
        sourceInfo.setEventCommitScn(eventCommitScn);
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

    public Instant getCommitTime() {
        return sourceInfo.getCommitTime();
    }

    public Scn getEventScn() {
        return sourceInfo.getEventScn();
    }

    public Scn getEventCommitScn() {
        return sourceInfo.getEventCommitScn();
    }

    public void setLcrPosition(String lcrPosition) {
        sourceInfo.setLcrPosition(lcrPosition);
    }

    public String getLcrPosition() {
        return sourceInfo.getLcrPosition();
    }

    public Scn getSnapshotCommitScn() {
        return snapshotCommitScn;
    }

    /**
     * Returns the SCN the initial snapshot is taken at, which the snapshot records are read as of.
     * <p>
     * For LogMiner it is the snapshot commit SCN, because the offset SCN is already the position
     * where mining resumes, before the snapshot. XStream and OpenLogReplicator start streaming at
     * the SCN the snapshot is taken at, so for them it is the offset SCN itself.
     *
     * @return the SCN the snapshot is taken at, only meaningful during the snapshot
     */
    public Scn getSnapshotAsOfScn() {
        return snapshotCommitScn.isNull() ? getScn() : snapshotCommitScn;
    }

    /**
     * Retires the snapshot commit SCN once every redo thread has committed past it, after which no
     * transaction that is part of the snapshot can be mined again.
     */
    private void retireSnapshotCommitScn() {
        this.snapshotCommitScn = Scn.NULL;
    }

    /**
     * Checks whether changes committed at the given SCN are already part of the initial snapshot.
     *
     * @param commitScn the commit SCN of a transaction, or the SCN of a schema change, may be {@code null}
     * @return true if the changes committed at or before the snapshot was taken
     */
    public boolean isCommitIncludedInSnapshot(Scn commitScn) {
        if (snapshotCommitScn.isNull() || commitScn == null || commitScn.isNull()) {
            return false;
        }
        return commitScn.compareTo(snapshotCommitScn) <= 0;
    }

    private boolean isSnapshotCommitScnRetired() {
        final CommitScn commitScn = sourceInfo.getCommitScn();
        return commitScn != null && commitScn.compareTo(snapshotCommitScn) > 0;
    }

    public String getTransactionId() {
        return sourceInfo.getTransactionId();
    }

    public Long getTransactionSequence() {
        return sourceInfo.getTransactionSequence();
    }

    public boolean isWindowAdvanceEnabled() {
        return windowAdvanceEnabled;
    }

    /**
     * Marks the window advance feature as having been enabled.
     * Once set to true, this cannot be unset (until offsets are cleared).
     */
    public void setWindowAdvanceEnabled() {
        this.windowAdvanceEnabled = true;
    }

    public void setTransactionId(String transactionId) {
        sourceInfo.setTransactionId(transactionId);
    }

    public void setTransactionSequence(Long transactionSequence) {
        sourceInfo.setTransactionSequence(transactionSequence);
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

    public void setStartScn(Scn startScn) {
        sourceInfo.setStartScn(startScn);
    }

    public void setStartTime(Instant startTime) {
        sourceInfo.setStartTime(startTime);
    }

    public void setCommitTime(Instant commitTime) {
        sourceInfo.setCommitTime(commitTime);
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

        if (getTransactionId() != null) {
            sb.append(", txId=").append(getTransactionId());
            if (getTransactionSequence() != null) {
                sb.append(", txSeq=").append(getTransactionSequence());
            }
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
     * Helper method to read the snapshot commit SCN from the offset map.
     *
     * @param offset the offset map
     * @return the snapshot commit SCN, may be {@code null}
     */
    public static Scn loadSnapshotCommitScn(Map<String, ?> offset) {
        return getScnFromOffsetMapByKey(offset, SNAPSHOT_COMMIT_SCN_KEY);
    }

    /**
     * Helper method to read the offset SCN and the snapshot commit SCN for LogMiner from the offset map.
     * <p>
     * Offsets written by older versions only contain {@code snapshot_scn}, the SCN the snapshot was
     * taken at, which becomes the snapshot commit SCN. Such versions kept the offset SCN at that SCN
     * until streaming persisted its first position, and only then serialized the transactions in
     * progress at the snapshot in {@code snapshot_pending_tx}:
     * <ul>
     * <li>If the offset SCN equals {@code snapshot_scn}, streaming has not yet persisted its mining
     * position, so the offset SCN is moved to just before the oldest start SCN in
     * {@code snapshot_pending_tx}, or left unchanged when no transaction was in progress.</li>
     * <li>Otherwise the offset SCN is already the position where mining resumes.</li>
     * </ul>
     *
     * @param offset the offset map
     * @param builder the offset context builder to apply the SCNs to
     * @return the builder
     */
    public static Builder loadLogMinerScns(Map<String, ?> offset, Builder builder) {
        Scn scn = getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY);
        Scn snapshotCommitScn = loadSnapshotCommitScn(offset);
        if (snapshotCommitScn == null) {
            final Scn legacySnapshotScn = getScnFromOffsetMapByKey(offset, LEGACY_SNAPSHOT_SCN_KEY);
            if (legacySnapshotScn != null) {
                snapshotCommitScn = legacySnapshotScn;
                if (legacySnapshotScn.equals(scn)) {
                    final Scn pendingTransactionStartScn = loadLegacyMinimumPendingTransactionStartScn(offset);
                    if (pendingTransactionStartScn != null && pendingTransactionStartScn.compareTo(scn) < 0) {
                        scn = pendingTransactionStartScn.subtract(Scn.ONE);
                    }
                }
            }
        }
        return builder.scn(scn).snapshotCommitScn(snapshotCommitScn);
    }

    private static Scn loadLegacyMinimumPendingTransactionStartScn(Map<String, ?> offset) {
        final String encoded = readOffsetValue(offset, LEGACY_SNAPSHOT_PENDING_TRANSACTIONS_KEY, String.class);
        if (encoded == null) {
            return null;
        }
        return Arrays.stream(encoded.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(s -> Scn.valueOf(s.split(":", 2)[1]))
                .min(Scn::compareTo)
                .orElse(null);
    }

    /**
     * Helper method to read the transaction id from the offset map.
     *
     * @param offset the offset map
     * @return the transaction identifier, may be {@code null}
     */
    public static String loadTransactionId(Map<String, ?> offset) {
        return readOffsetValue(offset, SourceInfo.TXID_KEY, String.class);
    }

    /**
     * Helper method to read the transaction sequence from the offset map.
     *
     * @param offset the offset map
     * @return the transaction sequence, may be {@code null}
     */
    public static Long loadTransactionSequence(Map<String, ?> offset) {
        return readOffsetValue(offset, SourceInfo.TXSEQ_KEY, Long.class);
    }

    /**
     * Helper method to read whether window advance has been enabled from the offset map.
     *
     * @param offset the offset map
     * @return true if window advance has ever been enabled, false otherwise
     */
    public static boolean loadWindowAdvanceEnabled(Map<String, ?> offset) {
        Boolean value = readOffsetValue(offset, WINDOW_ADVANCE_ENABLED_KEY, Boolean.class);
        return value != null && value;
    }

    private static <T> T readOffsetValue(Map<String, ?> offsets, String key, Class<T> valueType) {
        final Object value = offsets.get(key);
        return valueType.isInstance(value) ? valueType.cast(value) : null;
    }
}
