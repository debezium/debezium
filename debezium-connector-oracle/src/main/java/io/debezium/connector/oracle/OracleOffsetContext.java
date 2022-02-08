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
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;

public class OracleOffsetContext implements OffsetContext {

    public static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";
    public static final String SNAPSHOT_PENDING_TRANSACTIONS_KEY = "snapshot_pending_tx";
    public static final String SNAPSHOT_SCN_KEY = "snapshot_scn";

    private final Schema sourceInfoSchema;

    private final SourceInfo sourceInfo;
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

    /**
     * Whether a snapshot has been completed or not.
     */
    private boolean snapshotCompleted;

    public OracleOffsetContext(OracleConnectorConfig connectorConfig, Scn scn, Scn commitScn, String lcrPosition,
                               Scn snapshotScn, Map<String, Scn> snapshotPendingTransactions,
                               boolean snapshot, boolean snapshotCompleted, TransactionContext transactionContext,
                               IncrementalSnapshotContext<TableId> incrementalSnapshotContext) {
        this(connectorConfig, scn, lcrPosition, snapshotScn, snapshotPendingTransactions, snapshot, snapshotCompleted, transactionContext, incrementalSnapshotContext);
        sourceInfo.setCommitScn(commitScn);
    }

    public OracleOffsetContext(OracleConnectorConfig connectorConfig, Scn scn, String lcrPosition,
                               Scn snapshotScn, Map<String, Scn> snapshotPendingTransactions,
                               boolean snapshot, boolean snapshotCompleted, TransactionContext transactionContext,
                               IncrementalSnapshotContext<TableId> incrementalSnapshotContext) {
        sourceInfo = new SourceInfo(connectorConfig);
        sourceInfo.setScn(scn);
        sourceInfo.setLcrPosition(lcrPosition);
        sourceInfoSchema = sourceInfo.schema();

        this.snapshotScn = snapshotScn;
        this.snapshotPendingTransactions = snapshotPendingTransactions;

        this.transactionContext = transactionContext;
        this.incrementalSnapshotContext = incrementalSnapshotContext;

        this.snapshotCompleted = snapshotCompleted;
        if (this.snapshotCompleted) {
            postSnapshotCompletion();
        }
        else {
            sourceInfo.setSnapshot(snapshot ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }
    }

    public static class Builder {

        private OracleConnectorConfig connectorConfig;
        private Scn scn;
        private String lcrPosition;
        private boolean snapshot;
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

        public Builder lcrPosition(String lcrPosition) {
            this.lcrPosition = lcrPosition;
            return this;
        }

        public Builder snapshot(boolean snapshot) {
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

        OracleOffsetContext build() {
            return new OracleOffsetContext(connectorConfig, scn, lcrPosition, snapshotScn, snapshotPendingTransactions, snapshot, snapshotCompleted, transactionContext,
                    incrementalSnapshotContext);
        }
    }

    public static Builder create() {
        return new Builder();
    }

    @Override
    public Map<String, ?> getOffset() {
        if (sourceInfo.isSnapshot()) {
            Map<String, Object> offset = new HashMap<>();

            final Scn scn = sourceInfo.getScn();
            offset.put(SourceInfo.SCN_KEY, scn != null ? scn.toString() : scn);
            offset.put(SourceInfo.SNAPSHOT_KEY, true);
            offset.put(SNAPSHOT_COMPLETED_KEY, snapshotCompleted);

            if (snapshotPendingTransactions != null) {
                String encoded = snapshotPendingTransactions.entrySet().stream()
                        .map(e -> e.getKey() + ":" + e.getValue().toString())
                        .collect(Collectors.joining(","));
                offset.put(SNAPSHOT_PENDING_TRANSACTIONS_KEY, encoded);
            }
            offset.put(SNAPSHOT_SCN_KEY, snapshotScn != null ? snapshotScn.toString() : null);

            return offset;
        }
        else {
            final Map<String, Object> offset = new HashMap<>();
            if (sourceInfo.getLcrPosition() != null) {
                offset.put(SourceInfo.LCR_POSITION_KEY, sourceInfo.getLcrPosition());
            }
            else {
                final Scn scn = sourceInfo.getScn();
                final Scn commitScn = sourceInfo.getCommitScn();
                offset.put(SourceInfo.SCN_KEY, scn != null ? scn.toString() : null);
                offset.put(SourceInfo.COMMIT_SCN_KEY, commitScn != null ? commitScn.toString() : null);
            }
            if (snapshotPendingTransactions != null) {
                String encoded = snapshotPendingTransactions.entrySet().stream()
                        .map(e -> e.getKey() + ":" + e.getValue().toString())
                        .collect(Collectors.joining(","));
                offset.put(SNAPSHOT_PENDING_TRANSACTIONS_KEY, encoded);
            }
            offset.put(SNAPSHOT_SCN_KEY, snapshotScn != null ? snapshotScn.toString() : null);

            return incrementalSnapshotContext.store(transactionContext.store(offset));
        }
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    public void setScn(Scn scn) {
        sourceInfo.setScn(scn);
    }

    public void setCommitScn(Scn commitScn) {
        sourceInfo.setCommitScn(commitScn);
    }

    public Scn getScn() {
        return sourceInfo.getScn();
    }

    public Scn getCommitScn() {
        return sourceInfo.getCommitScn();
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

    public void setSourceTime(Instant instant) {
        sourceInfo.setSourceTime(instant);
    }

    public void setTableId(TableId tableId) {
        sourceInfo.tableEvent(tableId);
    }

    @Override
    public boolean isSnapshotRunning() {
        return sourceInfo.isSnapshot() && !snapshotCompleted;
    }

    @Override
    public void preSnapshotStart() {
        sourceInfo.setSnapshot(SnapshotRecord.TRUE);
        snapshotCompleted = false;
    }

    @Override
    public void preSnapshotCompletion() {
        snapshotCompleted = true;
    }

    @Override
    public void postSnapshotCompletion() {
        sourceInfo.setSnapshot(SnapshotRecord.FALSE);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("OracleOffsetContext [scn=").append(getScn());

        if (sourceInfo.isSnapshot()) {
            sb.append(", snapshot=").append(sourceInfo.isSnapshot());
            sb.append(", snapshot_completed=").append(snapshotCompleted);
        }

        sb.append("]");

        return sb.toString();
    }

    @Override
    public void markLastSnapshotRecord() {
        sourceInfo.setSnapshot(SnapshotRecord.LAST);
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
    public void incrementalSnapshotEvents() {
        sourceInfo.setSnapshot(SnapshotRecord.INCREMENTAL);
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
