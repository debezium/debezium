/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.oracle.xstream.LcrPosition;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;

public class OracleOffsetContext implements OffsetContext {

    private static final String SERVER_PARTITION_KEY = "server";
    private static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";

    private final Schema sourceInfoSchema;
    private final Map<String, String> partition;

    private final SourceInfo sourceInfo;
    private final TransactionContext transactionContext;

    /**
     * Whether a snapshot has been completed or not.
     */
    private boolean snapshotCompleted;

    public OracleOffsetContext(OracleConnectorConfig connectorConfig, Scn scn, Scn commitScn, LcrPosition lcrPosition,
                               boolean snapshot, boolean snapshotCompleted, TransactionContext transactionContext) {
        this(connectorConfig, scn, lcrPosition, snapshot, snapshotCompleted, transactionContext);
        sourceInfo.setCommitScn(commitScn);
    }

    private OracleOffsetContext(OracleConnectorConfig connectorConfig, Scn scn, LcrPosition lcrPosition,
                                boolean snapshot, boolean snapshotCompleted, TransactionContext transactionContext) {
        partition = Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());

        sourceInfo = new SourceInfo(connectorConfig);
        sourceInfo.setScn(scn);
        sourceInfo.setLcrPosition(lcrPosition);
        sourceInfoSchema = sourceInfo.schema();

        this.transactionContext = transactionContext;

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
        private LcrPosition lcrPosition;
        private boolean snapshot;
        private boolean snapshotCompleted;
        private TransactionContext transactionContext;

        public Builder logicalName(OracleConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
            return this;
        }

        public Builder scn(Scn scn) {
            this.scn = scn;
            return this;
        }

        public Builder lcrPosition(LcrPosition lcrPosition) {
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

        OracleOffsetContext build() {
            return new OracleOffsetContext(connectorConfig, scn, lcrPosition, snapshot, snapshotCompleted, transactionContext);
        }
    }

    public static Builder create() {
        return new Builder();
    }

    @Override
    public Map<String, ?> getPartition() {
        return partition;
    }

    @Override
    public Map<String, ?> getOffset() {
        if (sourceInfo.isSnapshot()) {
            Map<String, Object> offset = new HashMap<>();

            final Scn scn = sourceInfo.getScn();
            offset.put(SourceInfo.SCN_KEY, scn != null ? scn.toString() : scn);
            offset.put(SourceInfo.SNAPSHOT_KEY, true);
            offset.put(SNAPSHOT_COMPLETED_KEY, snapshotCompleted);

            return offset;
        }
        else {
            final Map<String, Object> offset = new HashMap<>();
            if (sourceInfo.getLcrPosition() != null) {
                offset.put(SourceInfo.LCR_POSITION_KEY, sourceInfo.getLcrPosition().toString());
            }
            else {
                final Scn scn = sourceInfo.getScn();
                final Scn commitScn = sourceInfo.getCommitScn();
                offset.put(SourceInfo.SCN_KEY, scn != null ? scn.toString() : null);
                offset.put(SourceInfo.COMMIT_SCN_KEY, commitScn != null ? commitScn.toString() : null);
            }
            return transactionContext.store(offset);
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

    public void setLcrPosition(LcrPosition lcrPosition) {
        sourceInfo.setLcrPosition(lcrPosition);
    }

    public LcrPosition getLcrPosition() {
        return sourceInfo.getLcrPosition();
    }

    public void setTransactionId(String transactionId) {
        sourceInfo.setTransactionId(transactionId);
    }

    public void setSourceTime(Instant instant) {
        sourceInfo.setSourceTime(instant);
    }

    public void setTableId(TableId tableId) {
        sourceInfo.setTableId(tableId);
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
        sourceInfo.setTableId((TableId) tableId);
        sourceInfo.setSourceTime(timestamp);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    public static class Loader implements OffsetContext.Loader {

        private final OracleConnectorConfig connectorConfig;
        private final OracleConnectorConfig.ConnectorAdapter adapter;

        // todo resolve adapter from the config rather than passing it
        public Loader(OracleConnectorConfig connectorConfig, OracleConnectorConfig.ConnectorAdapter adapter) {
            this.connectorConfig = connectorConfig;
            this.adapter = adapter;
        }

        @Override
        public Map<String, ?> getPartition() {
            return Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
        }

        @Override
        public OffsetContext load(Map<String, ?> offset) {
            boolean snapshot = Boolean.TRUE.equals(offset.get(SourceInfo.SNAPSHOT_KEY));
            boolean snapshotCompleted = Boolean.TRUE.equals(offset.get(SNAPSHOT_COMPLETED_KEY));
            Scn scn;
            if (adapter == OracleConnectorConfig.ConnectorAdapter.LOG_MINER) {
                scn = getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY);
                Scn commitScn = getScnFromOffsetMapByKey(offset, SourceInfo.COMMIT_SCN_KEY);
                return new OracleOffsetContext(connectorConfig, scn, commitScn, null, snapshot, snapshotCompleted, TransactionContext.load(offset));
            }
            else {
                LcrPosition lcrPosition = LcrPosition.valueOf((String) offset.get(SourceInfo.LCR_POSITION_KEY));
                scn = (lcrPosition != null ? lcrPosition.getScn() : getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY));
                return new OracleOffsetContext(connectorConfig, scn, lcrPosition, snapshot, snapshotCompleted, TransactionContext.load(offset));
            }

        }

        public static Scn getScnFromOffset(Map<String, ?> offset, LcrPosition lcrPosition) {
            if (lcrPosition != null) {
                return lcrPosition.getScn();
            }
            // Prioritize string-based SCN key over the numeric-based SCN key
            Object scn = offset.get(SourceInfo.SCN_KEY);
            if (scn instanceof String) {
                return Scn.valueOf((String) scn);
            }
            else if (scn != null) {
                return Scn.valueOf((Long) scn);
            }
            return null;
        }
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
}
