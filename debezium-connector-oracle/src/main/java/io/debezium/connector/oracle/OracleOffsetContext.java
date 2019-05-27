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

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;

public class OracleOffsetContext implements OffsetContext {

    private static final String SERVER_PARTITION_KEY = "server";
    private static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";

    private final Schema sourceInfoSchema;
    private final Map<String, String> partition;

    private final SourceInfo sourceInfo;

    /**
     * Whether a snapshot has been completed or not.
     */
    private boolean snapshotCompleted;

    private OracleOffsetContext(OracleConnectorConfig connectorConfig, long scn, LcrPosition lcrPosition, boolean snapshot, boolean snapshotCompleted) {
        partition = Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());

        sourceInfo = new SourceInfo(connectorConfig);
        sourceInfo.setScn(scn);
        sourceInfo.setLcrPosition(lcrPosition);
        sourceInfoSchema = sourceInfo.schema();

        this.snapshotCompleted = snapshotCompleted;
        if (this.snapshotCompleted) {
            postSnapshotCompletion();
        }
        else {
            sourceInfo.setSnapshot(snapshot);
        }
    }

    public static class Builder {

        private OracleConnectorConfig connectorConfig;
        private long scn;
        private LcrPosition lcrPosition;
        private boolean snapshot;
        private boolean snapshotCompleted;

        public Builder logicalName(OracleConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
            return this;
        }

        public Builder scn(long scn) {
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

        OracleOffsetContext build() {
            return new OracleOffsetContext(connectorConfig, scn, lcrPosition, snapshot, snapshotCompleted);
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

            offset.put(SourceInfo.SCN_KEY, sourceInfo.getScn());
            offset.put(SourceInfo.SNAPSHOT_KEY, true);
            offset.put(SNAPSHOT_COMPLETED_KEY, snapshotCompleted);

            return offset;
        }
        else {
            if (sourceInfo.getLcrPosition() != null) {
                return Collections.singletonMap(SourceInfo.LCR_POSITION_KEY, sourceInfo.getLcrPosition().toString());
            }
            return Collections.singletonMap(SourceInfo.SCN_KEY, sourceInfo.getScn());
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

    public void setScn(long scn) {
        sourceInfo.setScn(scn);
    }

    public long getScn() {
        return sourceInfo.getScn();
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
        sourceInfo.setSnapshot(true);
        snapshotCompleted = false;
    }

    @Override
    public void preSnapshotCompletion() {
        snapshotCompleted = true;
    }

    @Override
    public void postSnapshotCompletion() {
        sourceInfo.setSnapshot(false);
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

    public static class Loader implements OffsetContext.Loader {

        private final OracleConnectorConfig connectorConfig;

        public Loader(OracleConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Map<String, ?> getPartition() {
            return Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
        }

        @Override
        public OffsetContext load(Map<String, ?> offset) {
            LcrPosition lcrPosition = LcrPosition.valueOf((String) offset.get(SourceInfo.LCR_POSITION_KEY));
            Long scn = lcrPosition != null ? lcrPosition.getScn() : (Long) offset.get(SourceInfo.SCN_KEY);
            boolean snapshot = Boolean.TRUE.equals(offset.get(SourceInfo.SNAPSHOT_KEY));
            boolean snapshotCompleted = Boolean.TRUE.equals(offset.get(SNAPSHOT_COMPLETED_KEY));

            return new OracleOffsetContext(connectorConfig, scn, lcrPosition, snapshot, snapshotCompleted);
        }
    }
}
