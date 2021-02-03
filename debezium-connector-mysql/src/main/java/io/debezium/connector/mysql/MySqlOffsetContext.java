/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;

public class MySqlOffsetContext implements OffsetContext {

    private static final String SERVER_PARTITION_KEY = "server";
    private static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";

    private final Schema sourceInfoSchema;
    private final SourceInfo sourceInfo;
    private final Map<String, String> partition;
    private boolean snapshotCompleted;
    private final TransactionContext transactionContext;
    private final MySqlConnectorConfig connectorConfig;

    public MySqlOffsetContext(MySqlConnectorConfig connectorConfig, boolean snapshot, boolean snapshotCompleted,
                              TransactionContext transactionContext, SourceInfo sourceInfo) {
        this.connectorConfig = connectorConfig;
        partition = Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
        this.sourceInfo = sourceInfo;
        sourceInfoSchema = sourceInfo.schema();

        this.snapshotCompleted = snapshotCompleted;
        if (this.snapshotCompleted) {
            postSnapshotCompletion();
        }
        else {
            sourceInfo.setSnapshot(snapshot ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }
        this.transactionContext = transactionContext;
    }

    public MySqlOffsetContext(MySqlConnectorConfig connectorConfig, boolean snapshot, boolean snapshotCompleted, SourceInfo sourceInfo) {
        this(connectorConfig, snapshot, snapshotCompleted, new TransactionContext(), sourceInfo);
    }

    @Override
    public Map<String, ?> getPartition() {
        return partition;
    }

    @Override
    public Map<String, ?> getOffset() {
        @SuppressWarnings("unchecked")
        final Map<String, Object> offset = (Map<String, Object>) sourceInfo.offset();
        if (sourceInfo.isSnapshot()) {
            // TODO Added when responsibilites will split between StourceInfo and OffsetContext
            // offset.put(SourceInfo.SNAPSHOT_KEY, true);
        }
        else {
            return transactionContext.store(offset);
        }
        return offset;
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    @Override
    public boolean isSnapshotRunning() {
        return sourceInfo.isSnapshot() && !snapshotCompleted;
    }

    public boolean isSnapshotCompleted() {
        return snapshotCompleted;
    }

    @Override
    public void preSnapshotStart() {
        sourceInfo.setSnapshot(SnapshotRecord.TRUE);
        snapshotCompleted = false;
        sourceInfo.startSnapshot();
    }

    @Override
    public void preSnapshotCompletion() {
        snapshotCompleted = true;
        sourceInfo.markLastSnapshot(connectorConfig.getConfig());
    }

    @Override
    public void postSnapshotCompletion() {
        sourceInfo.setSnapshot(SnapshotRecord.FALSE);
    }

    public static MySqlOffsetContext initial(MySqlConnectorConfig config) {
        final MySqlOffsetContext offset = new MySqlOffsetContext(config, false, false, new SourceInfo(config));
        offset.getSource().setBinlogStartPoint("", 0L); // start from the beginning of the binlog
        return offset;
    }

    public static class Loader implements OffsetContext.Loader {

        private final MySqlConnectorConfig connectorConfig;

        public Loader(MySqlConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Map<String, ?> getPartition() {
            return Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
        }

        @Override
        public OffsetContext load(Map<String, ?> offset) {
            boolean snapshot = Boolean.TRUE.equals(offset.get(SourceInfo.SNAPSHOT_KEY));
            boolean snapshotCompleted = Boolean.TRUE.equals(offset.get(SNAPSHOT_COMPLETED_KEY));

            final SourceInfo sourceInfo = new SourceInfo(connectorConfig);
            final MySqlOffsetContext offsetContext = new MySqlOffsetContext(connectorConfig, snapshot, snapshotCompleted,
                    TransactionContext.load(offset), sourceInfo);
            sourceInfo.setOffset(offset);
            return offsetContext;
        }
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public void markLastSnapshotRecord() {
        sourceInfo.setSnapshot(SnapshotRecord.LAST);
    }

    @Override
    public void event(DataCollectionId tableId, Instant timestamp) {
        sourceInfo.setSourceTime(timestamp);
        sourceInfo.tableEvent((TableId) tableId);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    public SourceInfo getSource() {
        return sourceInfo;
    }
}
