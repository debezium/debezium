/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.SnapshotType;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Tracks the current position in the SQLite WAL stream.
 *
 * @author Zihan Dai
 */
public class SqliteOffsetContext extends CommonOffsetContext<SqliteSourceInfo> {

    private final Schema sourceInfoSchema;
    private final TransactionContext transactionContext;
    private final IncrementalSnapshotContext<TableId> incrementalSnapshotContext;
    private long walPosition;

    public SqliteOffsetContext(SqliteConnectorConfig connectorConfig, long walPosition,
                                SnapshotType snapshot, boolean snapshotCompleted,
                                TransactionContext transactionContext,
                                IncrementalSnapshotContext<TableId> incrementalSnapshotContext) {
        super(new SqliteSourceInfo(connectorConfig), snapshotCompleted);

        sourceInfo.setWalPosition(walPosition);
        sourceInfoSchema = sourceInfo.schema();

        if (this.snapshotCompleted) {
            postSnapshotCompletion();
        }
        else {
            setSnapshot(snapshot);
            sourceInfo.setSnapshot(snapshot != null ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }

        this.walPosition = walPosition;
        this.transactionContext = transactionContext;
        this.incrementalSnapshotContext = incrementalSnapshotContext;
    }

    public SqliteOffsetContext(SqliteConnectorConfig connectorConfig, long walPosition,
                                SnapshotType snapshot, boolean snapshotCompleted) {
        this(connectorConfig, walPosition, snapshot, snapshotCompleted,
                new TransactionContext(), new SignalBasedIncrementalSnapshotContext<>());
    }

    public static SqliteOffsetContext initial(SqliteConnectorConfig connectorConfig) {
        return new SqliteOffsetContext(connectorConfig, 0L, null, false);
    }

    @Override
    public Map<String, ?> getOffset() {
        Map<String, Object> offset = new HashMap<>();
        if (getSnapshot().isPresent()) {
            offset.put(AbstractSourceInfo.SNAPSHOT_KEY, getSnapshot().get().toString());
            offset.put(SNAPSHOT_COMPLETED_KEY, snapshotCompleted);
        }
        offset.put(SqliteHistoryRecordComparator.WAL_POSITION_KEY, walPosition);
        return sourceInfo.isSnapshot() ? offset : incrementalSnapshotContext.store(transactionContext.store(offset));
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    public long getWalPosition() {
        return walPosition;
    }

    public void setWalPosition(long walPosition) {
        this.walPosition = walPosition;
        sourceInfo.setWalPosition(walPosition);
    }

    @Override
    public boolean isSnapshotRunning() {
        return getSnapshot().isPresent() && !snapshotCompleted;
    }

    @Override
    public void preSnapshotCompletion() {
        // nothing to do
    }

    @Override
    public void postSnapshotCompletion() {
        sourceInfo.setSnapshot(SnapshotRecord.FALSE);
        snapshotCompleted = true;
    }

    @Override
    public void event(DataCollectionId collectionId, Instant timestamp) {
        sourceInfo.setTimestamp(timestamp);
        sourceInfo.setTableId((TableId) collectionId);
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
     * Loader for restoring a {@link SqliteOffsetContext} from previously stored offsets.
     */
    public static class Loader implements OffsetContext.Loader<SqliteOffsetContext> {

        private final SqliteConnectorConfig connectorConfig;

        public Loader(SqliteConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public SqliteOffsetContext load(Map<String, ?> offset) {
            long walPosition = offset.containsKey(SqliteHistoryRecordComparator.WAL_POSITION_KEY)
                    ? ((Number) offset.get(SqliteHistoryRecordComparator.WAL_POSITION_KEY)).longValue()
                    : 0L;

            boolean snapshotCompleted = Boolean.TRUE.equals(offset.get(SNAPSHOT_COMPLETED_KEY));

            return new SqliteOffsetContext(connectorConfig, walPosition, null, snapshotCompleted);
        }
    }
}
