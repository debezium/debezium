/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.SnapshotType;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;

public class SqlServerOffsetContext extends CommonOffsetContext<SourceInfo> {

    private final Schema sourceInfoSchema;
    private final TransactionContext transactionContext;
    private final IncrementalSnapshotContext<TableId> incrementalSnapshotContext;

    /**
     * The index of the current event within the current transaction.
     */
    private long eventSerialNo;

    public SqlServerOffsetContext(SqlServerConnectorConfig connectorConfig, TxLogPosition position, SnapshotType snapshot,
                                  boolean snapshotCompleted, long eventSerialNo, TransactionContext transactionContext,
                                  IncrementalSnapshotContext<TableId> incrementalSnapshotContext) {
        super(new SourceInfo(connectorConfig), snapshotCompleted);

        sourceInfo.setCommitLsn(position.getCommitLsn());
        sourceInfo.setChangeLsn(position.getInTxLsn());
        sourceInfoSchema = sourceInfo.schema();

        if (this.snapshotCompleted) {
            postSnapshotCompletion();
        }
        else {
            setSnapshot(snapshot);
            sourceInfo.setSnapshot(snapshot != null ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }
        this.eventSerialNo = eventSerialNo;
        this.transactionContext = transactionContext;
        this.incrementalSnapshotContext = incrementalSnapshotContext;
    }

    public SqlServerOffsetContext(SqlServerConnectorConfig connectorConfig, TxLogPosition position, SnapshotType snapshot, boolean snapshotCompleted) {
        this(connectorConfig, position, snapshot, snapshotCompleted, 1, new TransactionContext(),
                new SqlServerIncrementalSnapshotContext<>());
    }

    @Override
    public Map<String, ?> getOffset() {
        if (getSnapshot().isPresent()) {
            return Collect.hashMapOf(
                    AbstractSourceInfo.SNAPSHOT_KEY, getSnapshot().get().toString(),
                    SNAPSHOT_COMPLETED_KEY, snapshotCompleted,
                    SourceInfo.COMMIT_LSN_KEY, sourceInfo.getCommitLsn().toString());
        }
        else {
            return incrementalSnapshotContext.store(transactionContext.store(Collect.hashMapOf(
                    SourceInfo.COMMIT_LSN_KEY, sourceInfo.getCommitLsn().toString(),
                    SourceInfo.CHANGE_LSN_KEY,
                    sourceInfo.getChangeLsn() == null ? null : sourceInfo.getChangeLsn().toString(),
                    SourceInfo.EVENT_SERIAL_NO_KEY, eventSerialNo)));
        }
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    public TxLogPosition getChangePosition() {
        return TxLogPosition.valueOf(sourceInfo.getCommitLsn(), sourceInfo.getChangeLsn());
    }

    public long getEventSerialNo() {
        return eventSerialNo;
    }

    public void setChangePosition(TxLogPosition position, int eventCount) {
        if (getChangePosition().equals(position)) {
            eventSerialNo += eventCount;
        }
        else {
            eventSerialNo = eventCount;
        }
        sourceInfo.setCommitLsn(position.getCommitLsn());
        sourceInfo.setChangeLsn(position.getInTxLsn());
        sourceInfo.setEventSerialNo(eventSerialNo);
    }

    public boolean isSnapshotCompleted() {
        return snapshotCompleted;
    }

    public static class Loader implements OffsetContext.Loader<SqlServerOffsetContext> {

        private final SqlServerConnectorConfig connectorConfig;

        public Loader(SqlServerConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public SqlServerOffsetContext load(Map<String, ?> offset) {
            final Lsn changeLsn = Lsn.valueOf((String) offset.get(SourceInfo.CHANGE_LSN_KEY));
            final Lsn commitLsn = Lsn.valueOf((String) offset.get(SourceInfo.COMMIT_LSN_KEY));
            final SnapshotType snapshot = loadSnapshot(offset).orElse(null);
            final boolean snapshotCompleted = loadSnapshotCompleted(offset);

            // only introduced in 0.10.Beta1, so it might be not present when upgrading from earlier versions
            Long eventSerialNo = ((Long) offset.get(SourceInfo.EVENT_SERIAL_NO_KEY));
            if (eventSerialNo == null) {
                eventSerialNo = Long.valueOf(0);
            }

            return new SqlServerOffsetContext(connectorConfig, TxLogPosition.valueOf(commitLsn, changeLsn), snapshot, snapshotCompleted, eventSerialNo,
                    TransactionContext.load(offset), SqlServerIncrementalSnapshotContext.load(offset));
        }
    }

    @Override
    public String toString() {
        return "SqlServerOffsetContext [" +
                "sourceInfoSchema=" + sourceInfoSchema +
                ", sourceInfo=" + sourceInfo +
                ", snapshotCompleted=" + snapshotCompleted +
                ", eventSerialNo=" + eventSerialNo +
                "]";
    }

    @Override
    public void event(DataCollectionId tableId, Instant timestamp) {
        sourceInfo.setSourceTime(timestamp);
        sourceInfo.setTableId((TableId) tableId);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return incrementalSnapshotContext;
    }
}
