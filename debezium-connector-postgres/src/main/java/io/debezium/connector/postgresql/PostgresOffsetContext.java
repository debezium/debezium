/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.spi.OffsetState;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;
import io.debezium.time.Conversions;
import io.debezium.util.Clock;

public class PostgresOffsetContext implements OffsetContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSnapshotChangeEventSource.class);

    private static final String SERVER_PARTITION_KEY = "server";
    public static final String LAST_COMPLETELY_PROCESSED_LSN_KEY = "lsn_proc";
    public static final String LAST_COMMIT_LSN_KEY = "lsn_commit";

    private final Schema sourceInfoSchema;
    private final SourceInfo sourceInfo;
    private final Map<String, String> partition;
    private boolean lastSnapshotRecord;
    private Lsn lastCompletelyProcessedLsn;
    private Lsn lastCommitLsn;
    private Lsn streamingStoppingLsn = null;
    private final TransactionContext transactionContext;

    private PostgresOffsetContext(PostgresConnectorConfig connectorConfig, Lsn lsn, Lsn lastCompletelyProcessedLsn, Lsn lastCommitLsn, Long txId, Instant time,
                                  boolean snapshot,
                                  boolean lastSnapshotRecord, TransactionContext transactionContext) {
        partition = Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
        sourceInfo = new SourceInfo(connectorConfig);

        this.lastCompletelyProcessedLsn = lastCompletelyProcessedLsn;
        this.lastCommitLsn = lastCommitLsn;
        sourceInfo.update(lsn, time, txId, null, sourceInfo.xmin(), lastCommitLsn);
        sourceInfoSchema = sourceInfo.schema();

        this.lastSnapshotRecord = lastSnapshotRecord;
        if (this.lastSnapshotRecord) {
            postSnapshotCompletion();
        }
        else {
            sourceInfo.setSnapshot(snapshot ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }
        this.transactionContext = transactionContext;
    }

    @Override
    public Map<String, ?> getPartition() {
        return partition;
    }

    @Override
    public Map<String, ?> getOffset() {
        Map<String, Object> result = new HashMap<>();
        if (sourceInfo.timestamp() != null) {
            result.put(SourceInfo.TIMESTAMP_USEC_KEY, Conversions.toEpochMicros(sourceInfo.timestamp()));
        }
        if (sourceInfo.txId() != null) {
            result.put(SourceInfo.TXID_KEY, sourceInfo.txId());
        }
        if (sourceInfo.lsn() != null) {
            result.put(SourceInfo.LSN_KEY, sourceInfo.lsn().asLong());
        }
        if (sourceInfo.xmin() != null) {
            result.put(SourceInfo.XMIN_KEY, sourceInfo.xmin());
        }
        if (sourceInfo.isSnapshot()) {
            result.put(SourceInfo.SNAPSHOT_KEY, true);
            result.put(SourceInfo.LAST_SNAPSHOT_RECORD_KEY, lastSnapshotRecord);
        }
        if (lastCompletelyProcessedLsn != null) {
            result.put(LAST_COMPLETELY_PROCESSED_LSN_KEY, lastCompletelyProcessedLsn.asLong());
        }
        if (lastCommitLsn != null) {
            result.put(LAST_COMMIT_LSN_KEY, lastCommitLsn.asLong());
        }
        return sourceInfo.isSnapshot() ? result : transactionContext.store(result);
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
        return sourceInfo.isSnapshot();
    }

    @Override
    public void preSnapshotStart() {
        sourceInfo.setSnapshot(SnapshotRecord.TRUE);
        lastSnapshotRecord = false;
    }

    @Override
    public void preSnapshotCompletion() {
        lastSnapshotRecord = true;
    }

    @Override
    public void postSnapshotCompletion() {
        sourceInfo.setSnapshot(SnapshotRecord.FALSE);
    }

    public void updateSnapshotPosition(Instant timestamp, TableId tableId) {
        sourceInfo.update(timestamp, tableId);
    }

    public void updateWalPosition(Lsn lsn, Lsn lastCompletelyProcessedLsn, Instant commitTime, Long txId, TableId tableId, Long xmin) {
        this.lastCompletelyProcessedLsn = lastCompletelyProcessedLsn;
        sourceInfo.update(lsn, commitTime, txId, tableId, xmin);
    }

    public void updateCommitPosition(Lsn lsn, Lsn lastCompletelyProcessedLsn, Instant commitTime, Long txId, TableId tableId, Long xmin) {
        this.lastCompletelyProcessedLsn = lastCompletelyProcessedLsn;
        this.lastCommitLsn = lastCompletelyProcessedLsn;
        sourceInfo.update(lsn, commitTime, txId, tableId, xmin, lastCompletelyProcessedLsn);
    }

    boolean hasLastKnownPosition() {
        return sourceInfo.lsn() != null;
    }

    boolean hasCompletelyProcessedPosition() {
        return this.lastCompletelyProcessedLsn != null;
    }

    Lsn lsn() {
        return sourceInfo.lsn();
    }

    Lsn lastCompletelyProcessedLsn() {
        return lastCompletelyProcessedLsn;
    }

    Lsn lastCommitLsn() {
        return lastCommitLsn;
    }

    /**
     * Returns the LSN that the streaming phase should stream events up to or null if
     * a stopping point is not set. If set during the streaming phase, any event with
     * an LSN less than the stopping LSN will be processed and once the stopping LSN
     * is reached, the streaming phase will end. Useful for a pre-snapshot catch up
     * streaming phase.
     */
    Lsn getStreamingStoppingLsn() {
        return streamingStoppingLsn;
    }

    public void setStreamingStoppingLsn(Lsn streamingStoppingLsn) {
        this.streamingStoppingLsn = streamingStoppingLsn;
    }

    Long xmin() {
        return sourceInfo.xmin();
    }

    public static class Loader implements OffsetContext.Loader {

        private final PostgresConnectorConfig connectorConfig;

        public Loader(PostgresConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Map<String, ?> getPartition() {
            return Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
        }

        private Long readOptionalLong(Map<String, ?> offset, String key) {
            final Object obj = offset.get(key);
            return (obj == null) ? null : ((Number) obj).longValue();
        }

        @SuppressWarnings("unchecked")
        @Override
        public OffsetContext load(Map<String, ?> offset) {
            final Lsn lsn = Lsn.valueOf(readOptionalLong(offset, SourceInfo.LSN_KEY));
            final Lsn lastCompletelyProcessedLsn = Lsn.valueOf(readOptionalLong(offset, LAST_COMPLETELY_PROCESSED_LSN_KEY));
            final Lsn lastCommitLsn = Lsn.valueOf(readOptionalLong(offset, LAST_COMPLETELY_PROCESSED_LSN_KEY));
            final Long txId = readOptionalLong(offset, SourceInfo.TXID_KEY);

            final Instant useconds = Conversions.toInstantFromMicros((Long) offset.get(SourceInfo.TIMESTAMP_USEC_KEY));
            final boolean snapshot = (boolean) ((Map<String, Object>) offset).getOrDefault(SourceInfo.SNAPSHOT_KEY, Boolean.FALSE);
            final boolean lastSnapshotRecord = (boolean) ((Map<String, Object>) offset).getOrDefault(SourceInfo.LAST_SNAPSHOT_RECORD_KEY, Boolean.FALSE);
            return new PostgresOffsetContext(connectorConfig, lsn, lastCompletelyProcessedLsn, lastCommitLsn, txId, useconds, snapshot, lastSnapshotRecord,
                    TransactionContext.load(offset));
        }
    }

    @Override
    public String toString() {
        return "PostgresOffsetContext [sourceInfoSchema=" + sourceInfoSchema + ", sourceInfo=" + sourceInfo
                + ", partition=" + partition + ", lastSnapshotRecord=" + lastSnapshotRecord
                + ", lastCompletelyProcessedLsn=" + lastCompletelyProcessedLsn + ", lastCommitLsn=" + lastCommitLsn
                + ", transactionContext=" + transactionContext + "]";
    }

    public static PostgresOffsetContext initialContext(PostgresConnectorConfig connectorConfig, PostgresConnection jdbcConnection, Clock clock) {
        return initialContext(connectorConfig, jdbcConnection, clock, null, null);
    }

    public static PostgresOffsetContext initialContext(PostgresConnectorConfig connectorConfig, PostgresConnection jdbcConnection, Clock clock, Lsn lastCommitLsn,
                                                       Lsn lastCompletelyProcessedLsn) {
        try {
            LOGGER.info("Creating initial offset context");
            final Lsn lsn = Lsn.valueOf(jdbcConnection.currentXLogLocation());
            final long txId = jdbcConnection.currentTransactionId().longValue();
            LOGGER.info("Read xlogStart at '{}' from transaction '{}'", lsn, txId);
            return new PostgresOffsetContext(
                    connectorConfig,
                    lsn,
                    lastCompletelyProcessedLsn,
                    lastCommitLsn,
                    txId,
                    clock.currentTimeAsInstant(),
                    false,
                    false,
                    new TransactionContext());
        }
        catch (SQLException e) {
            throw new ConnectException("Database processing error", e);
        }
    }

    public OffsetState asOffsetState() {
        return new OffsetState(
                sourceInfo.lsn(),
                sourceInfo.txId(),
                sourceInfo.xmin(),
                sourceInfo.timestamp(),
                sourceInfo.isSnapshot());
    }

    @Override
    public void markLastSnapshotRecord() {
        sourceInfo.setSnapshot(SnapshotRecord.LAST);
    }

    @Override
    public void event(DataCollectionId tableId, Instant instant) {
        sourceInfo.update(instant, (TableId) tableId);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }
}
