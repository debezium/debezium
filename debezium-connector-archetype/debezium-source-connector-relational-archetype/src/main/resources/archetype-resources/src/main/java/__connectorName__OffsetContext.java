/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.SnapshotType;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Tracks the current read position within the ${connectorName} data source.
 *
 * <p>The offset map returned by {@link #getOffset()} is persisted by the Kafka Connect
 * framework and used to resume after a restart. Replace the placeholder implementation
 * with the fields meaningful to your connector (e.g. file position, LSN, sequence ID).
 */
public class ${connectorName}OffsetContext extends CommonOffsetContext<${connectorName}SourceInfo> {

    static final String POSITION_KEY = "position";

    private long position;
    private final TransactionContext transactionContext;

    public ${connectorName}OffsetContext(${connectorName}SourceInfo sourceInfo) {
        this(sourceInfo, null, false, new TransactionContext());
    }

    /**
     * Restores an offset from what {@link #getOffset()} stored, including whether a snapshot was
     * running when the connector stopped.
     */
    ${connectorName}OffsetContext(${connectorName}SourceInfo sourceInfo, SnapshotType snapshot,
                                  boolean snapshotCompleted, TransactionContext transactionContext) {
        super(sourceInfo, snapshotCompleted);
        if (snapshotCompleted) {
            postSnapshotCompletion();
        }
        else {
            setSnapshot(snapshot);
            sourceInfo.setSnapshot(snapshot != null ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }
        this.transactionContext = transactionContext;
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    @Override
    public Map<String, ?> getOffset() {
        final Map<String, Object> result = new HashMap<>();
        result.put(POSITION_KEY, position);
        // Persist the snapshot state so a restart in the middle of a snapshot is detected and the snapshot
        // is re-run instead of being mistaken for a completed one.
        if (getSnapshot().isPresent()) {
            result.put(AbstractSourceInfo.SNAPSHOT_KEY, getSnapshot().get().toString());
            result.put(SNAPSHOT_COMPLETED_KEY, snapshotCompleted);
        }
        // Transaction metadata is only tracked for streamed events.
        return sourceInfo.isSnapshot() ? result : transactionContext.store(result);
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfo.schema();
    }

    @Override
    public void event(DataCollectionId dataCollectionId, Instant instant) {
        // Record the table and time of the event before it is enqueued; they end up in the source block.
        sourceInfo.update(instant, (TableId) dataCollectionId);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }
}
