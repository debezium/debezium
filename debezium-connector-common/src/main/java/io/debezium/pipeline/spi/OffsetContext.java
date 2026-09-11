/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.spi;

import static io.debezium.pipeline.CommonOffsetContext.SNAPSHOT_COMPLETED_KEY;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.SnapshotType;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Keeps track of the current offset within the source DB's change stream. This reflects in the offset as committed to
 * Kafka and in the source info block contained within CDC messages themselves.
 *
 * @author Gunnar Morling
 *
 */
public interface OffsetContext {

    /**
     * Implementations load a connector-specific offset context based on the offset values stored in Kafka.
     */
    interface Loader<O extends OffsetContext> {

        default Optional<SnapshotType> loadSnapshot(Map<String, ?> offset) {

            Object snapshot = offset.getOrDefault(AbstractSourceInfo.SNAPSHOT_KEY, null);
            // this is to manage transition from a boolean snapshot to SnapshotType
            if (Boolean.TRUE.equals(snapshot) || Boolean.TRUE.toString().equals(snapshot)) {
                return Optional.of(SnapshotType.INITIAL);
            }

            return snapshot == null ? Optional.empty() : Optional.of(SnapshotType.valueOf((String) snapshot));
        }

        default boolean loadSnapshotCompleted(Map<String, ?> offset) {

            return Boolean.TRUE.equals(offset.get(SNAPSHOT_COMPLETED_KEY)) || "true".equals(offset.get(SNAPSHOT_COMPLETED_KEY));
        }

        O load(Map<String, ?> offset);
    }

    Map<String, ?> getOffset();

    /**
     * Captures the offset immediately before processing the next source event.
     * <p>
     * Connectors should invoke this before advancing their offset context to a source event that is about to be
     * dispatched. The captured offset can then be used for records that do not complete processing of that source
     * event, such as the delete and tombstone records produced for a primary key update.
     */
    default void markSourceEventStarted() {
    }

    /**
     * Returns an offset from which the current source event will be replayed after a restart.
     *
     * @return the offset captured before the current source event, or the current offset if the connector does not
     *         provide one
     */
    default Map<String, ?> getOffsetForIncompleteEvent() {
        return getOffset();
    }

    /**
     * Updates the transaction state in the offset captured for the current source event without advancing its source
     * position.
     * <p>
     * This is used when an implicit transaction boundary is emitted while processing a source event. It ensures that
     * the transaction boundary can be restored after a restart while the source event itself is still replayed.
     */
    default void updateTransactionContextForIncompleteEvent() {
    }

    Schema getSourceInfoSchema();

    Struct getSourceInfo();

    /**
     * Whether this offset indicates that an (uncompleted) snapshot is currently running or not.
     * @return
     */
    boolean isInitialSnapshotRunning();

    /**
     * Mark the position of the record in the snapshot.
     */
    void markSnapshotRecord(SnapshotRecord record);

    /**
     * Signals that a snapshot will begin, which should reflect in an updated offset state.
     * @param onDemand indicates whether the snapshot is initial or blocking
     */
    void preSnapshotStart(boolean onDemand);

    /**
     * Signals that a snapshot will complete, which should reflect in an updated offset state.
     */
    void preSnapshotCompletion();

    /**
     * Signals that a snapshot has been completed, which should reflect in an updated offset state.
     */
    void postSnapshotCompletion();

    /**
     * Records the name of the collection and the timestamp of the last event
     */
    void event(DataCollectionId collectionId, Instant timestamp);

    /**
     * Provide a context used by {@link TransactionMonitor} so persist its internal state into offsets to survive
     * between restarts.
     *
     * @return transaction context
     */
    TransactionContext getTransactionContext();

    /**
     * Signals that the streaming of a batch of <i>incremental</i> snapshot events will begin,
     * which should reflect in an updated offset state.
     */
    default void incrementalSnapshotEvents() {
    }

    /**
     * Provide a context used by {@link IncrementalSnapshotChangeEventSource} so persist its internal state into offsets to survive
     * between restarts.
     *
     * @return incremental snapshot context
     */
    default IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return null;
    };
}
