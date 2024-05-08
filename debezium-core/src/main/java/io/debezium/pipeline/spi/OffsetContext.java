/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.spi;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.SnapshotRecord;
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
        O load(Map<String, ?> offset);
    }

    Map<String, ?> getOffset();

    Schema getSourceInfoSchema();

    Struct getSourceInfo();

    /**
     * Whether this offset indicates that an (uncompleted) snapshot is currently running or not.
     * @return
     */
    boolean isSnapshotRunning();

    /**
     * Mark the position of the record in the snapshot.
     */
    void markSnapshotRecord(SnapshotRecord record);

    /**
     * Signals that a snapshot will begin, which should reflect in an updated offset state.
     */
    void preSnapshotStart();

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
