/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

/**
 * A Contract t
 *
 * @author Jiri Pechanec
 *
 * @param <T> data collection id class
 */
public interface IncrementalSnapshotChangeEventSource<P extends Partition, T extends DataCollectionId> {

    /**
     * Recognizes the dialect-specific SQL error a chunk query fails with when it references a
     * column that no longer exists in the database, which the incremental snapshot treats as a
     * stale cached schema and recovers from by deferring the chunk. Connectors opt in by passing
     * their classifier to the change event source; with {@link #NONE} the recovery never
     * triggers and such failures keep their pre-existing handling.
     */
    @FunctionalInterface
    interface UndefinedColumnClassifier {

        UndefinedColumnClassifier NONE = exception -> false;

        boolean isUndefinedColumn(SQLException exception);
    }

    void closeWindow(P partition, String id, OffsetContext offsetContext) throws InterruptedException;

    void pauseSnapshot(P partition, OffsetContext offsetContext) throws InterruptedException;

    void resumeSnapshot(P partition, OffsetContext offsetContext) throws InterruptedException;

    void processMessage(P partition, DataCollectionId dataCollectionId, Object key, OffsetContext offsetContext) throws InterruptedException;

    void init(P partition, OffsetContext offsetContext);

    void addDataCollectionNamesToSnapshot(SignalPayload<P> signalPayload, SnapshotConfiguration snapshotConfiguration)
            throws InterruptedException;

    void requestStopSnapshot(P partition, OffsetContext offsetContext, Map<String, Object> additionalData, List<String> dataCollectionIds);

    default void processHeartbeat(P partition, OffsetContext offsetContext) throws InterruptedException {
    }

    default void processFilteredEvent(P partition, OffsetContext offsetContext) throws InterruptedException {
    }

    default void processTransactionStartedEvent(P partition, OffsetContext offsetContext) throws InterruptedException {
    }

    default void processTransactionCommittedEvent(P partition, OffsetContext offsetContext) throws InterruptedException {
    }

    default void processSchemaChange(P partition, OffsetContext offsetContext, DataCollectionId dataCollectionId) throws InterruptedException {
    }
}
