/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.ThreadedSnapshotExecutor;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Reads one chunk per active table in every watermark window, each on a dedicated pooled connection, and buffers the
 * rows per table. Memory is bounded to {@code threadCount x chunkSize} regardless of the number of tables.
 *
 * <p>The connection pool is opened when tables are added and released when the snapshot completes. A pool that
 * cannot be opened after the configured retries fails the snapshot like any other unrecoverable database error.
 */
public class ParallelIncrementalSnapshotReadStrategy<P extends Partition, T extends DataCollectionId>
        implements IncrementalSnapshotReadStrategy<P, T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelIncrementalSnapshotReadStrategy.class);

    private final ParallelIncrementalSnapshotCoordinator<P, T> coordinator;
    private final ParallelIncrementalSnapshotCoordinator.WorkerFactory<P, T> workerFactory;
    private final WatermarkWindow<P> watermarkWindow;
    private final WindowSink<P, T> sink;
    private final CollectionCompletion<P, T> collectionCompletion;
    private final SnapshotProgressListener<P> progressListener;
    private final NotificationService<P, ? extends OffsetContext> notificationService;

    public ParallelIncrementalSnapshotReadStrategy(ParallelIncrementalSnapshotCoordinator<P, T> coordinator,
                                                   ParallelIncrementalSnapshotCoordinator.WorkerFactory<P, T> workerFactory,
                                                   WatermarkWindow<P> watermarkWindow,
                                                   WindowSink<P, T> sink,
                                                   CollectionCompletion<P, T> collectionCompletion,
                                                   SnapshotProgressListener<P> progressListener,
                                                   NotificationService<P, ? extends OffsetContext> notificationService) {
        this.coordinator = coordinator;
        this.workerFactory = workerFactory;
        this.watermarkWindow = watermarkWindow;
        this.sink = sink;
        this.collectionCompletion = collectionCompletion;
        this.progressListener = progressListener;
        this.notificationService = notificationService;
    }

    @Override
    public Map<Struct, Object[]> windowFor(T dataCollectionId) {
        return coordinator.getWindowBuffer(dataCollectionId);
    }

    @Override
    public boolean hasBufferedEvents() {
        return coordinator.hasAnyBufferedEvents();
    }

    @Override
    public boolean buffersEventsFor(IncrementalSnapshotContext<T> context, DataCollectionId dataCollectionId) {
        return true;
    }

    @Override
    public void drainWindows(IncrementalSnapshotContext<T> context, P partition, OffsetContext offsetContext) throws InterruptedException {
        final Map<T, Map<Struct, Object[]>> buffers = coordinator.getAllWindowBuffers();

        offsetContext.incrementalSnapshotEvents();
        for (Map.Entry<T, Map<Struct, Object[]>> entry : buffers.entrySet()) {
            final T tableId = entry.getKey();
            final Map<Struct, Object[]> tableWindow = entry.getValue();
            if (tableWindow.isEmpty()) {
                continue;
            }
            for (Object[] row : tableWindow.values()) {
                sink.send(partition, offsetContext, tableId, row);
            }
            tableWindow.clear();
            final TableSnapshotWorker<P, T> worker = coordinator.getActiveWorkers().get(tableId);
            if (worker != null) {
                worker.getContext().markChunkDrained();
            }
        }
        offsetContext.postSnapshotCompletion();
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean readRound(IncrementalSnapshotContext<T> context, P partition, OffsetContext offsetContext) throws InterruptedException {
        if (context instanceof AbstractIncrementalSnapshotContext<?>) {
            ((AbstractIncrementalSnapshotContext<T>) context).setParallelCoordinator(coordinator);
        }
        try {
            coordinator.ensureInitializedFromContext(context, workerFactory);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to open the parallel snapshot connection pool", e);
        }
        readParallelRound(context, partition, offsetContext);
        return true;
    }

    @Override
    public void tablesAdded(List<DataCollection<T>> tables) {
        LOGGER.info("Adding {} tables to the parallel incremental snapshot ({} worker threads)",
                tables.size(), coordinator.getThreadCount());
        try {
            coordinator.initializeTables(tables, workerFactory);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to open the parallel snapshot connection pool", e);
        }
    }

    @Override
    public void tableRemoved(T tableId) {
        coordinator.removeTable(tableId);
    }

    @Override
    public boolean tableSchemaChanged(T tableId) {
        return coordinator.rereadTable(tableId);
    }

    @Override
    public void snapshotCompleted() {
        coordinator.getConnectionPool().releaseWhenIdle();
    }

    @Override
    public void close() {
        coordinator.shutdown();
    }

    /**
     * Reads one chunk per active table inside a single watermark window. The close of that window drains all
     * per-table buffers and re-enters {@link #readRound} for the next round, mirroring the sequential pattern with
     * N concurrent reads per round.
     */
    private void readParallelRound(IncrementalSnapshotContext<T> context, P partition, OffsetContext offsetContext) throws InterruptedException {
        // Process completions from the PREVIOUS round: their last-chunk data
        // has been drained by drainWindows (called from closeWindow before
        // readChunk re-enters here), so buffers can now be released.
        for (T tableId : coordinator.consumeCompletedTables()) {
            final TableSnapshotWorker<P, T> worker = coordinator.getActiveWorkers().get(tableId);
            if (worker == null) {
                LOGGER.warn("Parallel snapshot of table '{}' completed but its worker is gone, the table was removed in the meantime", tableId);
            }
            else {
                final long rowsRead = worker.getTotalRowsRead();
                LOGGER.info("Parallel snapshot of table '{}' completed ({} rows, {})", tableId, rowsRead, worker.completionStatus());
                progressListener.rowsScanned(partition, (TableId) tableId, rowsRead);
                progressListener.dataCollectionSnapshotCompleted(partition, (TableId) tableId, rowsRead);
                notificationService.incrementalSnapshotNotificationService()
                        .notifyTableScanCompleted(worker.getContext(), partition, offsetContext, rowsRead, worker.completionStatus());
            }
            coordinator.markTableComplete(tableId);
            coordinator.removeWindowBuffer(tableId);
            collectionCompletion.completed(tableId, partition, offsetContext);
        }

        if (!coordinator.hasActiveTables()) {
            return;
        }

        try {
            watermarkWindow.open(partition, offsetContext);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to open watermark window for parallel round", e);
        }

        final List<T> completedThisRound = Collections.synchronizedList(new ArrayList<>());
        final Map<T, TableSnapshotWorker<P, T>> activeWorkers = coordinator.getActiveWorkers();

        DebeziumException primaryFailure = null;
        try (ThreadedSnapshotExecutor executor = new ThreadedSnapshotExecutor(coordinator.getExecutor())) {
            for (Map.Entry<T, TableSnapshotWorker<P, T>> entry : activeWorkers.entrySet()) {
                final T tableId = entry.getKey();
                final TableSnapshotWorker<P, T> worker = entry.getValue();

                executor.submit(() -> {
                    final JdbcConnection conn;
                    try {
                        conn = coordinator.getConnectionPool().borrow();
                    }
                    catch (SQLException borrowFailure) {
                        throw new DebeziumException(
                                "Failed to allocate snapshot connection on " + Thread.currentThread().getName(),
                                borrowFailure);
                    }
                    if (conn == null) {
                        throw new DebeziumException(
                                "Parallel snapshot pool unavailable on " + Thread.currentThread().getName());
                    }
                    boolean discardConnection = false;
                    try {
                        boolean hasMore = worker.readOneChunk(conn);
                        if (!hasMore) {
                            completedThisRound.add(tableId);
                        }
                    }
                    catch (SQLNonTransientConnectionException e) {
                        discardConnection = true;
                        throw e;
                    }
                    finally {
                        if (discardConnection) {
                            coordinator.getConnectionPool().discard(conn);
                        }
                        else {
                            coordinator.getConnectionPool().giveBack(conn);
                        }
                    }
                    return null;
                });
            }

            try {
                executor.awaitCompletion();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                primaryFailure = new DebeziumException("Parallel round interrupted", e);
            }
            catch (ExecutionException e) {
                primaryFailure = new DebeziumException("Parallel round worker failed", e);
            }
        }
        finally {
            // Window must always close: a leaked open window blocks any later snapshot signal.
            try {
                watermarkWindow.close(partition, offsetContext);
            }
            catch (Exception closeError) {
                if (primaryFailure != null) {
                    primaryFailure.addSuppressed(closeError);
                }
                else {
                    primaryFailure = new DebeziumException("Failed to close watermark window for parallel round", closeError);
                }
            }
        }

        if (primaryFailure != null) {
            throw primaryFailure;
        }

        // Workers with no chunk yet are skipped: a null chunk position NPEs in the notification.
        for (Map.Entry<T, TableSnapshotWorker<P, T>> entry : activeWorkers.entrySet()) {
            if (completedThisRound.contains(entry.getKey())) {
                continue;
            }
            final TableSnapshotContext<T> workerContext = entry.getValue().getContext();
            if (workerContext.chunkEndPosititon() != null && workerContext.maximumKey().isPresent()) {
                notificationService.incrementalSnapshotNotificationService()
                        .notifyInProgress(workerContext, partition, offsetContext, entry.getValue().getTotalRowsRead());
            }
        }

        // Don't remove workers/buffers yet. drainWindows needs to drain them
        // first. Store the list for the NEXT round's start.
        coordinator.recordCompletedTables(completedThisRound);
    }
}
