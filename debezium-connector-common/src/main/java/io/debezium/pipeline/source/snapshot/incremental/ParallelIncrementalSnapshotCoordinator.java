/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.RetryExecutor;
import io.debezium.util.Threads;

/**
 * Tracks the tables a parallel incremental snapshot is reading, their window buffers and their restart state, and
 * owns the worker executor and the {@link SnapshotConnectionPool} the workers read on.
 *
 * @author Ivan Senyk
 * @param <P> the type of partition
 * @param <T> the type of data collection identifier
 */
public class ParallelIncrementalSnapshotCoordinator<P extends Partition, T extends DataCollectionId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelIncrementalSnapshotCoordinator.class);

    private static final long EXECUTOR_TERMINATION_TIMEOUT_SECONDS = 10L;

    private final int threadCount;
    private final SnapshotConnectionPool connectionPool;
    private final ConcurrentHashMap<T, Map<Struct, Object[]>> windowBuffers = new ConcurrentHashMap<>();
    private final Map<T, TableSnapshotWorker<P, T>> activeWorkers = new ConcurrentHashMap<>();
    private final Queue<DataCollection<T>> pendingTables = new ConcurrentLinkedQueue<>();
    private final List<T> completedTables = new ArrayList<>();
    private final Map<T, AbstractIncrementalSnapshotContext.PerTableStateSnapshot> pendingRestoredState = new HashMap<>();
    private final ExecutorService executor;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private volatile WorkerFactory<P, T> workerFactory;

    @FunctionalInterface
    public interface WorkerFactory<P extends Partition, T extends DataCollectionId> {
        TableSnapshotWorker<P, T> create(DataCollection<T> dataCollection, Map<Struct, Object[]> windowBuffer);
    }

    public ParallelIncrementalSnapshotCoordinator(int threadCount,
                                                  SnapshotConnectionPool.ConnectionFactory connectionFactory,
                                                  String connectorName,
                                                  long releaseDelayMs,
                                                  RetryExecutor retryPolicy) {
        if (threadCount < 1) {
            throw new IllegalArgumentException("Thread count must be at least 1, got: " + threadCount);
        }
        this.threadCount = threadCount;
        this.connectionPool = new SnapshotConnectionPool(threadCount, connectionFactory, this::isIdle, connectorName, releaseDelayMs, retryPolicy);
        this.executor = Threads.newFixedThreadPool(
                ParallelIncrementalSnapshotCoordinator.class, connectorName, "snapshot-worker", threadCount);

        LOGGER.info("Initialized parallel incremental snapshot coordinator with capacity for {} worker threads, pool release delay {} ms",
                threadCount, releaseDelayMs);
    }

    private boolean isIdle() {
        return activeWorkers.isEmpty() && pendingTables.isEmpty();
    }

    public SnapshotConnectionPool getConnectionPool() {
        return connectionPool;
    }

    public synchronized void initializeTables(List<DataCollection<T>> tables, WorkerFactory<P, T> workerFactory) throws SQLException {
        connectionPool.open();
        this.workerFactory = workerFactory;
        final Set<T> known = new HashSet<>(activeWorkers.keySet());
        for (DataCollection<T> dc : pendingTables) {
            known.add(dc.getId());
        }
        for (DataCollection<T> dc : tables) {
            if (known.add(dc.getId())) {
                pendingTables.add(dc);
            }
        }
        activateNextTables();
        LOGGER.info("Parallel snapshot initialized: {} active, {} pending",
                activeWorkers.size(), pendingTables.size());
    }

    public synchronized void ensureInitializedFromContext(IncrementalSnapshotContext<T> context,
                                                          WorkerFactory<P, T> workerFactory)
            throws SQLException {
        if (!isIdle()) {
            return;
        }
        final List<DataCollection<T>> remaining = context.getDataCollections();
        if (remaining == null || remaining.isEmpty()) {
            return;
        }
        if (context instanceof AbstractIncrementalSnapshotContext<?>) {
            @SuppressWarnings("unchecked")
            final var abstractCtx = (AbstractIncrementalSnapshotContext<T>) context;
            pendingRestoredState.putAll(abstractCtx.consumeRestoredPerTableState());
        }
        LOGGER.info("Refilling parallel coordinator from offset context ({} pending tables, {} with restored state)",
                remaining.size(), pendingRestoredState.size());
        initializeTables(remaining, workerFactory);
    }

    private void activateNextTables() {
        while (activeWorkers.size() < threadCount) {
            final DataCollection<T> dc = pendingTables.poll();
            if (dc == null) {
                break;
            }
            final TableSnapshotWorker<P, T> worker = workerFactory.create(dc, getWindowBuffer(dc.getId()));
            activeWorkers.put(dc.getId(), worker);
            injectRestoredState(dc.getId(), worker);
            LOGGER.debug("Activated table {} for parallel snapshot", dc.getId());
        }
    }

    private void injectRestoredState(T tableId, TableSnapshotWorker<P, T> worker) {
        final AbstractIncrementalSnapshotContext.PerTableStateSnapshot state = pendingRestoredState.remove(tableId);
        if (state == null) {
            return;
        }
        final TableSnapshotContext<T> ctx = worker.getContext();
        if (state.maximumKey() != null) {
            ctx.maximumKey(state.maximumKey());
        }
        if (state.chunkPosition() != null) {
            ctx.nextChunkPosition(state.chunkPosition());
            // The restored position is the last key emitted before the restart; seed it as such or a store() before
            // the first drain would regress the persisted position to null.
            ctx.sendEvent(state.chunkPosition());
        }
        LOGGER.info("Injected restored per-table state for {} on activation: chunkPos={}, maxKey={}",
                tableId, state.chunkPosition() != null, state.maximumKey() != null);
    }

    public Map<T, TableSnapshotWorker<P, T>> getActiveWorkers() {
        return Collections.unmodifiableMap(activeWorkers);
    }

    public synchronized void markTableComplete(T tableId) {
        activeWorkers.remove(tableId);
        activateNextTables();
        if (isIdle()) {
            connectionPool.releaseWhenIdle();
        }
    }

    public synchronized Map<T, AbstractIncrementalSnapshotContext.PerTableStateSnapshot> snapshotPerTableState() {
        if (activeWorkers.isEmpty() && pendingRestoredState.isEmpty()) {
            return Map.of();
        }
        final Map<T, AbstractIncrementalSnapshotContext.PerTableStateSnapshot> state = new LinkedHashMap<>();
        for (Map.Entry<T, TableSnapshotWorker<P, T>> entry : activeWorkers.entrySet()) {
            final TableSnapshotContext<T> ctx = entry.getValue().getContext();
            state.put(entry.getKey(), new AbstractIncrementalSnapshotContext.PerTableStateSnapshot(
                    ctx.lastEventKeySent(),
                    ctx.maximumKey().orElse(null)));
        }
        // Restored state for tables still awaiting activation must survive the
        // next offset write, or a second crash in that window would lose it.
        pendingRestoredState.forEach(state::putIfAbsent);
        return state;
    }

    public synchronized boolean rereadTable(T tableId) {
        final TableSnapshotWorker<P, T> worker = activeWorkers.get(tableId);
        if (worker == null) {
            return false;
        }
        worker.invalidateSchemaCache();
        final Map<Struct, Object[]> buffer = windowBuffers.get(tableId);
        if (buffer == null || buffer.isEmpty()) {
            LOGGER.info("Invalidated schema cache of table {} after schema change", tableId);
            return true;
        }
        // Undrained rows were read with the old schema: discard them and re-read the chunk, as the sequential
        // rereadChunk does.
        buffer.clear();
        worker.getContext().revertChunk();
        LOGGER.info("Invalidated schema cache, discarded the buffered chunk and reverted the position of table {} after schema change", tableId);
        return true;
    }

    public synchronized boolean removeTable(T tableId) {
        final boolean removedPending = pendingTables.removeIf(dc -> dc.getId().equals(tableId));
        final boolean removedActive = activeWorkers.remove(tableId) != null;
        windowBuffers.remove(tableId);
        // A stop signal invalidates any restored resume position: a later
        // re-signal of the same table is a fresh snapshot request.
        pendingRestoredState.remove(tableId);
        if (removedActive) {
            activateNextTables();
        }
        if (isIdle()) {
            connectionPool.releaseWhenIdle();
        }
        return removedPending || removedActive;
    }

    public boolean hasActiveTables() {
        return !activeWorkers.isEmpty();
    }

    public synchronized void recordCompletedTables(List<T> tableIds) {
        completedTables.addAll(tableIds);
    }

    public synchronized List<T> consumeCompletedTables() {
        if (completedTables.isEmpty()) {
            return List.of();
        }
        final List<T> result = new ArrayList<>(completedTables);
        completedTables.clear();
        return result;
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    /**
     * Returns the window buffer of an active table, creating it on first use. Tables without a worker get no buffer,
     * see {@link #windowBufferFor}.
     */
    public Map<Struct, Object[]> getWindowBuffer(T dataCollectionId) {
        return windowBuffers.computeIfAbsent(dataCollectionId, k -> new LinkedHashMap<>());
    }

    /**
     * Returns the window buffer of a table, or an empty map when the table has none.
     */
    public Map<Struct, Object[]> windowBufferFor(T dataCollectionId) {
        return windowBuffers.getOrDefault(dataCollectionId, Collections.emptyMap());
    }

    public Map<T, Map<Struct, Object[]>> getAllWindowBuffers() {
        return Collections.unmodifiableMap(windowBuffers);
    }

    /**
     * Returns true if at least one per-table window buffer currently holds
     * events. Bounded by the number of active workers, so iterating here is
     * O(snapshot.max.threads) and intentionally lock-free.
     */
    public boolean hasAnyBufferedEvents() {
        for (Map<Struct, Object[]> buffer : windowBuffers.values()) {
            if (!buffer.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    public void removeWindowBuffer(T dataCollectionId) {
        final Map<Struct, Object[]> removed = windowBuffers.remove(dataCollectionId);
        if (removed != null) {
            LOGGER.debug("Removed window buffer for table {} (size: {})", dataCollectionId, removed.size());
        }
    }

    public void shutdown() {
        if (!shutdown.compareAndSet(false, true)) {
            return;
        }

        LOGGER.info("Shutting down parallel incremental snapshot coordinator");
        executor.shutdownNow();
        connectionPool.close();

        activeWorkers.clear();
        pendingTables.clear();
        windowBuffers.clear();
        synchronized (this) {
            pendingRestoredState.clear();
        }

        try {
            if (!executor.awaitTermination(EXECUTOR_TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                LOGGER.warn("Snapshot worker executor did not terminate within {} seconds of shutdownNow", EXECUTOR_TERMINATION_TIMEOUT_SECONDS);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Interrupted while waiting for the snapshot worker executor to terminate");
        }

        LOGGER.info("Parallel incremental snapshot coordinator shutdown complete");
    }

    public int getThreadCount() {
        return threadCount;
    }
}
