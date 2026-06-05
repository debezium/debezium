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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.VisibleForTesting;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Threads;

/**
 * Manages the JDBC connection pool, per-table window buffers, and per-table
 * snapshot state for parallel incremental snapshots.
 *
 * @author Ivan Senyk
 * @param <P> the type of partition
 * @param <T> the type of data collection identifier
 */
public class ParallelIncrementalSnapshotCoordinator<P extends Partition, T extends DataCollectionId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelIncrementalSnapshotCoordinator.class);

    private static final long EXECUTOR_TERMINATION_TIMEOUT_SECONDS = 10L;

    public enum PoolLifecycleState {
        CLOSED,
        OPEN
    }

    private final int threadCount;
    private final ConnectionFactory connectionFactory;
    private final long releaseDelayMs;
    private final ConcurrentHashMap<T, Map<Struct, Object[]>> windowBuffers;
    private final Queue<JdbcConnection> connectionPool;
    private final List<JdbcConnection> allConnections;
    private final Map<T, TableSnapshotWorker<P, T>> activeWorkers;
    private final Queue<DataCollection<T>> pendingTables;
    private final List<T> completedTables;
    private final ExecutorService executor;
    private final ScheduledExecutorService releaseScheduler;
    private final AtomicReference<PoolLifecycleState> poolState = new AtomicReference<>(PoolLifecycleState.CLOSED);
    private final ReentrantLock lifecycleLock = new ReentrantLock();
    /**
     * Incremented on every (re)open. A scheduled release captures the value at
     * schedule time and bails out when it finds a newer one, closing the race
     * with an ensurePoolOpen that runs while the release task is already
     * blocked on lifecycleLock.
     */
    private final AtomicLong releaseGeneration = new AtomicLong(0);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final Map<T, AbstractIncrementalSnapshotContext.PerTableStateSnapshot> pendingRestoredState = new HashMap<>();
    private volatile ScheduledFuture<?> pendingReleaseFuture;
    private volatile WorkerFactory<P, T> workerFactory;

    @FunctionalInterface
    public interface ConnectionFactory {
        JdbcConnection createConnection() throws SQLException;
    }

    @FunctionalInterface
    public interface WorkerFactory<P extends Partition, T extends DataCollectionId> {
        TableSnapshotWorker<P, T> create(DataCollection<T> dataCollection, Map<Struct, Object[]> windowBuffer);
    }

    public ParallelIncrementalSnapshotCoordinator(int threadCount,
                                                  ConnectionFactory connectionFactory,
                                                  String connectorName,
                                                  long releaseDelayMs) {
        if (threadCount < 1) {
            throw new IllegalArgumentException("Thread count must be at least 1, got: " + threadCount);
        }
        if (releaseDelayMs < 0) {
            throw new IllegalArgumentException("Release delay must be non-negative, got: " + releaseDelayMs);
        }

        this.threadCount = threadCount;
        this.connectionFactory = connectionFactory;
        this.releaseDelayMs = releaseDelayMs;
        this.windowBuffers = new ConcurrentHashMap<>();
        this.activeWorkers = new ConcurrentHashMap<>();
        this.pendingTables = new ConcurrentLinkedQueue<>();
        this.completedTables = new ArrayList<>();
        this.connectionPool = new ConcurrentLinkedQueue<>();
        this.allConnections = new CopyOnWriteArrayList<>();
        this.executor = Threads.newFixedThreadPool(
                ParallelIncrementalSnapshotCoordinator.class, connectorName, "snapshot-worker", threadCount);
        this.releaseScheduler = Threads.newSingleThreadScheduledExecutor(
                ParallelIncrementalSnapshotCoordinator.class, connectorName, "pool-release", true);

        LOGGER.info("Initialized parallel incremental snapshot coordinator with capacity for {} worker threads, pool release delay {} ms",
                threadCount, releaseDelayMs);
    }

    public void ensurePoolOpen() throws SQLException {
        lifecycleLock.lock();
        try {
            releaseGeneration.incrementAndGet();
            cancelPendingReleaseLocked();

            if (poolState.get() == PoolLifecycleState.OPEN) {
                return;
            }
            try {
                for (int i = 0; i < threadCount; i++) {
                    final JdbcConnection conn = connectionFactory.createConnection();
                    connectionPool.add(conn);
                    allConnections.add(conn);
                }
                poolState.set(PoolLifecycleState.OPEN);
                LOGGER.info("Parallel snapshot connection pool opened ({} connections)", threadCount);
            }
            catch (SQLException | RuntimeException e) {
                drainConnectionsQuietly();
                throw e;
            }
        }
        finally {
            lifecycleLock.unlock();
        }
    }

    private void releasePoolIfIdle() {
        releasePoolIfIdleAtGeneration(releaseGeneration.get());
    }

    private void releasePoolIfIdleAtGeneration(long expectedGeneration) {
        lifecycleLock.lock();
        try {
            if (releaseGeneration.get() != expectedGeneration) {
                return;
            }
            if (poolState.get() != PoolLifecycleState.OPEN) {
                return;
            }
            if (!activeWorkers.isEmpty() || !pendingTables.isEmpty()) {
                return;
            }
            drainConnectionsQuietly();
            poolState.set(PoolLifecycleState.CLOSED);
            LOGGER.info("Parallel snapshot connection pool released (no active or pending tables)");
        }
        finally {
            lifecycleLock.unlock();
        }
    }

    private void drainConnectionsQuietly() {
        for (JdbcConnection conn : allConnections) {
            closeQuietly(conn, "pool drain");
        }
        allConnections.clear();
        connectionPool.clear();
    }

    private static void closeQuietly(JdbcConnection conn, String context) {
        if (conn == null) {
            return;
        }
        try {
            conn.close();
        }
        catch (SQLException e) {
            LOGGER.trace("Failed to close snapshot connection during {}", context, e);
        }
    }

    public synchronized void initializeTables(List<DataCollection<T>> tables, WorkerFactory<P, T> workerFactory) throws SQLException {
        ensurePoolOpen();
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
        if (!activeWorkers.isEmpty() || !pendingTables.isEmpty()) {
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
            // The restored position came from the offset, so it is a proven-safe
            // restart point; seed it as such or a store() before the first drain
            // would regress the persisted position to null.
            ctx.markChunkDrained();
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
        if (activeWorkers.isEmpty() && pendingTables.isEmpty()) {
            scheduleReleaseIfNotPending();
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
                    ctx.safeResumePosition(),
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
        final Map<Struct, Object[]> buffer = windowBuffers.get(tableId);
        if (buffer != null) {
            buffer.clear();
        }
        // The discarded events must be re-read: rewind to the last drained
        // position, mirroring the sequential rereadChunk revert.
        worker.getContext().revertChunk();
        worker.invalidateSchemaCache();
        LOGGER.info("Invalidated schema cache, cleared window buffer and reverted chunk for table {} after schema change", tableId);
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
        if (activeWorkers.isEmpty() && pendingTables.isEmpty()) {
            scheduleReleaseIfNotPending();
        }
        return removedPending || removedActive;
    }

    public void scheduleReleaseIfNotPending() {
        if (poolState.get() != PoolLifecycleState.OPEN) {
            return;
        }
        lifecycleLock.lock();
        try {
            if (poolState.get() != PoolLifecycleState.OPEN) {
                return;
            }
            if (releaseDelayMs == 0) {
                releasePoolIfIdle();
                return;
            }
            if (pendingReleaseFuture != null && !pendingReleaseFuture.isDone()) {
                return;
            }
            final long generation = releaseGeneration.get();
            pendingReleaseFuture = releaseScheduler.schedule(
                    () -> releasePoolIfIdleAtGeneration(generation),
                    releaseDelayMs, TimeUnit.MILLISECONDS);
        }
        finally {
            lifecycleLock.unlock();
        }
    }

    private void cancelPendingReleaseLocked() {
        if (pendingReleaseFuture != null) {
            pendingReleaseFuture.cancel(false);
            pendingReleaseFuture = null;
        }
    }

    public boolean hasActiveTables() {
        return !activeWorkers.isEmpty();
    }

    public void recordCompletedTables(List<T> tableIds) {
        completedTables.addAll(tableIds);
    }

    public List<T> consumeCompletedTables() {
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
     * Returns a usable connection from the pool, or {@code null} when the
     * coordinator is shutting down or the pool is currently empty. Throws only
     * when reallocating a replacement for a stale connection fails on the
     * database side.
     */
    public JdbcConnection borrowConnection() throws SQLException {
        if (shutdown.get()) {
            return null;
        }
        final JdbcConnection conn = connectionPool.poll();
        if (conn == null) {
            return null;
        }
        try {
            if (conn.isValid()) {
                return conn;
            }
        }
        catch (SQLException e) {
            LOGGER.trace("Pooled snapshot connection failed validation, will be evicted", e);
        }
        allConnections.remove(conn);
        closeQuietly(conn, "stale connection eviction");

        final JdbcConnection fresh = connectionFactory.createConnection();
        // Recheck shutdown before publishing the new connection so a concurrent
        // shutdown that already iterated allConnections cannot leak it.
        if (shutdown.get()) {
            closeQuietly(fresh, "shutdown-coincident borrow");
            return null;
        }
        allConnections.add(fresh);
        if (shutdown.get()) {
            allConnections.remove(fresh);
            closeQuietly(fresh, "shutdown-coincident borrow");
            return null;
        }
        return fresh;
    }

    public void returnConnection(JdbcConnection connection) {
        if (connection != null) {
            connectionPool.add(connection);
        }
    }

    public Map<Struct, Object[]> getWindowBuffer(T dataCollectionId) {
        return windowBuffers.computeIfAbsent(dataCollectionId, k -> new ConcurrentHashMap<>());
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
        releaseScheduler.shutdownNow();
        executor.shutdownNow();

        lifecycleLock.lock();
        try {
            cancelPendingReleaseLocked();
            for (JdbcConnection conn : allConnections) {
                try {
                    if (conn != null && conn.isConnected()) {
                        conn.close();
                    }
                }
                catch (SQLException e) {
                    LOGGER.warn("Error closing snapshot connection during shutdown", e);
                }
            }
            allConnections.clear();
            connectionPool.clear();
            poolState.set(PoolLifecycleState.CLOSED);
        }
        finally {
            lifecycleLock.unlock();
        }

        activeWorkers.clear();
        pendingTables.clear();
        windowBuffers.clear();
        synchronized (this) {
            pendingRestoredState.clear();
        }

        awaitExecutorTerminationQuietly(executor, "snapshot worker executor");
        awaitExecutorTerminationQuietly(releaseScheduler, "release scheduler");

        LOGGER.info("Parallel incremental snapshot coordinator shutdown complete");
    }

    private static void awaitExecutorTerminationQuietly(ExecutorService es, String name) {
        try {
            if (!es.awaitTermination(EXECUTOR_TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                LOGGER.warn("{} did not terminate within {} seconds of shutdownNow", name, EXECUTOR_TERMINATION_TIMEOUT_SECONDS);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Interrupted while waiting for {} to terminate", name);
        }
    }

    public int getThreadCount() {
        return threadCount;
    }

    public int getConnectionPoolSize() {
        return allConnections.size();
    }

    public boolean isShutdown() {
        return shutdown.get();
    }

    @VisibleForTesting
    PoolLifecycleState getPoolState() {
        return poolState.get();
    }
}
