/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Manages the JDBC connection pool, per-table window buffers, and per-table
 * snapshot state for parallel incremental snapshots.
 *
 * <p>The parallel snapshot follows the same readChunk/closeWindow cycle as the
 * sequential path but reads N chunks (one per active table) in parallel per
 * round. This class tracks which tables are currently being snapshotted and
 * which are queued, activating up to {@code threadCount} tables at a time.
 *
 * @author Ivan Senyk
 * @param <P> the type of partition
 * @param <T> the type of data collection identifier
 */
public class ParallelIncrementalSnapshotCoordinator<P extends Partition, T extends DataCollectionId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelIncrementalSnapshotCoordinator.class);

    private final int threadCount;
    private final ConcurrentHashMap<T, Map<Struct, Object[]>> windowBuffers;
    private final Queue<JdbcConnection> connectionPool;
    private final List<JdbcConnection> allConnections;
    private final Map<T, TableSnapshotWorker<P, T>> activeWorkers;
    private final Queue<DataCollection<T>> pendingTables;
    private final List<T> completedTables;
    private volatile boolean shutdown = false;
    private WorkerFactory<P, T> workerFactory;

    @FunctionalInterface
    public interface ConnectionFactory {
        JdbcConnection createConnection() throws SQLException;
    }

    @FunctionalInterface
    public interface WorkerFactory<P extends Partition, T extends DataCollectionId> {
        TableSnapshotWorker<P, T> create(DataCollection<T> dataCollection, Map<Struct, Object[]> windowBuffer);
    }

    public ParallelIncrementalSnapshotCoordinator(int threadCount,
                                                  ConnectionFactory connectionFactory)
            throws SQLException {
        if (threadCount < 1) {
            throw new IllegalArgumentException("Thread count must be at least 1, got: " + threadCount);
        }

        this.threadCount = threadCount;
        this.windowBuffers = new ConcurrentHashMap<>();
        this.activeWorkers = new ConcurrentHashMap<>();
        this.pendingTables = new ConcurrentLinkedQueue<>();
        this.completedTables = new ArrayList<>();

        this.connectionPool = new ConcurrentLinkedQueue<>();
        this.allConnections = new ArrayList<>(threadCount);

        try {
            for (int i = 0; i < threadCount; i++) {
                JdbcConnection conn = connectionFactory.createConnection();
                connectionPool.add(conn);
                allConnections.add(conn);
            }
        }
        catch (SQLException | RuntimeException e) {
            // Close any connections opened so far before propagating; the coordinator
            // is never constructed on failure, so without this they would leak.
            for (JdbcConnection opened : allConnections) {
                try {
                    opened.close();
                }
                catch (SQLException closeError) {
                    e.addSuppressed(closeError);
                }
            }
            allConnections.clear();
            connectionPool.clear();
            throw e;
        }

        LOGGER.info("Initialized parallel incremental snapshot coordinator with {} threads and {} dedicated connections",
                threadCount, allConnections.size());
    }

    public void initializeTables(List<DataCollection<T>> tables, WorkerFactory<P, T> workerFactory) {
        this.workerFactory = workerFactory;
        pendingTables.addAll(tables);
        activateNextTables();
        LOGGER.info("Parallel snapshot initialized: {} active, {} pending",
                activeWorkers.size(), pendingTables.size());
    }

    private void activateNextTables() {
        while (activeWorkers.size() < threadCount) {
            DataCollection<T> dc = pendingTables.poll();
            if (dc == null) {
                break;
            }
            activeWorkers.put(dc.getId(), workerFactory.create(dc, getWindowBuffer(dc.getId())));
            LOGGER.debug("Activated table {} for parallel snapshot", dc.getId());
        }
    }

    public Map<T, TableSnapshotWorker<P, T>> getActiveWorkers() {
        return activeWorkers;
    }

    public void markTableComplete(T tableId) {
        activeWorkers.remove(tableId);
        activateNextTables();
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
        List<T> result = new ArrayList<>(completedTables);
        completedTables.clear();
        return result;
    }

    public JdbcConnection borrowConnection() {
        return connectionPool.poll();
    }

    public void returnConnection(JdbcConnection connection) {
        if (connection != null) {
            connectionPool.add(connection);
        }
    }

    public Map<Struct, Object[]> getWindowBuffer(T dataCollectionId) {
        // ConcurrentHashMap: workers put concurrently with CDC consumer remove/iterate.
        return windowBuffers.computeIfAbsent(dataCollectionId, k -> new ConcurrentHashMap<>());
    }

    public Map<T, Map<Struct, Object[]>> getAllWindowBuffers() {
        return windowBuffers;
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
        Map<Struct, Object[]> removed = windowBuffers.remove(dataCollectionId);
        if (removed != null) {
            LOGGER.debug("Removed window buffer for table {} (size: {})", dataCollectionId, removed.size());
        }
    }

    public void shutdown() {
        if (shutdown) {
            return;
        }

        LOGGER.info("Shutting down parallel incremental snapshot coordinator");
        shutdown = true;

        activeWorkers.clear();
        pendingTables.clear();
        windowBuffers.clear();
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

        LOGGER.info("Parallel incremental snapshot coordinator shutdown complete");
    }

    public int getThreadCount() {
        return threadCount;
    }

    public int getConnectionPoolSize() {
        return allConnections.size();
    }

    public boolean isShutdown() {
        return shutdown;
    }
}
