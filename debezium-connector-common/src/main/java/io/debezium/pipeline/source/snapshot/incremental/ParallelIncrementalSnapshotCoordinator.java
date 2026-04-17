/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Coordinates parallel execution of incremental snapshots for multiple tables.
 *
 * <p>Manages a thread pool and connection pool following the same patterns as
 * the chunked initial snapshot infrastructure ({@code ThreadedSnapshotExecutor}
 * and {@code ConcurrentLinkedQueue<JdbcConnection>} pool).
 *
 * <p>Each table gets its own isolated window buffer to prevent thread conflicts.
 *
 * @param <P> the type of partition
 * @param <T> the type of data collection identifier
 */
public class ParallelIncrementalSnapshotCoordinator<P extends Partition, T extends DataCollectionId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelIncrementalSnapshotCoordinator.class);

    private final int threadCount;
    private final ExecutorService executorService;
    private final CompletionService<Void> completionService;
    private final ConcurrentHashMap<T, Map<Struct, Object[]>> windowBuffers;
    private final Queue<JdbcConnection> connectionPool;
    private final List<JdbcConnection> allConnections;
    private int submittedTasks = 0;
    private volatile boolean shutdown = false;

    @FunctionalInterface
    public interface ConnectionFactory {
        JdbcConnection createConnection() throws SQLException;
    }

    public ParallelIncrementalSnapshotCoordinator(int threadCount,
                                                   ConnectionFactory connectionFactory) throws SQLException {
        if (threadCount < 1) {
            throw new IllegalArgumentException("Thread count must be at least 1, got: " + threadCount);
        }

        this.threadCount = threadCount;
        this.windowBuffers = new ConcurrentHashMap<>();

        this.executorService = Executors.newFixedThreadPool(threadCount);
        this.completionService = new ExecutorCompletionService<>(executorService);

        this.connectionPool = new ConcurrentLinkedQueue<>();
        this.allConnections = new ArrayList<>(threadCount);

        for (int i = 0; i < threadCount; i++) {
            JdbcConnection conn = connectionFactory.createConnection();
            connectionPool.add(conn);
            allConnections.add(conn);
        }

        LOGGER.info("Initialized parallel incremental snapshot coordinator with {} threads and {} dedicated connections",
                threadCount, allConnections.size());
    }

    public void submit(Callable<Void> task) {
        if (shutdown) {
            throw new IllegalStateException("Cannot submit task, coordinator has been shutdown");
        }
        completionService.submit(task);
        submittedTasks++;
    }

    public JdbcConnection borrowConnection() {
        return connectionPool.poll();
    }

    public void returnConnection(JdbcConnection connection) {
        if (connection != null) {
            connectionPool.add(connection);
        }
    }

    public void awaitCompletion() throws InterruptedException, ExecutionException {
        for (int i = 0; i < submittedTasks; i++) {
            completionService.take().get();
        }
    }

    public void resetTaskCount() {
        submittedTasks = 0;
    }

    public Map<Struct, Object[]> getWindowBuffer(T dataCollectionId) {
        return windowBuffers.computeIfAbsent(dataCollectionId, k -> new LinkedHashMap<>());
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

        executorService.shutdownNow();

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
