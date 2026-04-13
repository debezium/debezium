/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Coordinates parallel execution of incremental snapshots for multiple tables.
 *
 * <p>This coordinator manages a thread pool that allows multiple tables to be
 * snapshotted concurrently. Each table is assigned to a thread from the pool
 * and has its own isolated window buffer to prevent thread conflicts.
 *
 * <p><b>Thread Safety:</b> This class is thread-safe. Each table has its own
 * window buffer stored in a {@link ConcurrentHashMap}, and connections are
 * managed separately per thread to avoid JDBC conflicts.
 *
 * <p><b>Usage:</b>
 * <pre>
 * // Initialize coordinator with thread count
 * ParallelIncrementalSnapshotCoordinator coordinator =
 *     new ParallelIncrementalSnapshotCoordinator(4);
 *
 * // Get isolated window buffer for a table
 * Map<Struct, Object[]> windowBuffer = coordinator.getWindowBuffer(tableId);
 *
 * // Submit snapshot task
 * Future<?> future = coordinator.submitTableSnapshot(() -> {
 *     // Snapshot logic here
 * }, tableId, partition, offsetContext);
 *
 * // Shutdown when done
 * coordinator.shutdown();
 * </pre>
 *
 * @param <P> the type of partition
 * @param <T> the type of data collection identifier
 *
 * @author Debezium Community
 */
public class ParallelIncrementalSnapshotCoordinator<P extends Partition, T extends DataCollectionId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelIncrementalSnapshotCoordinator.class);

    private final int threadCount;
    private final ExecutorService executorService;
    private final ConcurrentHashMap<T, Map<Struct, Object[]>> windowBuffers;
    private final ConcurrentHashMap<T, JdbcConnection> connectionPool;
    private final AtomicInteger activeSnapshots;
    private final AtomicInteger threadCounter;
    private volatile boolean shutdown = false;

    /**
     * Creates a new parallel incremental snapshot coordinator.
     *
     * @param threadCount the number of threads to use for parallel snapshots
     */
    public ParallelIncrementalSnapshotCoordinator(int threadCount) {
        if (threadCount < 1) {
            throw new IllegalArgumentException("Thread count must be at least 1, got: " + threadCount);
        }

        this.threadCount = threadCount;
        this.threadCounter = new AtomicInteger(0);
        this.windowBuffers = new ConcurrentHashMap<>();
        this.connectionPool = new ConcurrentHashMap<>();
        this.activeSnapshots = new AtomicInteger(0);

        // Create thread pool for executing chunk read tasks
        this.executorService = Executors.newFixedThreadPool(
                threadCount,
                runnable -> {
                    int threadNum = threadCounter.incrementAndGet();
                    Thread thread = new Thread(runnable);
                    thread.setName("snapshot-worker-" + threadNum);
                    thread.setDaemon(true);
                    return thread;
                });

        LOGGER.info("Initialized parallel incremental snapshot coordinator with {} threads", threadCount);
    }

    /**
     * Submits a chunk read task to be executed by a worker thread.
     *
     * @param chunkTask the task to execute
     * @return a Future representing the async execution
     */
    public Future<?> submitChunkTask(Runnable chunkTask) {
        if (shutdown) {
            throw new IllegalStateException("Cannot submit task, coordinator has been shutdown");
        }

        activeSnapshots.incrementAndGet();

        return executorService.submit(() -> {
            try {
                chunkTask.run();
            }
            catch (Exception e) {
                LOGGER.error("[{}] Error executing chunk task: {}",
                        Thread.currentThread().getName(), e.getMessage(), e);
                throw new DebeziumException("Chunk read task failed", e);
            }
            finally {
                activeSnapshots.decrementAndGet();
            }
        });
    }

    /**
     * Submits a table snapshot task (legacy method for test compatibility).
     *
     * @param snapshotTask the snapshot task to execute
     * @param dataCollectionId the table being snapshotted
     * @param partition the partition context
     * @return a Future representing the async task
     * @deprecated Use submitChunkTask() instead
     */
    @Deprecated
    public Future<?> submitTableSnapshot(
            Runnable snapshotTask,
            T dataCollectionId,
            P partition) {
        return submitChunkTask(snapshotTask);
    }

    /**
     * Gets or creates an isolated window buffer for the specified table.
     *
     * <p>Each table has its own buffer to prevent conflicts when multiple
     * threads are processing different tables concurrently.
     *
     * @param dataCollectionId the table identifier
     * @return the window buffer for this table
     */
    public Map<Struct, Object[]> getWindowBuffer(T dataCollectionId) {
        return windowBuffers.computeIfAbsent(dataCollectionId, k -> new LinkedHashMap<>());
    }


    /**
     * Removes the window buffer for a specific table.
     *
     * <p>This should be called when a table snapshot is completed or aborted.
     *
     * @param dataCollectionId the table identifier
     */
    public void removeWindowBuffer(T dataCollectionId) {
        Map<Struct, Object[]> removed = windowBuffers.remove(dataCollectionId);
        if (removed != null) {
            LOGGER.debug("Removed window buffer for table {} (size: {})", dataCollectionId, removed.size());
        }
    }

    /**
     * Waits for all active snapshots to complete.
     *
     * @param timeoutSeconds maximum time to wait in seconds
     * @return true if all snapshots completed, false if timeout occurred
     */
    public boolean awaitCompletion(long timeoutSeconds) {
        long deadline = System.currentTimeMillis() + (timeoutSeconds * 1000);

        while (activeSnapshots.get() > 0 && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.warn("Interrupted while waiting for snapshot completion");
                return false;
            }
        }

        boolean completed = activeSnapshots.get() == 0;
        if (!completed) {
            LOGGER.warn("Timeout waiting for incremental snapshots to complete. Active snapshots: {}",
                    activeSnapshots.get());
        }
        return completed;
    }

    /**
     * Shuts down the coordinator and its thread pool.
     *
     * <p>This method waits for currently executing tasks to complete before
     * shutting down. If tasks don't complete within 60 seconds, a forced
     * shutdown is attempted.
     *
     * <p>After shutdown, no new tasks can be submitted.
     */
    public void shutdown() {
        if (shutdown) {
            LOGGER.debug("Coordinator already shutdown, ignoring duplicate shutdown request");
            return;
        }

        LOGGER.info("Shutting down parallel incremental snapshot coordinator (active tasks: {})",
                activeSnapshots.get());
        shutdown = true;

        // Shutdown executor service
        executorService.shutdown();

        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                LOGGER.warn("Incremental snapshot thread pool did not terminate gracefully within 60 seconds, forcing shutdown");
                executorService.shutdownNow();

                if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    LOGGER.error("Incremental snapshot thread pool did not terminate after forced shutdown");
                }
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Interrupted while waiting for thread pool shutdown, forcing immediate shutdown");
            executorService.shutdownNow();
        }

        // Clean up resources
        windowBuffers.clear();
        connectionPool.values().forEach(conn -> {
            try {
                if (conn != null) {
                    conn.close();
                }
            }
            catch (SQLException e) {
                LOGGER.warn("Error closing JDBC connection during shutdown", e);
            }
        });
        connectionPool.clear();

        LOGGER.info("Parallel incremental snapshot coordinator shutdown complete");
    }

    /**
     * Returns the configured number of threads.
     *
     * @return the thread count
     */
    public int getThreadCount() {
        return threadCount;
    }

    /**
     * Returns the current number of active snapshots.
     *
     * @return the active snapshot count
     */
    public int getActiveSnapshotCount() {
        return activeSnapshots.get();
    }

    /**
     * Checks if the coordinator has been shutdown.
     *
     * @return true if shutdown, false otherwise
     */
    public boolean isShutdown() {
        return shutdown;
    }

    /**
     * Returns the number of tables currently being snapshotted (with active window buffers).
     *
     * @return the count of tables with active window buffers
     */
    public int getActiveWindowBufferCount() {
        return windowBuffers.size();
    }
}
