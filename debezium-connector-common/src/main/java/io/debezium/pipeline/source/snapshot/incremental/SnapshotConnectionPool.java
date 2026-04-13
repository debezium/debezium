/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.jdbc.JdbcConnection;

/**
 * Connection pool for parallel incremental snapshot processing.
 *
 * <p>This pool manages multiple JDBC connections in NORMAL mode (not replication mode)
 * used exclusively for parallel snapshot reads. Each worker thread gets its own
 * dedicated connection to avoid JDBC conflicts.
 *
 * <p><b>Important:</b> These connections are separate from the main WAL streaming
 * connection and do not interfere with replication slot or LSN position tracking.
 *
 * <p><b>Thread Safety:</b> This class is thread-safe using a BlockingQueue for
 * connection pooling.
 *
 * @author Debezium Community
 */
public class SnapshotConnectionPool {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotConnectionPool.class);

    private final int poolSize;
    private final BlockingQueue<JdbcConnection> availableConnections;
    private final List<JdbcConnection> allConnections;
    private final ConnectionFactory connectionFactory;
    private volatile boolean shutdown = false;

    /**
     * Factory interface for creating JDBC connections.
     */
    @FunctionalInterface
    public interface ConnectionFactory {
        /**
         * Creates a new JDBC connection in NORMAL mode (not replication).
         *
         * @return a new JDBC connection
         * @throws SQLException if connection creation fails
         */
        JdbcConnection createConnection() throws SQLException;
    }

    /**
     * Creates a new snapshot connection pool.
     *
     * @param poolSize the number of connections to maintain in the pool
     * @param connectionFactory factory to create new connections
     */
    public SnapshotConnectionPool(int poolSize, ConnectionFactory connectionFactory) {
        if (poolSize < 1) {
            throw new IllegalArgumentException("Pool size must be at least 1, got: " + poolSize);
        }

        this.poolSize = poolSize;
        this.connectionFactory = connectionFactory;
        this.availableConnections = new ArrayBlockingQueue<>(poolSize);
        this.allConnections = new ArrayList<>(poolSize);

        LOGGER.info("Initializing snapshot connection pool with {} connections", poolSize);
        initializeConnections();
    }

    /**
     * Initializes all connections in the pool.
     */
    private void initializeConnections() {
        for (int i = 0; i < poolSize; i++) {
            try {
                JdbcConnection connection = connectionFactory.createConnection();
                allConnections.add(connection);
                availableConnections.offer(connection);
                LOGGER.debug("Created snapshot connection {} of {}", i + 1, poolSize);
            }
            catch (SQLException e) {
                // Clean up any connections created so far
                shutdown();
                throw new DebeziumException("Failed to initialize snapshot connection pool", e);
            }
        }
        LOGGER.info("Snapshot connection pool initialized successfully with {} connections", poolSize);
    }

    /**
     * Acquires a connection from the pool.
     *
     * <p>This method blocks until a connection becomes available or the timeout expires.
     *
     * @param timeoutSeconds maximum time to wait for a connection
     * @return a JDBC connection from the pool
     * @throws InterruptedException if interrupted while waiting
     * @throws DebeziumException if timeout occurs or pool is shutdown
     */
    public JdbcConnection acquire(long timeoutSeconds) throws InterruptedException {
        if (shutdown) {
            throw new DebeziumException("Cannot acquire connection, pool has been shutdown");
        }

        String threadName = Thread.currentThread().getName();
        LOGGER.debug("[{}] Acquiring connection from pool (available: {})", threadName, availableConnections.size());

        JdbcConnection connection = availableConnections.poll(timeoutSeconds, TimeUnit.SECONDS);

        if (connection == null) {
            throw new DebeziumException(
                    String.format("Timeout waiting for snapshot connection after %d seconds (pool size: %d)",
                            timeoutSeconds, poolSize));
        }

        LOGGER.debug("[{}] Acquired connection from pool (remaining: {})", threadName, availableConnections.size());
        return connection;
    }

    /**
     * Releases a connection back to the pool.
     *
     * @param connection the connection to release
     */
    public void release(JdbcConnection connection) {
        if (connection == null) {
            return;
        }

        String threadName = Thread.currentThread().getName();

        if (shutdown) {
            LOGGER.debug("[{}] Pool is shutdown, closing connection instead of returning to pool", threadName);
            closeConnection(connection);
            return;
        }

        boolean returned = availableConnections.offer(connection);
        if (!returned) {
            LOGGER.warn("[{}] Failed to return connection to pool, closing it", threadName);
            closeConnection(connection);
        }
        else {
            LOGGER.debug("[{}] Released connection to pool (available: {})", threadName, availableConnections.size());
        }
    }

    /**
     * Shuts down the pool and closes all connections.
     */
    public void shutdown() {
        if (shutdown) {
            LOGGER.debug("Pool already shutdown, ignoring duplicate shutdown request");
            return;
        }

        LOGGER.info("Shutting down snapshot connection pool");
        shutdown = true;

        // Close all connections
        for (JdbcConnection connection : allConnections) {
            closeConnection(connection);
        }

        availableConnections.clear();
        allConnections.clear();

        LOGGER.info("Snapshot connection pool shutdown complete");
    }

    /**
     * Closes a single connection, handling errors gracefully.
     */
    private void closeConnection(JdbcConnection connection) {
        try {
            if (connection != null && connection.isConnected()) {
                connection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.warn("Error closing snapshot connection", e);
        }
    }

    /**
     * Returns the configured pool size.
     *
     * @return the pool size
     */
    public int getPoolSize() {
        return poolSize;
    }

    /**
     * Returns the number of currently available connections.
     *
     * @return the available connection count
     */
    public int getAvailableConnectionCount() {
        return availableConnections.size();
    }

    /**
     * Checks if the pool has been shutdown.
     *
     * @return true if shutdown, false otherwise
     */
    public boolean isShutdown() {
        return shutdown;
    }
}
