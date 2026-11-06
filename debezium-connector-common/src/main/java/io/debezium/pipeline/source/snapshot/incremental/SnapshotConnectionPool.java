/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.sql.SQLException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.VisibleForTesting;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.RetryExecutor;
import io.debezium.util.Threads;

/**
 * Fixed-size pool of dedicated JDBC connections for the parallel incremental snapshot workers.
 *
 * <p>The pool is opened on demand and released once the owner reports that no work is left, after a configurable
 * delay so that back-to-back signals reuse the same connections. A release scheduled while new work arrives is
 * cancelled through a generation counter. Borrowed connections are validated and replaced when stale, and a
 * connection that failed with a non-transient error is discarded and replaced so the pool keeps its size.
 */
public class SnapshotConnectionPool implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotConnectionPool.class);
    private static final long SCHEDULER_TERMINATION_TIMEOUT_SECONDS = 10L;

    @FunctionalInterface
    public interface ConnectionFactory {
        JdbcConnection createConnection() throws SQLException;
    }

    enum State {
        CLOSED,
        OPEN
    }

    private final int size;
    private final ConnectionFactory connectionFactory;
    private final BooleanSupplier idle;
    private final long releaseDelayMs;
    private final RetryExecutor retryPolicy;
    private final Queue<JdbcConnection> available = new ConcurrentLinkedQueue<>();
    private final List<JdbcConnection> all = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService releaseScheduler;
    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final ReentrantLock lifecycleLock = new ReentrantLock();
    /**
     * Incremented on every open. A scheduled release captures the value at schedule time and bails out when it
     * finds a newer one, closing the race with an open that runs while the release task is already blocked on the
     * lifecycle lock.
     */
    private final AtomicLong releaseGeneration = new AtomicLong(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile ScheduledFuture<?> pendingRelease;

    /**
     * @param size number of connections to open
     * @param connectionFactory opens one dedicated connection
     * @param idle whether the owner has no work left, consulted when a release is due
     * @param connectorName logical name of the connector, used to name the release thread
     * @param releaseDelayMs how long to wait after the owner became idle before closing the connections; zero
     *            releases immediately
     * @param retryPolicy retries the opening of the connections on transient failures
     */
    public SnapshotConnectionPool(int size, ConnectionFactory connectionFactory, BooleanSupplier idle, String connectorName, long releaseDelayMs,
                                  RetryExecutor retryPolicy) {
        if (size < 1) {
            throw new IllegalArgumentException("Pool size must be at least 1, got: " + size);
        }
        if (releaseDelayMs < 0) {
            throw new IllegalArgumentException("Release delay must be non-negative, got: " + releaseDelayMs);
        }
        this.size = size;
        this.connectionFactory = connectionFactory;
        this.idle = idle;
        this.releaseDelayMs = releaseDelayMs;
        this.retryPolicy = retryPolicy;
        this.releaseScheduler = Threads.newSingleThreadScheduledExecutor(SnapshotConnectionPool.class, connectorName, "pool-release", true);
    }

    /**
     * Opens the connections unless the pool is already open, and cancels a pending release either way. Opening is
     * retried through the retry policy; the exception of the last attempt is rethrown.
     */
    public void open() throws SQLException {
        lifecycleLock.lock();
        try {
            releaseGeneration.incrementAndGet();
            cancelPendingReleaseLocked();
            if (state.get() == State.OPEN) {
                return;
            }
            retryPolicy.executeWithRetry(() -> {
                openConnections();
                return null;
            }, "open the parallel snapshot connection pool");
            state.set(State.OPEN);
            LOGGER.info("Parallel snapshot connection pool opened ({} connections)", size);
        }
        finally {
            lifecycleLock.unlock();
        }
    }

    private void openConnections() throws SQLException {
        try {
            for (int i = 0; i < size; i++) {
                final JdbcConnection connection = connectionFactory.createConnection();
                available.add(connection);
                all.add(connection);
            }
        }
        catch (SQLException | RuntimeException e) {
            closeAllQuietly();
            throw e;
        }
    }

    /**
     * Releases the connections once the owner is idle: immediately when the delay is zero, otherwise after the
     * delay, unless work arrives in the meantime.
     */
    public void releaseWhenIdle() {
        if (state.get() != State.OPEN) {
            return;
        }
        lifecycleLock.lock();
        try {
            if (state.get() != State.OPEN) {
                return;
            }
            if (releaseDelayMs == 0) {
                releaseIfIdle(releaseGeneration.get());
                return;
            }
            if (pendingRelease != null && !pendingRelease.isDone()) {
                return;
            }
            final long generation = releaseGeneration.get();
            pendingRelease = releaseScheduler.schedule(() -> releaseIfIdle(generation), releaseDelayMs, TimeUnit.MILLISECONDS);
        }
        finally {
            lifecycleLock.unlock();
        }
    }

    private void releaseIfIdle(long expectedGeneration) {
        lifecycleLock.lock();
        try {
            if (releaseGeneration.get() != expectedGeneration || state.get() != State.OPEN || !idle.getAsBoolean()) {
                return;
            }
            closeAllQuietly();
            state.set(State.CLOSED);
            LOGGER.info("Parallel snapshot connection pool released (no active or pending tables)");
        }
        finally {
            lifecycleLock.unlock();
        }
    }

    private void cancelPendingReleaseLocked() {
        if (pendingRelease != null) {
            pendingRelease.cancel(false);
            pendingRelease = null;
        }
    }

    /**
     * Returns a usable connection, or {@code null} when the pool is closed or currently empty. A stale connection
     * is evicted and replaced; the replacement is the only thing that can fail here.
     */
    public JdbcConnection borrow() throws SQLException {
        if (closed.get()) {
            return null;
        }
        final JdbcConnection connection = available.poll();
        if (connection == null) {
            return null;
        }
        try {
            if (connection.isValid()) {
                return connection;
            }
        }
        catch (SQLException e) {
            LOGGER.debug("Pooled snapshot connection failed validation, will be evicted", e);
        }
        all.remove(connection);
        closeQuietly(connection, "stale connection eviction");

        final JdbcConnection fresh = connectionFactory.createConnection();
        all.add(fresh);
        if (closed.get()) {
            all.remove(fresh);
            closeQuietly(fresh, "borrow during close");
            return null;
        }
        return fresh;
    }

    /**
     * Returns a borrowed connection to the pool, or closes it when the pool was closed in the meantime.
     */
    public void giveBack(JdbcConnection connection) {
        if (connection == null) {
            return;
        }
        if (closed.get()) {
            closeQuietly(connection, "return after close");
            return;
        }
        available.add(connection);
        if (closed.get() && available.remove(connection)) {
            closeQuietly(connection, "return during close");
        }
    }

    /**
     * Drops a connection that failed with a non-transient error and opens a replacement so the pool keeps its size.
     * If the replacement cannot be opened the pool shrinks by one; borrowers already cope with an empty pool.
     */
    public void discard(JdbcConnection connection) {
        if (connection == null) {
            return;
        }
        all.remove(connection);
        closeQuietly(connection, "non-transient connection failure");
        if (closed.get()) {
            return;
        }
        try {
            final JdbcConnection fresh = connectionFactory.createConnection();
            if (closed.get()) {
                closeQuietly(fresh, "replacement during close");
                return;
            }
            all.add(fresh);
            available.add(fresh);
        }
        catch (SQLException e) {
            LOGGER.warn("Failed to replace a discarded snapshot connection, the pool shrinks by one", e);
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        releaseScheduler.shutdownNow();
        lifecycleLock.lock();
        try {
            cancelPendingReleaseLocked();
            closeAllQuietly();
            state.set(State.CLOSED);
        }
        finally {
            lifecycleLock.unlock();
        }
        try {
            if (!releaseScheduler.awaitTermination(SCHEDULER_TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                LOGGER.warn("Pool release scheduler did not terminate within {} seconds", SCHEDULER_TERMINATION_TIMEOUT_SECONDS);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void closeAllQuietly() {
        for (JdbcConnection connection : all) {
            closeQuietly(connection, "pool release");
        }
        all.clear();
        available.clear();
    }

    private static void closeQuietly(JdbcConnection connection, String reason) {
        try {
            connection.close();
        }
        catch (SQLException e) {
            LOGGER.debug("Failed to close snapshot connection during {}", reason, e);
        }
    }

    @VisibleForTesting
    State state() {
        return state.get();
    }

    @VisibleForTesting
    int connectionCount() {
        return all.size();
    }
}
