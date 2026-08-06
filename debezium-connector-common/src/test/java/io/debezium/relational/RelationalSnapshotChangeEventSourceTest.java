/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.RelationalSnapshotChangeEventSource.PooledWork;

/**
 * Unit tests for {@link RelationalSnapshotChangeEventSource}.
 *
 * <p>These focus on the per-chunk/table retry loop in
 * {@link RelationalSnapshotChangeEventSource#createPooledResourceCallable} that backs the
 * {@code snapshot.errors.max.retries} option. Both {@code createDataEventsForTableCallable} (legacy full-table
 * snapshot) and {@code createDataEventsForChunkedTableCallable} (chunked snapshot) run their work through this same
 * method.
 *
 * @author Hagar Yasser
 */
class RelationalSnapshotChangeEventSourceTest {

    @Test
    void shouldSucceedWithoutRetryWhenWorkSucceedsFirstTime() throws Exception {
        final AtomicInteger attempts = new AtomicInteger();
        final PooledWork<OffsetContext> work = (connection, offset) -> attempts.incrementAndGet();

        callableFor(work, 3).call();

        assertThat(attempts.get()).isEqualTo(1);
    }

    @Test
    void shouldRetryFailingWorkThenSucceedWithinMaxRetries() throws Exception {
        final int failuresBeforeSuccess = 2;
        final AtomicInteger attempts = new AtomicInteger();
        final PooledWork<OffsetContext> work = (connection, offset) -> {
            if (attempts.incrementAndGet() <= failuresBeforeSuccess) {
                throw new RuntimeException("injected snapshot failure #" + attempts.get());
            }
        };

        // maxRetries (3) > failuresBeforeSuccess (2), so the chunk eventually succeeds
        callableFor(work, 3).call();

        // two failed attempts followed by a successful one
        assertThat(attempts.get()).isEqualTo(failuresBeforeSuccess + 1);
    }

    @Test
    void shouldSucceedWhenLastAllowedRetrySucceeds() throws Exception {
        final int maxRetries = 2;
        final AtomicInteger attempts = new AtomicInteger();
        // fails on the initial attempt and the first retry, succeeds on the second (last allowed) retry
        final PooledWork<OffsetContext> work = (connection, offset) -> {
            if (attempts.incrementAndGet() <= maxRetries) {
                throw new RuntimeException("injected snapshot failure #" + attempts.get());
            }
        };

        callableFor(work, maxRetries).call();

        assertThat(attempts.get()).isEqualTo(maxRetries + 1);
    }

    @Test
    void shouldFailAfterExhaustingMaxRetries() {
        final int maxRetries = 3;
        final AtomicInteger attempts = new AtomicInteger();
        final RuntimeException failure = new RuntimeException("always fails");
        final PooledWork<OffsetContext> work = (connection, offset) -> {
            attempts.incrementAndGet();
            throw failure;
        };

        // the original exception from the final attempt is rethrown
        assertThatThrownBy(() -> callableFor(work, maxRetries).call()).isSameAs(failure);

        // one initial attempt plus maxRetries retries
        assertThat(attempts.get()).isEqualTo(maxRetries + 1);
    }

    @Test
    void shouldNotRetryWhenRetriesDisabled() {
        final AtomicInteger attempts = new AtomicInteger();
        final PooledWork<OffsetContext> work = (connection, offset) -> {
            attempts.incrementAndGet();
            throw new RuntimeException("injected snapshot failure");
        };

        // snapshot.errors.max.retries defaults to 0 (disabled)
        assertThatThrownBy(() -> callableFor(work, 0).call()).isInstanceOf(RuntimeException.class);

        // only the initial attempt, no retries
        assertThat(attempts.get()).isEqualTo(1);
    }

    private Callable<Void> callableFor(PooledWork<OffsetContext> work, int maxRetries) throws SQLException {
        final Queue<JdbcConnection> connectionPool = new ConcurrentLinkedQueue<>();
        connectionPool.add(validConnection());
        final Queue<OffsetContext> offsetPool = new ConcurrentLinkedQueue<>();
        offsetPool.add(mock(OffsetContext.class));
        return source().createPooledResourceCallable(connectionPool, maxRetries, offsetPool, work, null);
    }

    private RelationalSnapshotChangeEventSource<Partition, OffsetContext> source() {
        return mock(RelationalSnapshotChangeEventSource.class, CALLS_REAL_METHODS);
    }

    private JdbcConnection validConnection() throws SQLException {
        final JdbcConnection connection = mock(JdbcConnection.class);
        // keep the connection valid so the retry loop re-runs the work in place rather than reconnecting
        when(connection.isValid()).thenReturn(true);
        return connection;
    }
}
