/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Clock;
import io.debezium.util.RetryExecutor;

public class SnapshotConnectionPoolTest {

    private final List<JdbcConnection> created = new ArrayList<>();
    private final AtomicBoolean idle = new AtomicBoolean(true);
    private SnapshotConnectionPool pool;

    @AfterEach
    public void closePool() {
        if (pool != null) {
            pool.close();
        }
    }

    private SnapshotConnectionPool pool(int size, long releaseDelayMs) {
        pool = new SnapshotConnectionPool(size, this::createConnection, idle::get, "test", releaseDelayMs,
                new RetryExecutor(1, 1L, 1L, 1.0, Clock.SYSTEM));
        return pool;
    }

    private JdbcConnection createConnection() throws SQLException {
        final JdbcConnection connection = mock(JdbcConnection.class);
        when(connection.isValid()).thenReturn(true);
        created.add(connection);
        return connection;
    }

    @Test
    public void shouldOpenOneConnectionPerSlotOnlyOnce() throws Exception {
        final SnapshotConnectionPool pool = pool(3, 0);

        pool.open();
        pool.open();

        assertThat(created).hasSize(3);
        assertThat(pool.connectionCount()).isEqualTo(3);
        assertThat(pool.state()).isEqualTo(SnapshotConnectionPool.State.OPEN);
    }

    @Test
    public void shouldHandOutAndTakeBackConnections() throws Exception {
        final SnapshotConnectionPool pool = pool(1, 0);
        pool.open();

        final JdbcConnection borrowed = pool.borrow();
        assertThat(borrowed).isSameAs(created.get(0));
        assertThat(pool.borrow()).as("the only connection is out").isNull();

        pool.giveBack(borrowed);
        assertThat(pool.borrow()).isSameAs(borrowed);
    }

    @Test
    public void shouldReplaceAStaleConnectionOnBorrow() throws Exception {
        final SnapshotConnectionPool pool = pool(1, 0);
        pool.open();
        final JdbcConnection stale = created.get(0);
        when(stale.isValid()).thenReturn(false);

        final JdbcConnection borrowed = pool.borrow();

        assertThat(borrowed).isNotSameAs(stale);
        assertThat(created).hasSize(2);
        verify(stale).close();
        assertThat(pool.connectionCount()).isEqualTo(1);
    }

    @Test
    public void shouldCloseAndReplaceADiscardedConnection() throws Exception {
        final SnapshotConnectionPool pool = pool(1, 0);
        pool.open();
        final JdbcConnection broken = pool.borrow();

        pool.discard(broken);

        verify(broken).close();
        assertThat(pool.connectionCount()).isEqualTo(1);
        assertThat(pool.borrow()).isNotSameAs(broken);
    }

    @Test
    public void shouldReleaseImmediatelyWhenIdleAndNoDelayIsConfigured() throws Exception {
        final SnapshotConnectionPool pool = pool(2, 0);
        pool.open();

        pool.releaseWhenIdle();

        assertThat(pool.state()).isEqualTo(SnapshotConnectionPool.State.CLOSED);
        assertThat(pool.connectionCount()).isZero();
        for (JdbcConnection connection : created) {
            verify(connection).close();
        }
    }

    @Test
    public void shouldKeepTheConnectionsWhileTheOwnerIsNotIdle() throws Exception {
        final SnapshotConnectionPool pool = pool(1, 0);
        pool.open();
        idle.set(false);

        pool.releaseWhenIdle();

        assertThat(pool.state()).isEqualTo(SnapshotConnectionPool.State.OPEN);
        verify(created.get(0), never()).close();
    }

    @Test
    public void shouldReleaseAfterTheDelayUnlessWorkArrives() throws Exception {
        final SnapshotConnectionPool pool = pool(1, 50);
        pool.open();

        pool.releaseWhenIdle();
        pool.open();
        Thread.sleep(150);
        assertThat(pool.state()).as("the open cancelled the pending release").isEqualTo(SnapshotConnectionPool.State.OPEN);

        pool.releaseWhenIdle();
        Awaitility.await().until(() -> pool.state() == SnapshotConnectionPool.State.CLOSED);
        assertThat(pool.connectionCount()).isZero();
    }

    @Test
    public void shouldCloseEverythingOnCloseAndRefuseLateReturns() throws Exception {
        final SnapshotConnectionPool pool = pool(2, 0);
        pool.open();
        final JdbcConnection borrowed = pool.borrow();

        pool.close();
        pool.giveBack(borrowed);

        for (JdbcConnection connection : created) {
            verify(connection, atLeastOnce()).close();
        }
        assertThat(pool.borrow()).isNull();
        assertThat(pool.state()).isEqualTo(SnapshotConnectionPool.State.CLOSED);
    }

    @Test
    public void shouldCloseThePartiallyOpenedConnectionsWhenOpeningFails() throws Exception {
        final AtomicBoolean failNext = new AtomicBoolean(false);
        pool = new SnapshotConnectionPool(2, () -> {
            if (failNext.getAndSet(true)) {
                throw new SQLException("second connection refused");
            }
            return createConnection();
        }, idle::get, "test", 0, new RetryExecutor(1, 1L, 1L, 1.0, Clock.SYSTEM));

        assertThatThrownBy(pool::open).isInstanceOf(SQLException.class);

        assertThat(created).hasSize(1);
        verify(created.get(0)).close();
        assertThat(pool.state()).isEqualTo(SnapshotConnectionPool.State.CLOSED);
        assertThat(pool.connectionCount()).isZero();
    }
}
