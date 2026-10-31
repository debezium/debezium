/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.heartbeat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.heartbeat.Heartbeat.ScheduledHeartbeat;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.spi.OffsetContext;

/**
 * Unit tests for {@link DatabaseHeartbeatImpl}.
 */
class DatabaseHeartbeatImplTest {

    private static final String ACTION_QUERY = "SELECT 1";
    private static final Map<String, Object> PARTITION = Map.of("server", "test");
    private static final int CONCURRENT_ITERATIONS = 10;
    private static final long EXECUTION_HOLD_MILLIS = 20;
    private static final long EXECUTION_TIMEOUT_MILLIS = 500;

    @Test
    @FixFor("debezium/dbz#1328")
    void shouldRunTheActionQueryOncePerInterval() throws Exception {
        final JdbcConnection connection = mock(JdbcConnection.class);
        final AtomicInteger executions = new AtomicInteger();
        when(connection.execute(ACTION_QUERY)).thenAnswer(invocation -> {
            executions.incrementAndGet();
            return connection;
        });

        final var heartbeat = new CompositeHeartbeat(expiredUntilReset(), List.of(databaseHeartbeat(connection)));
        final OffsetContext offset = offsetContext();

        heartbeat.emitWithDelay(PARTITION, offset);

        assertThat(executions.get()).isEqualTo(1);

        // The interval has not elapsed yet, so the action query must not be executed again.
        heartbeat.emitWithDelay(PARTITION, offset);

        assertThat(executions.get()).isEqualTo(1);
    }

    @Test
    @FixFor("debezium/dbz#1328")
    void shouldNotRunTheActionQueryConcurrently() throws Exception {
        final JdbcConnection connection = mock(JdbcConnection.class);
        final AtomicInteger inFlight = new AtomicInteger();
        final AtomicInteger maxInFlight = new AtomicInteger();
        // The executions wait for each other and hold the connection, so that an overlap is certain
        // rather than lucky.
        final CountDownLatch bothExecuting = new CountDownLatch(2);
        when(connection.execute(ACTION_QUERY)).thenAnswer(invocation -> {
            final int concurrent = inFlight.incrementAndGet();
            maxInFlight.accumulateAndGet(concurrent, Math::max);
            try {
                bothExecuting.countDown();
                bothExecuting.await(EXECUTION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                Thread.sleep(EXECUTION_HOLD_MILLIS);
            }
            finally {
                inFlight.decrementAndGet();
            }
            return connection;
        });

        // The interval is always elapsed, so both dispatch paths execute the action query every time.
        final var heartbeat = new CompositeHeartbeat(alwaysExpired(), List.of(databaseHeartbeat(connection)));
        final OffsetContext offset = offsetContext();
        final CyclicBarrier start = new CyclicBarrier(2);
        final ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            final Future<?> guarded = executor.submit(() -> {
                for (int i = 0; i < CONCURRENT_ITERATIONS; i++) {
                    start.await();
                    heartbeat.emitWithDelay(PARTITION, offset);
                }
                return null;
            });
            final Future<?> unconditional = executor.submit(() -> {
                for (int i = 0; i < CONCURRENT_ITERATIONS; i++) {
                    start.await();
                    heartbeat.emit(PARTITION, offset);
                }
                return null;
            });

            guarded.get(1, TimeUnit.MINUTES);
            unconditional.get(1, TimeUnit.MINUTES);
        }
        finally {
            executor.shutdownNow();
        }

        assertThat(maxInFlight.get()).isEqualTo(1);
    }

    @Test
    @FixFor("debezium/dbz#1328")
    void shouldRunTheActionQueryForTheUnconditionalEmit() throws Exception {
        final JdbcConnection connection = mock(JdbcConnection.class);
        final AtomicInteger executions = new AtomicInteger();
        when(connection.execute(ACTION_QUERY)).thenAnswer(invocation -> {
            executions.incrementAndGet();
            return connection;
        });

        final var heartbeat = new CompositeHeartbeat(alwaysExpired(), List.of(databaseHeartbeat(connection)));
        final OffsetContext offset = offsetContext();

        heartbeat.emit(PARTITION, offset);

        assertThat(executions.get()).isEqualTo(1);
    }

    private DatabaseHeartbeatImpl databaseHeartbeat(JdbcConnection connection) {
        return new DatabaseHeartbeatImpl(connection, ACTION_QUERY, null);
    }

    private OffsetContext offsetContext() {
        final OffsetContext offset = mock(OffsetContext.class);
        doReturn(Map.of("position", 1)).when(offset).getOffset();
        return offset;
    }

    private ScheduledHeartbeat expiredUntilReset() {
        return new ScheduledHeartbeat() {
            private boolean expired = true;

            @Override
            public boolean expired() {
                return expired;
            }

            @Override
            public void reset() {
                expired = false;
            }

            @Override
            public boolean isEnabled() {
                return true;
            }
        };
    }

    private ScheduledHeartbeat alwaysExpired() {
        return new ScheduledHeartbeat() {
            @Override
            public boolean expired() {
                return true;
            }

            @Override
            public boolean isEnabled() {
                return true;
            }
        };
    }
}
