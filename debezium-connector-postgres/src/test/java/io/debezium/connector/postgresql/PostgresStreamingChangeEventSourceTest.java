/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.PostgresStreamingChangeEventSource.RecordPosition;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.ReplicationMessage.Operation;

public class PostgresStreamingChangeEventSourceTest {

    @AfterEach
    void clearInterruptFlag() {
        Thread.interrupted();
    }

    @Test
    void shouldClearTimeoutInterruptWhenWatchdogAlreadyTerminated() throws Exception {
        final Thread terminated = new Thread(() -> {
        });
        terminated.start();
        terminated.join();
        // The watchdog interrupted this thread before it terminated, so join() does not throw.
        Thread.currentThread().interrupt();

        PostgresStreamingChangeEventSource.stopWatchdog(terminated, () -> true);

        assertThat(Thread.currentThread().isInterrupted()).isFalse();
    }

    @Test
    void shouldWaitForPendingWatchdogBeforeClearingTimeoutInterrupt() throws Exception {
        final Thread caller = Thread.currentThread();
        final CountDownLatch fire = new CountDownLatch(1);
        // Models Threads.timeout(): it interrupts the caller after its loop, without checking for cancellation.
        final Thread pending = new Thread(() -> {
            awaitUninterruptibly(fire);
            caller.interrupt();
        });
        pending.start();
        // Releases the watchdog once the caller waits in join(). Falls back to a release after 2 s if the caller never waits.
        final Thread releaser = new Thread(() -> {
            try {
                Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> caller.getState() == Thread.State.WAITING);
            }
            catch (Exception e) {
                // release anyway
            }
            fire.countDown();
        });
        releaser.start();
        // The caller is interrupted while the watchdog is still pending, so the first join() throws.
        Thread.currentThread().interrupt();

        try {
            PostgresStreamingChangeEventSource.stopWatchdog(pending, () -> {
                assertThat(pending.isAlive()).as("Watchdog must terminate before the timeout interrupt is cleared").isFalse();
                return true;
            });

            assertThat(Thread.currentThread().isInterrupted()).isFalse();
        }
        finally {
            // Release and reap both threads even when the termination assertion fails.
            fire.countDown();
            awaitTermination(pending);
            awaitTermination(releaser);
        }
    }

    @Test
    void shouldKeepInterruptFromElsewhereWhenDrainHasNotTimedOut() throws Exception {
        final Thread terminated = new Thread(() -> {
        });
        terminated.start();
        terminated.join();
        Thread.currentThread().interrupt();

        PostgresStreamingChangeEventSource.stopWatchdog(terminated, () -> false);

        assertThat(Thread.currentThread().isInterrupted()).isTrue();
    }

    @Test
    void shouldTreatEqualPositionsAsAcknowledged() {
        assertThat(position(100L, 90L, 2, "INSERT").isAtOrAfter(position(100L, 90L, 2, "INSERT"))).isTrue();
    }

    @Test
    void shouldTreatLaterCommitLsnAsAcknowledged() {
        assertThat(position(200L, 150L, 5, "UPDATE").isAtOrAfter(position(100L, 90L, 2, "INSERT"))).isTrue();
    }

    @Test
    void shouldNotTreatEarlierCommitLsnAsAcknowledged() {
        assertThat(position(100L, 90L, 2, "INSERT").isAtOrAfter(position(200L, 150L, 5, "UPDATE"))).isFalse();
    }

    @Test
    void shouldCompareCommitLsnUnsigned() {
        final RecordPosition high = position(0x8000_0000_0000_0000L, 1L, 0, "INSERT");
        final RecordPosition low = position(1L, 1L, 0, "INSERT");
        assertThat(high.isAtOrAfter(low)).isTrue();
        assertThat(low.isAtOrAfter(high)).isFalse();
    }

    @Test
    void shouldHandleNullCommitLsn() {
        assertThat(position(null, 1L, 0, "INSERT").isAtOrAfter(position(100L, 1L, 0, "INSERT"))).isFalse();
        assertThat(position(100L, 1L, 0, "INSERT").isAtOrAfter(position(null, 1L, 0, "INSERT"))).isTrue();
        assertThat(position(null, 1L, 0, "INSERT").isAtOrAfter(position(null, 1L, 0, "INSERT"))).isTrue();
        assertThat(position(null, 1L, 0, "INSERT").isAtOrAfter(position(null, 2L, 0, "INSERT"))).isFalse();
    }

    @Test
    void shouldNotTreatSameCommitWithDifferentProgressAsAcknowledged() {
        assertThat(position(100L, 90L, 1, "INSERT").isAtOrAfter(position(100L, 90L, 2, "INSERT"))).isFalse();
        assertThat(position(100L, 90L, 1, "INSERT").isAtOrAfter(position(100L, 90L, 1, "UPDATE"))).isFalse();
    }

    @Test
    void shouldNormalizeNumericOffsetValues() {
        final Map<String, Object> asInteger = new HashMap<>();
        asInteger.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, 100);
        asInteger.put(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY, 90);
        final Map<String, Object> asLong = new HashMap<>();
        asLong.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, 100L);
        asLong.put(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY, 90L);

        assertThat(RecordPosition.of(asInteger)).isEqualTo(RecordPosition.of(asLong));
    }

    @Test
    void shouldRoundTripDataChangePosition() {
        final PostgresOffsetContext context = dataChangeContext();

        assertThat(context.lsnEventsProcessed()).isPositive();
        assertThat(RecordPosition.of(context)).isEqualTo(RecordPosition.of(context.getOffset()));
        assertThat(RecordPosition.of(context.getOffset()).eventsProcessed()).isPositive();
    }

    @Test
    void shouldRoundTripCommitPosition() {
        final PostgresOffsetContext context = dataChangeContext();
        context.updateCommitPosition(Lsn.valueOf(200L), Lsn.valueOf(200L));
        context.resetLsnEventsProcessed();

        final Map<String, ?> offset = context.getOffset();
        assertThat(offset).containsKey(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY);
        assertThat(offset).doesNotContainKey(PostgresOffsetContext.LSN_EVENTS_PROCESSED_KEY);
        assertThat(RecordPosition.of(context)).isEqualTo(RecordPosition.of(offset));
    }

    @Test
    void shouldRoundTripLoadedOffset() {
        final Map<String, ?> offset = dataChangeContext().getOffset();
        final PostgresOffsetContext loaded = new PostgresOffsetContext.Loader(connectorConfig()).load(offset);

        assertThat(RecordPosition.of(loaded)).isEqualTo(RecordPosition.of(offset));
    }

    private static RecordPosition position(Long commitLsn, Long processedLsn, long eventsProcessed, String messageType) {
        return new RecordPosition(commitLsn, processedLsn, eventsProcessed, messageType);
    }

    private static PostgresConnectorConfig connectorConfig() {
        return new PostgresConnectorConfig(TestHelper.defaultConfig().build());
    }

    private static PostgresOffsetContext dataChangeContext() {
        final Map<String, Object> initial = new HashMap<>();
        initial.put(SourceInfo.LSN_KEY, 100L);
        initial.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, 50L);
        initial.put(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY, 50L);
        final PostgresOffsetContext context = new PostgresOffsetContext.Loader(connectorConfig()).load(initial);
        // Two messages on the same LSN, as streaming does for a multi-event transaction.
        context.updateWalPosition(Lsn.valueOf(100L), Lsn.valueOf(50L), Instant.ofEpochSecond(1), 1L, null, null, Operation.UPDATE);
        context.incrementLsnEventsProcessed(Lsn.valueOf(100L));
        context.updateWalPosition(Lsn.valueOf(100L), Lsn.valueOf(50L), Instant.ofEpochSecond(1), 1L, null, null, Operation.UPDATE);
        context.incrementLsnEventsProcessed(Lsn.valueOf(100L));
        return context;
    }

    private static void awaitTermination(Thread thread) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.isAlive()) {
            final long remaining = deadline - System.nanoTime();
            assertThat(remaining).as("Test thread must terminate during cleanup").isPositive();
            try {
                TimeUnit.NANOSECONDS.timedJoin(thread, remaining);
            }
            catch (InterruptedException e) {
                // The simulated watchdog can still interrupt the caller while a broken implementation is being tested.
            }
        }
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        while (true) {
            try {
                latch.await();
                return;
            }
            catch (InterruptedException e) {
                // keep waiting
            }
        }
    }
}
