/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

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
