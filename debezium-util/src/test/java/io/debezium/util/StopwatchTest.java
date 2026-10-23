/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

public class StopwatchTest {

    @FixFor("DBZ-7436")
    @Test
    public void whenCallingDurationsOnAStartedStopwatchItShouldNotFailWithANPE() {
        Locale.setDefault(new Locale("en", "US"));
        Stopwatch reusable = Stopwatch.reusable();

        reusable.start();

        String reusableStopWatchResults = reusable.durations().toString();

        Stopwatch.StopwatchSet multiple = Stopwatch.multiple();

        Stopwatch stopwatch = multiple.create();

        String multipleStopWatchResults = stopwatch.durations().toString();

        Stopwatch accumulating = Stopwatch.accumulating();

        String accumulatingStopWatchResults = accumulating.durations().toString();

        assertThat(reusableStopWatchResults).isEqualTo("  0.00000s total;   0 samples;  0.00000s avg;  0.00000s min;  0.00000s max");
        assertThat(multipleStopWatchResults).isEqualTo("  0.00000s total;   0 samples;  0.00000s avg;  0.00000s min;  0.00000s max");
        assertThat(accumulatingStopWatchResults).isEqualTo("  0.00000s total;   0 samples;  0.00000s avg;  0.00000s min;  0.00000s max");
    }

    @Test
    @FixFor("debezium/dbz#2887")
    public void shouldExecuteStartCallbackOnlyOnceUnderConcurrentStarts() throws Exception {
        final int threadCount = 20;
        final int iterations = 100;
        final ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        try {
            for (int i = 0; i < iterations; i++) {
                final AtomicInteger startCount = new AtomicInteger();
                final Stopwatch stopwatch = Stopwatch.createWith(new TestDurations(), startCount::incrementAndGet, null);
                final CyclicBarrier barrier = new CyclicBarrier(threadCount);
                final Future<?>[] futures = new Future<?>[threadCount];

                for (int t = 0; t < threadCount; t++) {
                    futures[t] = executor.submit(() -> {
                        try {
                            barrier.await();
                            stopwatch.start();
                        }
                        catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                }

                for (Future<?> future : futures) {
                    future.get(10, TimeUnit.SECONDS);
                }

                assertThat(startCount.get()).isEqualTo(1);
            }
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    @FixFor("debezium/dbz#2887")
    public void shouldExecuteStopCallbackAndRecordDurationOnlyOnceUnderConcurrentStops() throws Exception {
        final int threadCount = 20;
        final int iterations = 100;
        final ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        try {
            for (int i = 0; i < iterations; i++) {
                final AtomicInteger stopCount = new AtomicInteger();
                final TestDurations durations = new TestDurations();
                final Stopwatch stopwatch = Stopwatch.createWith(durations, null, stopCount::incrementAndGet);
                stopwatch.start();

                final CyclicBarrier barrier = new CyclicBarrier(threadCount);
                final Future<?>[] futures = new Future<?>[threadCount];

                for (int t = 0; t < threadCount; t++) {
                    futures[t] = executor.submit(() -> {
                        try {
                            barrier.await();
                            stopwatch.stop();
                        }
                        catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                }

                for (Future<?> future : futures) {
                    future.get(10, TimeUnit.SECONDS);
                }

                assertThat(stopCount.get()).isEqualTo(1);
                assertThat(durations.count.get()).isEqualTo(1);
            }
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    @FixFor("debezium/dbz#2887")
    public void shouldNotDeadlockMultipleStopwatchAwaitUnderConcurrentStarts() throws Exception {
        final int threadCount = 20;
        final int iterations = 50;
        final ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        try {
            for (int i = 0; i < iterations; i++) {
                final Stopwatch.StopwatchSet set = Stopwatch.multiple();
                final Stopwatch stopwatch = set.create();
                final CyclicBarrier barrier = new CyclicBarrier(threadCount);
                final Future<?>[] futures = new Future<?>[threadCount];

                for (int t = 0; t < threadCount; t++) {
                    futures[t] = executor.submit(() -> {
                        try {
                            barrier.await();
                            stopwatch.start();
                        }
                        catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                }

                for (Future<?> future : futures) {
                    future.get(10, TimeUnit.SECONDS);
                }

                stopwatch.stop();

                final Future<?> awaitFuture = executor.submit(() -> {
                    try {
                        set.await();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                });

                awaitFuture.get(5, TimeUnit.SECONDS);
            }
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    @FixFor("debezium/dbz#2887")
    public void shouldNotDeadlockMultipleStopwatchAwaitUnderConcurrentStartAndStop() throws Exception {
        final int iterations = 10_000;
        final ExecutorService executor = Executors.newFixedThreadPool(3);

        try {
            for (int i = 0; i < iterations; i++) {
                final Stopwatch.StopwatchSet set = Stopwatch.multiple();
                final Stopwatch stopwatch = set.create();
                final CyclicBarrier barrier = new CyclicBarrier(2);

                final Future<?> starter = executor.submit(() -> {
                    barrier.await();
                    return stopwatch.start();
                });
                final Future<?> stopper = executor.submit(() -> {
                    barrier.await();
                    return stopwatch.stop();
                });
                starter.get(10, TimeUnit.SECONDS);
                stopper.get(10, TimeUnit.SECONDS);

                // The stopwatch may still be running if stop() won the race, so stop it
                stopwatch.stop();

                final Future<?> awaitFuture = executor.submit(() -> {
                    set.await();
                    return null;
                });
                awaitFuture.get(5, TimeUnit.SECONDS);
                assertThat(set.statistics().getCount()).isEqualTo(1);
            }
        }
        finally {
            executor.shutdownNow();
        }
    }

    private static final class TestDurations extends Stopwatch.BaseDurations {
        private final AtomicInteger count = new AtomicInteger();

        @Override
        public void add(Duration duration) {
            count.incrementAndGet();
        }

        @Override
        public Stopwatch.Statistics statistics() {
            return null;
        }
    }
}
