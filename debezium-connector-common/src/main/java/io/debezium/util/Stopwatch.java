/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.text.DecimalFormat;
import java.time.Duration;
import java.util.LongSummaryStatistics;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import io.debezium.annotation.ThreadSafe;

/**
 * A stopwatch for measuring durations. All NewStopwatch implementations are threadsafe, although using a single stopwatch
 * object across threads requires caution and care.
 *
 * @author Randall Hauch
 */
@ThreadSafe
public abstract class Stopwatch {

    /**
     * Start the stopwatch. Calling this method on an already-started stopwatch has no effect.
     *
     * @return this object to enable chaining methods
     * @see #stop
     */
    public abstract Stopwatch start();

    /**
     * Stop the stopwatch. Calling this method on an already-stopped stopwatch has no effect.
     *
     * @return this object to enable chaining methods
     * @see #start()
     */
    public abstract Stopwatch stop();

    /**
     * Get the total and average durations measured by this stopwatch.
     *
     * @return the durations; never null
     */
    public abstract Durations durations();

    /**
     * The average and total durations as measured by one or more stopwatches.
     */
    @ThreadSafe
    public interface Durations {

        /**
         * Get the statistics for the durations in nanoseconds.
         *
         * @return the statistics; never null
         */
        Statistics statistics();
    }

    /**
     * The timing statistics for a recorded set of samples.
     */
    public interface Statistics {
        /**
         * Returns the count of durations recorded.
         *
         * @return the count of durations
         */
        long getCount();

        /**
         * Returns the total of all recorded durations.
         *
         * @return The total duration; never null but possibly {@link Duration#ZERO}.
         */
        Duration getTotal();

        /**
         * Returns the minimum of all recorded durations.
         *
         * @return The minimum duration; never null but possibly {@link Duration#ZERO}.
         */
        Duration getMinimum();

        /**
         * Returns the maximum of all recorded durations.
         *
         * @return The maximum duration; never null but possibly {@link Duration#ZERO}.
         */
        Duration getMaximum();

        /**
         * Returns the arithmetic mean of all recorded durations.
         *
         * @return The average duration; never null but possibly {@link Duration#ZERO}.
         */
        Duration getAverage();

        /**
         * Returns a string representation of the total of all recorded durations.
         *
         * @return the string representation of the total duration; never null but possibly {@link Duration#ZERO}.
         */
        default String getTotalAsString() {
            return asString(getTotal());
        }

        /**
         * Returns a string representation of the minimum of all recorded durations.
         *
         * @return the string representation of the minimum duration; never null but possibly {@link Duration#ZERO}.
         */
        default String getMinimumAsString() {
            return asString(getMinimum());
        }

        /**
         * Returns a string representation of the maximum of all recorded durations.
         *
         * @return the string representation of the maximum duration; never null but possibly {@link Duration#ZERO}.
         */
        default String getMaximumAsString() {
            return asString(getMaximum());
        }

        /**
         * Returns a string representation of the arithmetic mean of all recorded durations.
         *
         * @return the string representation of the average duration; never null but possibly {@link Duration#ZERO}.
         */
        default String getAverageAsString() {
            return asString(getAverage());
        }
    }

    private static Statistics createStatistics(LongSummaryStatistics stats) {
        boolean some = stats.getCount() > 0L;
        return new Statistics() {
            @Override
            public long getCount() {
                return stats.getCount();
            }

            @Override
            public Duration getMaximum() {
                return some ? Duration.ofNanos(stats.getMax()) : Duration.ZERO;
            }

            @Override
            public Duration getMinimum() {
                return some ? Duration.ofNanos(stats.getMin()) : Duration.ZERO;
            }

            @Override
            public Duration getTotal() {
                return some ? Duration.ofNanos(stats.getSum()) : Duration.ZERO;
            }

            @Override
            public Duration getAverage() {
                return some ? Duration.ofNanos((long) stats.getAverage()) : Duration.ZERO;
            }

            private String fixedLengthSeconds(Duration duration) {
                double seconds = duration.toNanos() * 1e-9;
                String result = new DecimalFormat("##0.00000").format(seconds) + "s";
                if (result.length() == 8) {
                    return "  " + result;
                }
                if (result.length() == 9) {
                    return " " + result;
                }
                return result;
            }

            private String fixedLength(long count) {
                String result = new DecimalFormat("###0").format(count);
                if (result.length() == 1) {
                    return "   " + result;
                }
                if (result.length() == 2) {
                    return "  " + result;
                }
                if (result.length() == 3) {
                    return " " + result;
                }
                return result;
            }

            @Override
            public String toString() {
                StringBuilder sb = new StringBuilder();
                sb.append(fixedLengthSeconds(getTotal()) + " total;");
                sb.append(fixedLength(getCount()) + " samples;");
                sb.append(fixedLengthSeconds(getAverage()) + " avg;");
                sb.append(fixedLengthSeconds(getMinimum()) + " min;");
                sb.append(fixedLengthSeconds(getMaximum()) + " max");
                return sb.toString();
            }
        };
    }

    /**
     * Create a new {@link Stopwatch} that can be reused. The resulting {@link Stopwatch#durations()}, however,
     * only reflect the most recently completed stopwatch interval.
     * <p>
     * For example, the following code shows this behavior:
     *
     * <pre>
     * Stopwatch sw = Stopwatch.reusable();
     * sw.start();
     * sleep(3000); // sleep 3 seconds
     * sw.stop();
     * print(sw.durations()); // total and average duration are each 3 seconds
     *
     * sw.start();
     * sleep(2000); // sleep 2 seconds
     * sw.stop();
     * print(sw.durations()); // total and average duration are each 2 seconds
     * </pre>
     *
     * @return the new stopwatch; never null
     */
    public static Stopwatch reusable() {
        return createWith(new SingleDuration(), null, null);
    }

    /**
     * Create a new {@link Stopwatch} that records all of the measured durations of the stopwatch.
     * <p>
     * For example, the following code shows this behavior:
     *
     * <pre>
     * Stopwatch sw = Stopwatch.accumulating();
     * sw.start();
     * sleep(3000); // sleep 3 seconds
     * sw.stop();
     * print(sw.durations()); // total and average duration are each 3 seconds
     *
     * sw.start();
     * sleep(2000); // sleep 2 seconds
     * sw.stop();
     * print(sw.durations()); // total duration is now 5 seconds, average is 2.5 seconds
     * </pre>
     *
     * @return the new stopwatch; never null
     */
    public static Stopwatch accumulating() {
        return createWith(new MultipleDurations(), null, null);
    }

    /**
     * A set of stopwatches whose durations are combined. New stopwatches can be created at any time, and when
     * {@link Stopwatch#stop()} will always record their duration with this set.
     * <p>
     * This set is threadsafe, meaning that multiple threads can {@link #create()} new stopwatches concurrently, and each
     * stopwatch's duration is measured separately. Additionally, all of the other methods of this interface are also threadsafe.
     * </p>
     */
    @ThreadSafe
    public interface StopwatchSet extends Durations {
        /**
         * Create a new stopwatch that records durations with this set.
         *
         * @return the new stopwatch; never null
         */
        Stopwatch create();

        /**
         * Time the given function.
         *
         * @param runnable the function to call
         */
        default void time(Runnable runnable) {
            time(1, runnable);
        }

        /**
         * Time the given function.
         *
         * @param runnable the function that is to be executed; may not be null
         * @return the result of the operation
         */
        default <T> T time(Callable<T> runnable) {
            Stopwatch sw = create().start();
            try {
                return runnable.call();
            }
            catch (RuntimeException e) {
                throw e;
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            finally {
                sw.stop();
            }
        }

        /**
         * Time the given function multiple times.
         *
         * @param repeat the number of times to repeat the function call; must be positive
         * @param runnable the function to call; may not be null
         */
        default void time(int repeat, Runnable runnable) {
            for (int i = 0; i != repeat; ++i) {
                Stopwatch sw = create().start();
                try {
                    runnable.run();
                }
                finally {
                    sw.stop();
                }
            }
        }

        /**
         * Time the given function multiple times.
         *
         * @param repeat the number of times to repeat the function call; must be positive
         * @param runnable the function that is to be executed a number of times; may not be null
         * @param cleanup the function that is to be called after each time call to the runnable function, and not included
         *            in the time measurements; may be null
         * @throws Exception the exception thrown by the runnable function
         */
        default <T> void time(int repeat, Callable<T> runnable, Consumer<T> cleanup) throws Exception {
            for (int i = 0; i != repeat; ++i) {
                T result = null;
                Stopwatch sw = create().start();
                try {
                    result = runnable.call();
                }
                finally {
                    sw.stop();
                    if (cleanup != null) {
                        cleanup.accept(result);
                    }
                }
            }
        }

        /**
         * Block until all running stopwatches have been {@link Stopwatch#stop() stopped}. This means that if a stopwatch
         * is {@link #create() created} but never started, this method will not wait for it. Likewise, if a stopwatch
         * is {@link #create() created} and started, then this method will block until the stopwatch is {@link Stopwatch#stop()
         * stopped} (even if the same stopwatch is started multiple times).
         * are stopped.
         *
         * @throws InterruptedException if the thread is interrupted before unblocking
         */
        void await() throws InterruptedException;

        /**
         * Block until all stopwatches that have been {@link #create() created} and {@link Stopwatch#start() started} are
         * stopped.
         *
         * @param timeout the maximum length of time that this method should block
         * @param unit the unit for the timeout; may not be null
         * @throws InterruptedException if the thread is interrupted before unblocking
         */
        void await(long timeout, TimeUnit unit) throws InterruptedException;
    }

    /**
     * Create a new set of stopwatches. The resulting object is threadsafe, and each {@link Stopwatch} created by
     * {@link StopwatchSet#create()} is also threadsafe.
     *
     * @return the stopwatches set; never null
     */
    public static StopwatchSet multiple() {
        MultipleDurations durations = new MultipleDurations();
        VariableLatch latch = new VariableLatch(0);
        return new StopwatchSet() {
            @Override
            public Statistics statistics() {
                return durations.statistics();
            }

            @Override
            public Stopwatch create() {
                return createWith(durations, latch::countUp, latch::countDown);
            }

            @Override
            public void await() throws InterruptedException {
                latch.await();
            }

            @Override
            public void await(long timeout, TimeUnit unit) throws InterruptedException {
                latch.await(timeout, unit);
            }

            @Override
            public String toString() {
                return statistics().toString();
            }
        };
    }

    /**
     * Create a new stopwatch that updates the given {@link BaseDurations duration}, and optionally has functions to
     * be called after the stopwatch is started and stopped.
     * <p>
     * The resulting stopwatch is threadsafe.
     * </p>
     *
     * @param duration the duration that should be updated; may not be null
     * @param uponStart the function that should be called when the stopwatch is successfully started (after not running); may be
     *            null
     * @param uponStop the function that should be called when the stopwatch is successfully stopped (after it was running); may
     *            be null
     * @return the new stopwatch
     */
    protected static Stopwatch createWith(BaseDurations duration, Runnable uponStart, Runnable uponStop) {
        return new Stopwatch() {
            private final AtomicLong started = new AtomicLong(0L);

            @Override
            public Stopwatch start() {
                started.getAndUpdate(existing -> {
                    if (existing == 0L) {
                        // Has not yet been started ...
                        existing = System.nanoTime();
                        if (uponStart != null) {
                            uponStart.run();
                        }
                    }
                    return existing;
                });
                return this;
            }

            @Override
            public Stopwatch stop() {
                started.getAndUpdate(existing -> {
                    if (existing != 0L) {
                        // Is running but has not yet been stopped ...
                        duration.add(Duration.ofNanos(System.nanoTime() - existing));
                        if (uponStop != null) {
                            uponStop.run();
                        }
                        return 0L;
                    }
                    return existing;
                });
                return this;
            }

            @Override
            public Durations durations() {
                return duration;
            }

            @Override
            public String toString() {
                return durations().toString();
            }
        };
    }

    /**
     * Compute the readable string representation of the supplied duration.
     *
     * @param duration the duration; may not be null
     * @return the string representation; never null
     */
    protected static String asString(Duration duration) {
        return duration.toString().substring(2);
    }

    /**
     * Abstract base class for {@link Durations} implementations.
     */
    @ThreadSafe
    protected static abstract class BaseDurations implements Durations {
        public abstract void add(Duration duration);

        @Override
        public String toString() {
            return statistics().toString();
        }
    }

    /**
     * A {@link Durations} implementation that only remembers the most recently {@link #add(Duration) added} duration.
     */
    @ThreadSafe
    private static final class SingleDuration extends BaseDurations {
        private final AtomicReference<Statistics> stats = new AtomicReference<>();

        SingleDuration() {
            LongSummaryStatistics stats = new LongSummaryStatistics();
            this.stats.set(createStatistics(stats));
        }

        @Override
        public Statistics statistics() {
            return stats.get();
        }

        @Override
        public void add(Duration duration) {
            LongSummaryStatistics stats = new LongSummaryStatistics();
            if (duration != null) {
                stats.accept(duration.toNanos());
            }
            this.stats.set(createStatistics(stats));
        }
    }

    /**
     * A {@link Durations} implementation that accumulates all {@link #add(Duration) added} durations.
     */
    @ThreadSafe
    private static final class MultipleDurations extends BaseDurations {
        private final ConcurrentLinkedQueue<Duration> durations = new ConcurrentLinkedQueue<>();

        @Override
        public Statistics statistics() {
            return createStatistics(durations.stream().mapToLong(Duration::toNanos).summaryStatistics());
        }

        @Override
        public void add(Duration duration) {
            durations.add(duration);
        }
    }
}
