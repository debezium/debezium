/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Utilities related to threads and threading.
 * 
 * @author Randall Hauch
 */
public class Threads {

    /**
     * Measures the amount time that has elapsed since the last {@link #reset() reset}.
     */
    public static interface TimeSince {
        /**
         * Reset the elapsed time to 0.
         */
        void reset();

        /**
         * Get the time that has elapsed since the last call to {@link #reset() reset}.
         * 
         * @return the number of milliseconds
         */
        long elapsedTime();
    }

    /**
     * Obtain a {@link TimeSince} that uses the given clock to record the time elapsed.
     * 
     * @param clock the clock; may not be null
     * @return the {@link TimeSince} object; never null
     */
    public static TimeSince timeSince(Clock clock) {
        return new TimeSince() {
            private long lastTimeInMillis;

            @Override
            public void reset() {
                lastTimeInMillis = clock.currentTimeInMillis();
            }

            @Override
            public long elapsedTime() {
                long elapsed = clock.currentTimeInMillis() - lastTimeInMillis;
                return elapsed <= 0L ? 0L : elapsed;
            }
        };
    }

    /**
     * Create a thread that will interrupt the calling thread when the {@link TimeSince elapsed time} has exceeded the
     * specified amount. The supplied {@link TimeSince} object will be {@link TimeSince#reset() reset} when the
     * new thread is started, and should also be {@link TimeSince#reset() reset} any time the elapsed time should be reset to 0.
     * 
     * @param threadName the name of the new thread; may not be null
     * @param timeout the maximum amount of time that can elapse before the thread is interrupted; must be positive
     * @param timeoutUnit the unit for {@code timeout}; may not be null
     * @param elapsedTimer the component used to measure the elapsed time; may not be null
     * @return the new thread that has not yet been {@link Thread#start() started}; never null
     */
    public static Thread interruptAfterTimeout(String threadName,
                                               long timeout, TimeUnit timeoutUnit,
                                               TimeSince elapsedTimer) {
        Thread threadToInterrupt = Thread.currentThread();
        return interruptAfterTimeout(threadName, timeout, timeoutUnit, elapsedTimer, threadToInterrupt);
    }

    /**
     * Create a thread that will interrupt the given thread when the {@link TimeSince elapsed time} has exceeded the
     * specified amount. The supplied {@link TimeSince} object will be {@link TimeSince#reset() reset} when the
     * new thread is started, and should also be {@link TimeSince#reset() reset} any time the elapsed time should be reset to 0.
     * 
     * @param threadName the name of the new thread; may not be null
     * @param timeout the maximum amount of time that can elapse before the thread is interrupted; must be positive
     * @param timeoutUnit the unit for {@code timeout}; may not be null
     * @param elapsedTimer the component used to measure the elapsed time; may not be null
     * @param threadToInterrupt the thread that should be interrupted upon timeout; may not be null
     * @return the new thread that has not yet been {@link Thread#start() started}; never null
     */
    public static Thread interruptAfterTimeout(String threadName,
                                               long timeout, TimeUnit timeoutUnit,
                                               TimeSince elapsedTimer, Thread threadToInterrupt) {
        return timeout(threadName, timeout, timeoutUnit, 100, TimeUnit.MILLISECONDS,
                       elapsedTimer::elapsedTime, elapsedTimer::reset,
                       () -> threadToInterrupt.interrupt());
    }

    /**
     * Create a thread that will call the supplied function when the {@link TimeSince elapsed time} has exceeded the
     * specified amount. The supplied {@link TimeSince} object will be {@link TimeSince#reset() reset} when the
     * new thread is started, and should also be {@link TimeSince#reset() reset} any time the elapsed time should be reset to 0.
     * <p>
     * The thread checks the elapsed time every 100 milliseconds.
     * 
     * @param threadName the name of the new thread; may not be null
     * @param timeout the maximum amount of time that can elapse before the thread is interrupted; must be positive
     * @param timeoutUnit the unit for {@code timeout}; may not be null
     * @param elapsedTimer the component used to measure the elapsed time; may not be null
     * @param uponTimeout the function to be called when the maximum amount of time has elapsed; may not be null
     * @return the new thread that has not yet been {@link Thread#start() started}; never null
     */
    public static Thread timeout(String threadName,
                                 long timeout, TimeUnit timeoutUnit,
                                 TimeSince elapsedTimer, Runnable uponTimeout) {
        return timeout(threadName, timeout, timeoutUnit, 100, TimeUnit.MILLISECONDS,
                       elapsedTimer::elapsedTime, elapsedTimer::reset,
                       uponTimeout);
    }

    /**
     * Create a thread that will call the supplied function when the {@link TimeSince elapsed time} has exceeded the
     * specified amount. The supplied {@link TimeSince} object will be {@link TimeSince#reset() reset} when the
     * new thread is started, and should also be {@link TimeSince#reset() reset} any time the elapsed time should be reset to 0.
     * <p>
     * The thread checks the elapsed time every 100 milliseconds.
     * 
     * @param threadName the name of the new thread; may not be null
     * @param timeout the maximum amount of time that can elapse before the thread is interrupted; must be positive
     * @param timeoutUnit the unit for {@code timeout}; may not be null
     * @param sleepInterval the amount of time for the new thread to sleep after checking the elapsed time; must be positive
     * @param sleepUnit the unit for {@code sleepInterval}; may not be null
     * @param elapsedTimer the component used to measure the elapsed time; may not be null
     * @param uponTimeout the function to be called when the maximum amount of time has elapsed; may not be null
     * @return the new thread that has not yet been {@link Thread#start() started}; never null
     */
    public static Thread timeout(String threadName,
                                 long timeout, TimeUnit timeoutUnit,
                                 long sleepInterval, TimeUnit sleepUnit,
                                 TimeSince elapsedTimer, Runnable uponTimeout) {
        return timeout(threadName, timeout, timeoutUnit, sleepInterval, sleepUnit,
                       elapsedTimer::elapsedTime, elapsedTimer::reset,
                       uponTimeout);
    }

    /**
     * Create a thread that will call the supplied function when the elapsed time has exceeded the
     * specified amount.
     * 
     * @param threadName the name of the new thread; may not be null
     * @param timeout the maximum amount of time that can elapse before the thread is interrupted; must be positive
     * @param timeoutUnit the unit for {@code timeout}; may not be null
     * @param sleepInterval the amount of time for the new thread to sleep after checking the elapsed time; must be positive
     * @param sleepUnit the unit for {@code sleepInterval}; may not be null
     * @param elapsedTime the function that returns the total elapsed time; may not be null
     * @param uponStart the function that will be called when the returned thread is {@link Thread#start() started}; may be null
     * @param uponTimeout the function to be called when the maximum amount of time has elapsed; may not be null
     * @return the new thread that has not yet been {@link Thread#start() started}; never null
     */
    public static Thread timeout(String threadName,
                                 long timeout, TimeUnit timeoutUnit,
                                 long sleepInterval, TimeUnit sleepUnit,
                                 LongSupplier elapsedTime,
                                 Runnable uponStart, Runnable uponTimeout) {
        final long timeoutInMillis = timeoutUnit.toMillis(timeout);
        final long sleepTimeInMillis = sleepUnit.toMillis(sleepInterval);
        Runnable r = () -> {
            if (uponStart != null) uponStart.run();
            while (elapsedTime.getAsLong() < timeoutInMillis) {
                try {
                    Thread.sleep(sleepTimeInMillis);
                } catch (InterruptedException e) {
                    // awoke from sleep
                    Thread.interrupted();
                    return;
                }
            }
            // Otherwise we've timed out ...
            uponTimeout.run();
        };
        return new Thread(r, threadName);
    }

    private Threads() {
    }

}
