/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * A class that can be used to perform an action at a regular interval. To use, set up a {@code Metronome} instance and perform
 * a repeated event (perhaps using a loop), calling {@link #pause()} before each event.
 *
 * @author Randall Hauch
 */
@FunctionalInterface
public interface Metronome {

    /**
     * Pause until the next tick of the metronome.
     *
     * @throws InterruptedException if the thread was interrupted while pausing
     */
    public void pause() throws InterruptedException;

    /**
     * Create a new metronome that starts ticking immediately and that uses {@link Thread#sleep(long)} to wait.
     * <p>
     * Generally speaking, this is a simple but inaccurate approach for periods anywhere close to the precision of the supplied
     * Clock (which for the {@link Clock#system() system clock} is typically around 10-15 milliseconds for modern Linux and OS X
     * systems, and potentially worse on Windows and older Linux/Unix systems. And because this metronome uses
     * Thread#sleep(long), thread context switches are likely and will negatively affect the precision of the metronome's
     * period.
     * <p>
     * Although the method seemingly supports taking {@link TimeUnit#MICROSECONDS} and {@link TimeUnit#NANOSECONDS}, it is
     * likely that the JVM and operating system do not support such fine-grained precision. And as mentioned above, care should
     * be used when specifying a {@code period} of 20 milliseconds or smaller.
     *
     * @param period the period of time that the metronome ticks and for which {@link #pause()} waits
     * @param timeSystem the time system that will provide the current time; may not be null
     * @return the new metronome; never null
     */
    public static Metronome sleeper(Duration period, Clock timeSystem) {
        long periodInMillis = period.toMillis();
        return new Metronome() {
            private long next = timeSystem.currentTimeInMillis() + periodInMillis;

            @Override
            public void pause() throws InterruptedException {
                for (;;) {
                    final long now = timeSystem.currentTimeInMillis();
                    if (next <= now) {
                        break;
                    }
                    Thread.sleep(next - now);
                }
                next = next + periodInMillis;
            }

            @Override
            public String toString() {
                return "Metronome (sleep for " + periodInMillis + " ms)";
            }
        };
    }

    /**
     * Create a new metronome that starts ticking immediately and that uses {@link LockSupport#parkNanos(long)} to wait.
     * <p>
     * {@link LockSupport#parkNanos(long)} uses the underlying platform-specific timed wait mechanism, which may be more
     * accurate for smaller periods than {@link #sleeper(long, TimeUnit, Clock)}. However, like
     * {@link #sleeper(long, TimeUnit, Clock)}, the resulting Metronome may result in thread context switches.
     * <p>
     * Although the method seemingly supports taking {@link TimeUnit#MICROSECONDS} and {@link TimeUnit#NANOSECONDS}, it is
     * likely that the JVM and operating system do not support such fine-grained precision. And as mentioned above, care should
     * be used when specifying a {@code period} of 10-15 milliseconds or smaller.
     *
     * @param period the period of time that the metronome ticks and for which {@link #pause()} waits
     * @param timeSystem the time system that will provide the current time; may not be null
     * @return the new metronome; never null
     */
    public static Metronome parker(Duration period, Clock timeSystem) {
        long periodInNanos = period.toNanos();
        return new Metronome() {
            private long next = timeSystem.currentTimeInNanos() + periodInNanos;

            @Override
            public void pause() throws InterruptedException {
                while (next > timeSystem.currentTimeInNanos()) {
                    LockSupport.parkNanos(next - timeSystem.currentTimeInNanos());
                    if (Thread.currentThread().isInterrupted()) {
                        throw new InterruptedException();
                    }
                }
                next = next + periodInNanos;
            }

            @Override
            public String toString() {
                return "Metronome (park for " + TimeUnit.NANOSECONDS.toMillis(periodInNanos) + " ms)";
            }
        };
    }
}
