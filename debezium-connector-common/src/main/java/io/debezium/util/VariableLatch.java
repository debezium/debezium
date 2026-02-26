/*
 * Based on java.util.concurrent.CountDownLatch, which was
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 *
 * Any changes relative to CountDownLatch are also released to the
 * public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */
package io.debezium.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

import io.debezium.annotation.ThreadSafe;

/**
 * A latch that works similarly to {@link CountDownLatch} except that it can also increase the count dynamically.
 */
@ThreadSafe
public class VariableLatch {

    /**
     * Create a new variable latch.
     *
     * @return the variable latch; never null
     */
    public static VariableLatch create() {
        return create(0);
    }

    /**
     * Create a new variable latch.
     *
     * @param initialValue the initial number of latches
     * @return the variable latch; never null
     */
    public static VariableLatch create(int initialValue) {
        return new VariableLatch(initialValue);
    }

    /**
     * Synchronization control For CountDownLatch.
     * Uses AQS state to represent count.
     */
    private static final class Sync extends AbstractQueuedSynchronizer {
        private static final long serialVersionUID = 4982264981922014374L;

        Sync(int count) {
            setState(count);
        }

        int getCount() {
            return getState();
        }

        @Override
        protected int tryAcquireShared(int acquires) {
            return (getState() == 0) ? 1 : -1;
        }

        @Override
        protected boolean tryReleaseShared(int releases) {
            // Increment or decrement count; signal when transition to zero
            for (;;) {
                int c = getState();
                if (c == 0 && releases >= 0) {
                    return false;
                }
                int nextc = c - releases;
                if (nextc < 0) {
                    nextc = 0;
                }
                if (compareAndSetState(c, nextc)) {
                    return nextc == 0;
                }
            }
        }
    }

    private final Sync sync;

    /**
     * Constructs a {@code CountDownLatch} initialized with the given count.
     *
     * @param count the number of times {@link #countDown} must be invoked
     *            before threads can pass through {@link #await}
     * @throws IllegalArgumentException if {@code count} is negative
     */
    public VariableLatch(int count) {
        if (count < 0) {
            throw new IllegalArgumentException("count < 0");
        }
        this.sync = new Sync(count);
    }

    /**
     * Causes the current thread to wait until the latch has counted down to
     * zero, unless the thread is {@linkplain Thread#interrupt interrupted}.
     *
     * <p>
     * If the current count is zero then this method returns immediately.
     *
     * <p>
     * If the current count is greater than zero then the current thread becomes disabled for thread scheduling purposes and lies
     * dormant until one of two things happen:
     * <ul>
     * <li>The count reaches zero due to invocations of the {@link #countDown} method; or
     * <li>Some other thread {@linkplain Thread#interrupt interrupts} the current thread.
     * </ul>
     *
     * <p>
     * If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting,
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's interrupted status is cleared.
     *
     * @throws InterruptedException if the current thread is interrupted
     *             while waiting
     */
    public void await() throws InterruptedException {
        sync.acquireSharedInterruptibly(1);
    }

    /**
     * Causes the current thread to wait until the latch has counted down to
     * zero, unless the thread is {@linkplain Thread#interrupt interrupted},
     * or the specified waiting time elapses.
     *
     * <p>
     * If the current count is zero then this method returns immediately with the value {@code true}.
     *
     * <p>
     * If the current count is greater than zero then the current thread becomes disabled for thread scheduling purposes and lies
     * dormant until one of three things happen:
     * <ul>
     * <li>The count reaches zero due to invocations of the {@link #countDown} method; or
     * <li>Some other thread {@linkplain Thread#interrupt interrupts} the current thread; or
     * <li>The specified waiting time elapses.
     * </ul>
     *
     * <p>
     * If the count reaches zero then the method returns with the value {@code true}.
     *
     * <p>
     * If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting,
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's interrupted status is cleared.
     *
     * <p>
     * If the specified waiting time elapses then the value {@code false} is returned. If the time is less than or equal to zero,
     * the method will not wait at all.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the {@code timeout} argument
     * @return {@code true} if the count reached zero and {@code false} if the waiting time elapsed before the count reached zero
     * @throws InterruptedException if the current thread is interrupted
     *             while waiting
     */
    public boolean await(long timeout, TimeUnit unit)
            throws InterruptedException {
        return sync.tryAcquireSharedNanos(1, unit.toNanos(timeout));
    }

    /**
     * Decrements the count of the latch, releasing all waiting threads if the count reaches zero.
     *
     * <p>
     * If the current count is greater than zero then it is decremented. If the new count is zero then all waiting threads are
     * re-enabled for thread scheduling purposes.
     *
     * <p>
     * If the current count equals zero then nothing happens.
     */
    public void countDown() {
        sync.releaseShared(1);
    }

    /**
     * Decrements the count of the latch, releasing all waiting threads if the count reaches zero.
     *
     * <p>
     * If the current count is greater than zero then it is decremented. If the new count is zero then all waiting threads are
     * re-enabled for thread scheduling purposes.
     *
     * <p>
     * If the current count equals zero then nothing happens.
     *
     * @param count the number of counts to decrease
     */
    public void countDown(int count) {
        sync.releaseShared(1 * (Math.abs(count)));
    }

    /**
     * Increments the count of the latch by one.
     */
    public void countUp() {
        sync.releaseShared(-1);
    }

    /**
     * Increments the count of the latch by a positive number.
     *
     * @param count the number of counts to increase
     */
    public void countUp(int count) {
        sync.releaseShared(-1 * (Math.abs(count)));
    }

    /**
     * Returns the current count.
     *
     * <p>
     * This method is typically used for debugging and testing purposes.
     *
     * @return the current count
     */
    public long getCount() {
        return sync.getCount();
    }

    /**
     * Returns a string identifying this latch, as well as its state.
     * The state, in brackets, includes the String {@code "Count ="} followed by the current count.
     *
     * @return a string identifying this latch, as well as its state
     */
    @Override
    public String toString() {
        return super.toString() + "[Count = " + sync.getCount() + "]";
    }

}
