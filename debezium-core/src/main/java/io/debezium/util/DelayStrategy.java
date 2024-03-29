/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.time.Duration;
import java.util.function.BooleanSupplier;

/**
 * Encapsulates the logic of determining a delay when some criteria is met.
 *
 * @author Randall Hauch
 */
@FunctionalInterface
public interface DelayStrategy {

    /**
     * Attempt to sleep when the specified criteria is met.
     *
     * @param criteria {@code true} if this method should sleep, or {@code false} if there is no need to sleep
     * @return {@code true} if this invocation caused the thread to sleep, or {@code false} if this method did not sleep
     */
    default boolean sleepWhen(BooleanSupplier criteria) {
        return sleepWhen(criteria.getAsBoolean());
    }

    /**
     * Attempt to sleep when the specified criteria is met.
     *
     * @param criteria {@code true} if this method should sleep, or {@code false} if there is no need to sleep
     * @return {@code true} if this invocation caused the thread to sleep, or {@code false} if this method did not sleep
     */
    boolean sleepWhen(boolean criteria);

    /**
     * Create a delay strategy that never delays.
     *
     * @return the strategy; never null
     */
    static DelayStrategy none() {
        return (criteria) -> false;
    }

    /**
     * Create a delay strategy that applies a constant delay as long as the criteria is met. As soon as
     * the criteria is not met, the delay resets to zero.
     *
     * @param delay the initial delay; must be positive
     * @return the strategy; never null
     */
    static DelayStrategy constant(Duration delay) {
        long delayInMilliseconds = delay.toMillis();

        return (criteria) -> {
            if (!criteria) {
                return false;
            }
            try {
                Thread.sleep(delayInMilliseconds);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return true;
        };
    }

    /**
     * Create a delay strategy that applies an linearly-increasing delay as long as the criteria is met. As soon as
     * the criteria is not met, the delay resets to zero.
     *
     * @param delay the initial delay; must be positive
     * @return the strategy; never null
     */
    static DelayStrategy linear(Duration delay) {
        long delayInMilliseconds = delay.toMillis();
        if (delayInMilliseconds <= 0) {
            throw new IllegalArgumentException("Initial delay must be positive");
        }
        return new DelayStrategy() {
            private long misses = 0;

            @Override
            public boolean sleepWhen(boolean criteria) {
                if (!criteria) {
                    // Don't sleep ...
                    misses = 0;
                    return false;
                }
                // Compute how long to delay ...
                ++misses;
                try {
                    Thread.sleep(misses * delayInMilliseconds);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return true;
            }
        };
    }

    /**
     * Create a delay strategy that applies an exponentially-increasing delay as long as the criteria is met. As soon as
     * the criteria is not met, the delay resets to zero.
     *
     * @param initialDelay the initial delay; must be positive
     * @param maxDelay the maximum delay; must be greater than the initial delay
     * @return the strategy; never null
     */
    static DelayStrategy exponential(Duration initialDelay, Duration maxDelay) {
        return exponential(initialDelay, maxDelay, 2.0);
    }

    /**
     * Same as {@link #exponential(Duration, Duration, double, boolean)} with {@code bounded} parameter set to {@code false}
     */
    static DelayStrategy exponential(Duration initialDelay, Duration maxDelay, double backOffMultiplier) {
        return exponential(initialDelay, maxDelay, backOffMultiplier, false);
    }

    /**
     * Same as {@link #exponential(Duration, Duration, double, boolean)} with {@code bounded} parameter set to {@code true}
     */
    static DelayStrategy boundedExponential(Duration initialDelay, Duration maxDelay, double backOffMultiplier) {
        return exponential(initialDelay, maxDelay, backOffMultiplier, true);
    }

    /**
     * Create a delay strategy that applies an exponentially-increasing delay as long as the criteria is met. As soon as
     * the criteria is not met, the delay resets to the intial value.
     *
     * @param initialDelay the initial delay; must be positive
     * @param maxDelay the maximum delay; must be greater than the initial delay
     * @param backOffMultiplier the factor by which the delay increases each pass
     * @param bounded if true the delay resets also when maximum delay was reached
     * @return the strategy
     */
    static DelayStrategy exponential(Duration initialDelay, Duration maxDelay, double backOffMultiplier, boolean bounded) {
        final long initialDelayInMilliseconds = initialDelay.toMillis();
        final long maxDelayInMilliseconds = maxDelay.toMillis();
        if (backOffMultiplier <= 1.0) {
            throw new IllegalArgumentException("Backup multiplier must be greater than 1");
        }
        if (initialDelayInMilliseconds <= 0) {
            throw new IllegalArgumentException("Initial delay must be positive");
        }
        if (initialDelayInMilliseconds >= maxDelayInMilliseconds) {
            throw new IllegalArgumentException("Maximum delay must be greater than initial delay");
        }
        return new DelayStrategy() {
            private long previousDelay = 0;

            @Override
            public boolean sleepWhen(boolean criteria) {
                if (!criteria || maxDelayedReached()) {
                    // Don't sleep ...
                    previousDelay = 0;
                    return false;
                }
                // Compute how long to delay ...
                if (previousDelay == 0) {
                    previousDelay = initialDelayInMilliseconds;
                }
                else {
                    long nextDelay = (long) (previousDelay * backOffMultiplier);
                    previousDelay = Math.min(nextDelay, maxDelayInMilliseconds);
                }
                // We expect to sleep ...
                try {
                    Thread.sleep(previousDelay);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return true;
            }

            private boolean maxDelayedReached() {
                return bounded && previousDelay >= maxDelayInMilliseconds;
            }
        };
    }
}
