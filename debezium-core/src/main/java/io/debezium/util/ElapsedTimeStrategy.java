/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.function.BooleanSupplier;

/**
 * Encapsulates the logic of determining a delay when some criteria is met.
 *
 * @author Randall Hauch
 */
@FunctionalInterface
public interface ElapsedTimeStrategy {

    /**
     * Determine if the time period has elapsed since this method was last called.
     *
     * @return {@code true} if this invocation caused the thread to sleep, or {@code false} if this method did not sleep
     */
    boolean hasElapsed();

    /**
     * Create an elapsed time strategy that always is elapsed.
     *
     * @return the strategy; never null
     */
    public static ElapsedTimeStrategy none() {
        return () -> true;
    }

    /**
     * Create a strategy whose time periods are constant.
     *
     * @param clock the clock used to determine if sufficient time has elapsed; may not be null
     * @param delayInMilliseconds the time period; must be positive
     * @return the strategy; never null
     */
    public static ElapsedTimeStrategy constant(Clock clock, long delayInMilliseconds) {
        if (delayInMilliseconds <= 0) {
            throw new IllegalArgumentException("Initial delay must be positive");
        }
        return new ElapsedTimeStrategy() {
            private long nextTimestamp = 0L;

            @Override
            public boolean hasElapsed() {
                if (nextTimestamp == 0L) {
                    // Initialize ...
                    nextTimestamp = clock.currentTimeInMillis() + delayInMilliseconds;
                    return true;
                }
                long current = clock.currentTimeInMillis();
                if (current >= nextTimestamp) {
                    do {
                        long multiple = 1 + (current - nextTimestamp) / delayInMilliseconds;
                        nextTimestamp += multiple * delayInMilliseconds;
                    } while (current > nextTimestamp);
                    return true;
                }
                return false;
            }
        };
    }

    /**
     * Create a strategy whose time periods start out at one length but then change to another length after another
     * period has elapsed.
     *
     * @param clock the clock used to determine if sufficient time has elapsed; may not be null
     * @param preStepDelayInMilliseconds the time period before the step has occurred; must be positive
     * @param stepFunction the function that determines if the step time has elapsed; may not be null
     * @param postStepDelayInMilliseconds the time period before the step has occurred; must be positive
     * @return the strategy; never null
     */
    public static ElapsedTimeStrategy step(Clock clock,
                                           long preStepDelayInMilliseconds,
                                           BooleanSupplier stepFunction,
                                           long postStepDelayInMilliseconds) {
        if (preStepDelayInMilliseconds <= 0) {
            throw new IllegalArgumentException("Pre-step delay must be positive");
        }
        if (postStepDelayInMilliseconds <= 0) {
            throw new IllegalArgumentException("Post-step delay must be positive");
        }
        return new ElapsedTimeStrategy() {
            private long nextTimestamp = 0L;
            private boolean elapsed = false;
            private long delta = 0L;

            @Override
            public boolean hasElapsed() {
                if (nextTimestamp == 0L) {
                    // Initialize ...
                    elapsed = stepFunction.getAsBoolean();
                    delta = elapsed ? postStepDelayInMilliseconds : preStepDelayInMilliseconds;
                    nextTimestamp = clock.currentTimeInMillis() + delta;
                    return true;
                }
                if (!elapsed) {
                    elapsed = stepFunction.getAsBoolean();
                    if (elapsed) {
                        delta = postStepDelayInMilliseconds;
                    }
                }
                long current = clock.currentTimeInMillis();
                if (current >= nextTimestamp) {
                    do {
                        assert delta > 0;
                        long multiple = 1 + (current - nextTimestamp) / delta;
                        nextTimestamp += multiple * delta;
                    } while (nextTimestamp <= current);
                    return true;
                }
                return false;
            }
        };
    }

    /**
     * Create a strategy whose time periods linearly increase in length.
     *
     * @param clock the clock used to determine if sufficient time has elapsed; may not be null
     * @param delayInMilliseconds the initial delay; must be positive
     * @return the strategy; never null
     */
    public static ElapsedTimeStrategy linear(Clock clock, long delayInMilliseconds) {
        if (delayInMilliseconds <= 0) {
            throw new IllegalArgumentException("Initial delay must be positive");
        }
        return new ElapsedTimeStrategy() {
            private long nextTimestamp = 0L;
            private long counter = 1L;

            @Override
            public boolean hasElapsed() {
                if (nextTimestamp == 0L) {
                    // Initialize ...
                    nextTimestamp = clock.currentTimeInMillis() + delayInMilliseconds;
                    counter = 1L;
                    return true;
                }
                long current = clock.currentTimeInMillis();
                if (current >= nextTimestamp) {
                    do {
                        if (counter < Long.MAX_VALUE) {
                            ++counter;
                        }
                        nextTimestamp += (delayInMilliseconds * counter);
                    } while (nextTimestamp <= current);
                    return true;
                }
                return false;
            }
        };
    }

    /**
     * Create a strategy whose time periods increase exponentially.
     *
     * @param clock the clock used to determine if sufficient time has elapsed; may not be null
     * @param initialDelayInMilliseconds the initial delay; must be positive
     * @param maxDelayInMilliseconds the maximum delay; must be greater than the initial delay
     * @return the strategy; never null
     */
    public static ElapsedTimeStrategy exponential(Clock clock,
                                                  long initialDelayInMilliseconds,
                                                  long maxDelayInMilliseconds) {
        return exponential(clock, initialDelayInMilliseconds, maxDelayInMilliseconds, 2.0);
    }

    /**
     * Create a strategy whose time periods increase exponentially.
     *
     * @param clock the clock used to determine if sufficient time has elapsed; may not be null
     * @param initialDelayInMilliseconds the initial delay; must be positive
     * @param maxDelayInMilliseconds the maximum delay; must be greater than the initial delay
     * @param multiplier the factor by which the delay increases each pass
     * @return the strategy
     */
    public static ElapsedTimeStrategy exponential(Clock clock,
                                                  long initialDelayInMilliseconds,
                                                  long maxDelayInMilliseconds,
                                                  double multiplier) {
        if (multiplier <= 1.0) {
            throw new IllegalArgumentException("Multiplier must be greater than 1");
        }
        if (initialDelayInMilliseconds <= 0) {
            throw new IllegalArgumentException("Initial delay must be positive");
        }
        if (initialDelayInMilliseconds >= maxDelayInMilliseconds) {
            throw new IllegalArgumentException("Maximum delay must be greater than initial delay");
        }
        return new ElapsedTimeStrategy() {
            private long nextTimestamp = 0L;
            private long previousDelay = 0L;

            @Override
            public boolean hasElapsed() {
                if (nextTimestamp == 0L) {
                    // Initialize ...
                    nextTimestamp = clock.currentTimeInMillis() + initialDelayInMilliseconds;
                    previousDelay = initialDelayInMilliseconds;
                    return true;
                }
                long current = clock.currentTimeInMillis();
                if (current >= nextTimestamp) {
                    do {
                        // Compute how long to delay ...
                        long nextDelay = (long) (previousDelay * multiplier);
                        if (nextDelay >= maxDelayInMilliseconds) {
                            previousDelay = maxDelayInMilliseconds;
                            // If we're not there yet, then we know the increment is linear from here ...
                            if (nextTimestamp < current) {
                                long multiple = 1 + (current - nextTimestamp) / maxDelayInMilliseconds;
                                nextTimestamp += multiple * maxDelayInMilliseconds;
                            }
                        }
                        else {
                            previousDelay = nextDelay;
                        }
                        nextTimestamp += previousDelay;
                    } while (nextTimestamp <= current);
                    return true;
                }
                return false;
            }
        };
    }
}
