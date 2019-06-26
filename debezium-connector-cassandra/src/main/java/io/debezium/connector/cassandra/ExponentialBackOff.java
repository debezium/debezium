/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.util.Random;

/**
 * This class internally keeps track of the number of times backoff has been called, and
 * exponentially increase wait time each time doWait() is called. The wait time is calculated
 * as min(maximum_backoff_milliseconds, ((2^n)+random_number_milliseconds_less_than_1000)).
 * The random_number_milliseconds_less_than_1000 is used to avoid cases in which many
 * clients are synchronized by some situation and all retry at once, sending requests
 * in synchronized waves.
 */
public class ExponentialBackOff {
    private static final double DEFAULT_BASE = 2;
    private static final Random random = new Random();

    private final long maxBackOffSeconds;
    private final double base;

    private long currentBackOffMs;
    private double iteration;

    public ExponentialBackOff(long maxBackOffSeconds) {
        this(DEFAULT_BASE, maxBackOffSeconds);
    }

    public ExponentialBackOff(double base, long maxBackOffSeconds) {
        this.base = base;
        this.iteration = 0;
        this.maxBackOffSeconds = maxBackOffSeconds;
        this.currentBackOffMs = calculateBackoff();
    }

    public void doWait() throws InterruptedException {
        Thread.sleep(getAndIncrementBackOffMs());
    }

    public long getBackoffMs() {
        return currentBackOffMs;
    }

    public long getAndIncrementBackOffMs() {
        long temp = currentBackOffMs;
        iteration += 1;
        currentBackOffMs = calculateBackoff();
        return temp;
    }

    private long calculateBackoff() {
        if (currentBackOffMs >= maxBackOffSeconds) {
            return maxBackOffSeconds;
        }

        long sleepTimeMs = Math.round(Math.pow(base, iteration)) * 1000L + random.nextInt(1000);
        long maxSleepTimeMs = maxBackOffSeconds * 1000L;
        return Math.min(sleepTimeMs, maxSleepTimeMs);
    }
}
