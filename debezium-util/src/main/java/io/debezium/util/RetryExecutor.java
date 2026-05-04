/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * General-purpose retry executor with exponential backoff and jitter.
 *
 * <p>Handles transient failures by retrying operations with configurable delays
 * and exponential backoff. Can be reused across Debezium components.
 *
 * <p><b>Features:</b>
 * <ul>
 *   <li>Configurable maximum retry attempts (or unlimited with -1)</li>
 *   <li>Exponential backoff with configurable multiplier</li>
 *   <li>Maximum delay cap to prevent indefinite waiting</li>
 *   <li>Jitter to prevent thundering herd problem</li>
 * </ul>
 *
 * @author Ivan Senyk
 */
public class RetryExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetryExecutor.class);
    private static final double JITTER_FACTOR = 0.25;

    private final int maxAttempts;
    private final long initialDelayMs;
    private final long maxDelayMs;
    private final double backoffMultiplier;
    private final Clock clock;

    /**
     * Creates a new retry executor with the specified configuration.
     *
     * @param maxAttempts Maximum number of retry attempts. Use -1 for unlimited retries.
     * @param initialDelayMs Initial delay in milliseconds before the first retry.
     * @param maxDelayMs Maximum delay in milliseconds between retries (cap for exponential backoff).
     * @param backoffMultiplier Multiplier for exponential backoff (typically 2.0 for doubling).
     * @param clock Clock for time operations and delays.
     */
    public RetryExecutor(
                         int maxAttempts,
                         long initialDelayMs,
                         long maxDelayMs,
                         double backoffMultiplier,
                         Clock clock) {
        this.maxAttempts = maxAttempts;
        this.initialDelayMs = initialDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.backoffMultiplier = backoffMultiplier;
        this.clock = clock;

        LOGGER.info("Retry executor initialized: maxAttempts={}, initialDelay={}ms, " +
                "maxDelay={}ms, backoffMultiplier={}",
                maxAttempts < 0 ? "unlimited" : maxAttempts,
                initialDelayMs, maxDelayMs, backoffMultiplier);
    }

    public <T> T executeWithRetry(RetryableOperation<T> operation, String operationName) throws SQLException {
        int attempt = 0;
        SQLException lastException = null;

        while (shouldRetry(attempt)) {
            try {
                if (attempt > 0) {
                    LOGGER.info("[{}] Retrying {} (attempt {} of {})",
                            Thread.currentThread().getName(), operationName, attempt + 1, formatMaxAttempts());
                }

                T result = operation.execute();

                if (attempt > 0) {
                    LOGGER.info("[{}] Successfully completed {} after {} retry attempt(s)",
                            Thread.currentThread().getName(), operationName, attempt);
                }

                return result;

            }
            catch (SQLException e) {
                lastException = e;
                attempt++;

                if (!shouldRetry(attempt)) {
                    LOGGER.error("[{}] Failed to execute {} after {} attempt(s). Giving up.",
                            Thread.currentThread().getName(), operationName, attempt, e);
                    throw e;
                }

                long delayMs = calculateDelay(attempt);
                LOGGER.warn("[{}] Database error during {}: {}. Will retry in {}ms (attempt {} of {})",
                        Thread.currentThread().getName(), operationName, e.getMessage(), delayMs, attempt, formatMaxAttempts());

                try {
                    waitWithBackoff(delayMs);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    LOGGER.error("[{}] Retry wait interrupted for {}", Thread.currentThread().getName(), operationName);
                    throw new SQLException("Retry interrupted", ie);
                }
            }
        }

        throw lastException != null ? lastException
                : new SQLException("Operation failed without exception: " + operationName);
    }

    public <T> T executeWithRetryInterruptible(
                                               RetryableOperationInterruptible<T> operation,
                                               String operationName)
            throws SQLException, InterruptedException {

        int attempt = 0;
        SQLException lastException = null;

        while (shouldRetry(attempt)) {
            try {
                if (attempt > 0) {
                    LOGGER.info("[{}] Retrying {} (attempt {} of {})",
                            Thread.currentThread().getName(), operationName, attempt + 1, formatMaxAttempts());
                }

                T result = operation.execute();

                if (attempt > 0) {
                    LOGGER.info("[{}] Successfully completed {} after {} retry attempt(s)",
                            Thread.currentThread().getName(), operationName, attempt);
                }

                return result;

            }
            catch (SQLException e) {
                lastException = e;
                attempt++;

                if (!shouldRetry(attempt)) {
                    LOGGER.error("[{}] Failed to execute {} after {} attempt(s). Giving up.",
                            Thread.currentThread().getName(), operationName, attempt, e);
                    throw e;
                }

                long delayMs = calculateDelay(attempt);
                LOGGER.warn("[{}] Database error during {}: {}. Will retry in {}ms (attempt {} of {})",
                        Thread.currentThread().getName(), operationName, e.getMessage(), delayMs, attempt, formatMaxAttempts());

                waitWithBackoff(delayMs);
            }
        }

        throw lastException != null ? lastException
                : new SQLException("Operation failed without exception: " + operationName);
    }

    private boolean shouldRetry(int attempt) {
        return maxAttempts < 0 || attempt < maxAttempts;
    }

    private long calculateDelay(int attempt) {
        if (attempt <= 0) {
            return 0;
        }

        double delay = initialDelayMs * Math.pow(backoffMultiplier, attempt - 1);
        delay = Math.min(delay, maxDelayMs);

        double jitter = delay * JITTER_FACTOR;
        delay = delay - jitter + (ThreadLocalRandom.current().nextDouble() * jitter * 2);

        return (long) delay;
    }

    private void waitWithBackoff(long delayMs) throws InterruptedException {
        Metronome metronome = Metronome.sleeper(Duration.ofMillis(delayMs), clock);
        metronome.pause();
    }

    private String formatMaxAttempts() {
        return maxAttempts < 0 ? "unlimited" : String.valueOf(maxAttempts);
    }

    @FunctionalInterface
    public interface RetryableOperation<T> {
        T execute() throws SQLException;
    }

    @FunctionalInterface
    public interface RetryableOperationInterruptible<T> {
        T execute() throws SQLException, InterruptedException;
    }
}
