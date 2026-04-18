/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * Retry policy for incremental snapshot operations with exponential backoff.
 *
 * <p>This policy handles transient database connection failures during incremental
 * snapshot operations by retrying failed operations with configurable delays and
 * exponential backoff.
 *
 * <p><b>Features:</b>
 * <ul>
 *   <li>Configurable maximum retry attempts (or unlimited with -1)</li>
 *   <li>Exponential backoff with configurable multiplier</li>
 *   <li>Maximum delay cap to prevent indefinite waiting</li>
 *   <li>Jitter to prevent thundering herd problem</li>
 * </ul>
 *
 * <p><b>Usage Example:</b>
 * <pre>
 * IncrementalSnapshotRetryPolicy retryPolicy = new IncrementalSnapshotRetryPolicy(
 *     5,      // max 5 attempts
 *     1000,   // start with 1 second delay
 *     60000,  // max 60 seconds delay
 *     2.0,    // double delay each retry
 *     clock
 * );
 *
 * Object result = retryPolicy.executeWithRetry(
 *     () -> performDatabaseOperation(),
 *     "database operation"
 * );
 * </pre>
 *
 * @author Ivan Senyk
 */
public class IncrementalSnapshotRetryPolicy {

    private static final Logger LOGGER = LoggerFactory.getLogger(IncrementalSnapshotRetryPolicy.class);
    private static final double JITTER_FACTOR = 0.25; // ±25% jitter

    private final int maxAttempts;
    private final long initialDelayMs;
    private final long maxDelayMs;
    private final double backoffMultiplier;
    private final Clock clock;

    /**
     * Creates a new retry policy with the specified configuration.
     *
     * @param maxAttempts Maximum number of retry attempts. Use -1 for unlimited retries.
     * @param initialDelayMs Initial delay in milliseconds before the first retry.
     * @param maxDelayMs Maximum delay in milliseconds between retries (cap for exponential backoff).
     * @param backoffMultiplier Multiplier for exponential backoff (typically 2.0 for doubling).
     * @param clock Clock for time operations and delays.
     */
    public IncrementalSnapshotRetryPolicy(
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

        LOGGER.info("Incremental snapshot retry policy initialized: maxAttempts={}, initialDelay={}ms, " +
                "maxDelay={}ms, backoffMultiplier={}",
                maxAttempts < 0 ? "unlimited" : maxAttempts,
                initialDelayMs, maxDelayMs, backoffMultiplier);
    }

    /**
     * Executes an operation with retry logic for SQLException.
     *
     * <p>The operation will be retried according to the configured retry policy
     * if it throws a SQLException. All other exceptions are propagated immediately.
     *
     * @param operation The operation to execute
     * @param operationName Name of the operation for logging purposes
     * @param <T> Return type of the operation
     * @return The result of the successful operation
     * @throws SQLException if all retry attempts are exhausted
     */
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

        // Should never reach here, but handle gracefully
        throw lastException != null ? lastException
                : new SQLException("Operation failed without exception: " + operationName);
    }

    /**
     * Executes an operation that may throw InterruptedException with retry logic.
     *
     * <p>Similar to {@link #executeWithRetry(RetryableOperation, String)} but supports
     * operations that throw InterruptedException.
     *
     * @param operation The operation to execute
     * @param operationName Name of the operation for logging purposes
     * @param <T> Return type of the operation
     * @return The result of the successful operation
     * @throws SQLException if all retry attempts are exhausted
     * @throws InterruptedException if the operation or retry wait is interrupted
     */
    public <T> T executeWithRetryInterruptible(
            RetryableOperationInterruptible<T> operation,
            String operationName) throws SQLException, InterruptedException {

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

    /**
     * Determines if another retry attempt should be made.
     *
     * @param attempt Current attempt number (0-based)
     * @return true if should retry, false otherwise
     */
    private boolean shouldRetry(int attempt) {
        // maxAttempts < 0 means unlimited retries
        return maxAttempts < 0 || attempt < maxAttempts;
    }

    /**
     * Calculates the delay before the next retry with exponential backoff and jitter.
     *
     * <p>The delay is calculated as:
     * <pre>
     * delay = initialDelay × (multiplier ^ (attempt - 1))
     * delay = min(delay, maxDelay)  // cap at maximum
     * delay = delay ± jitter         // add randomness
     * </pre>
     *
     * @param attempt Current attempt number (1-based)
     * @return Delay in milliseconds
     */
    private long calculateDelay(int attempt) {
        if (attempt <= 0) {
            return 0;
        }

        // Exponential backoff: initialDelay * (multiplier ^ (attempt - 1))
        double delay = initialDelayMs * Math.pow(backoffMultiplier, attempt - 1);

        // Cap at max delay
        delay = Math.min(delay, maxDelayMs);

        // Add jitter (±25%) to prevent thundering herd
        double jitter = delay * JITTER_FACTOR;
        delay = delay - jitter + (ThreadLocalRandom.current().nextDouble() * jitter * 2);

        return (long) delay;
    }

    /**
     * Waits for the specified delay with support for interruption.
     *
     * @param delayMs Delay in milliseconds
     * @throws InterruptedException if the wait is interrupted
     */
    private void waitWithBackoff(long delayMs) throws InterruptedException {
        Metronome metronome = Metronome.sleeper(Duration.ofMillis(delayMs), clock);
        metronome.pause();
    }

    /**
     * Formats the maximum attempts for logging.
     *
     * @return "unlimited" if maxAttempts < 0, otherwise the number as string
     */
    private String formatMaxAttempts() {
        return maxAttempts < 0 ? "unlimited" : String.valueOf(maxAttempts);
    }

    /**
     * Functional interface for retryable operations that may throw SQLException.
     *
     * @param <T> The return type of the operation
     */
    @FunctionalInterface
    public interface RetryableOperation<T> {
        /**
         * Executes the operation.
         *
         * @return The result of the operation
         * @throws SQLException if a database error occurs
         */
        T execute() throws SQLException;
    }

    /**
     * Functional interface for retryable operations that may throw SQLException or InterruptedException.
     *
     * @param <T> The return type of the operation
     */
    @FunctionalInterface
    public interface RetryableOperationInterruptible<T> {
        /**
         * Executes the operation.
         *
         * @return The result of the operation
         * @throws SQLException if a database error occurs
         * @throws InterruptedException if the operation is interrupted
         */
        T execute() throws SQLException, InterruptedException;
    }
}
