/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;

/**
 * Tests RetryingRunnable.
 * Refactored and inspired by: io.debezium.embedded.async.RetryingCallableTest
 */
public class RetryingRunnableTest {

    private final List<Class<? extends Exception>> retriableExceptions = List.of(SQLRecoverableException.class);
    private final AtomicInteger runs = new AtomicInteger();
    private final AtomicInteger heals = new AtomicInteger();

    @BeforeEach
    void init() {
        runs.set(0);
        heals.set(0);
    }

    @Test
    void shouldExecuteNeverFailing() throws InterruptedException, SQLException {
        getNeverFailing(0).run();
        Assertions.assertThat(runs.get()).isEqualTo(1);
        Assertions.assertThat(heals.get()).isEqualTo(0);
    }

    @Test
    void shouldNotRetryWhenRunnableDoesNotFail() throws InterruptedException, SQLException {
        getNeverFailing(10).run();
        Assertions.assertThat(runs.get()).isEqualTo(1);
        Assertions.assertThat(heals.get()).isEqualTo(0);
    }

    @Test
    void shouldIgnoreInfiniteRetryWhenRunnableDoesNotFail() throws InterruptedException, SQLException {
        getNeverFailing(-1).run();
        Assertions.assertThat(runs.get()).isEqualTo(1);
        Assertions.assertThat(heals.get()).isEqualTo(0);
    }

    @Test
    void shouldRetryAsManyTimesAsRequested() throws InterruptedException, SQLException {
        getTwoTimesFailing(10).run();

        // Runnable should fail 2 times and 3rd time it should succeed.
        assertThat(runs.get()).isEqualTo(3);
        Assertions.assertThat(heals.get()).isEqualTo(2);
    }

    @Test
    void shouldRetryAsManyTimesAsRequestedWhenAlwaysFails() throws InterruptedException {
        try {
            getAlwaysFailing(5).run();
        }
        catch (SQLException e) {
            // Good
        }

        // Should be called 6 times - 1 call + 5 retries.
        assertThat(runs.get()).isEqualTo(6);
        Assertions.assertThat(heals.get()).isEqualTo(5);
    }

    @Test
    void shouldNotRetryWhenRetriesAreDisabled() throws InterruptedException {
        try {
            getAlwaysFailing(0).run();
        }
        catch (SQLException e) {
            // Good
        }

        // Should be called only 1 time.
        assertThat(runs.get()).isEqualTo(1);
        Assertions.assertThat(heals.get()).isEqualTo(0);
    }

    @Test
    void shouldNotRetryAsManyTimesAsRequestedForNonMatchingSuppliedRetriableExceptions() throws InterruptedException {
        try {
            getAlwaysFailing(5, retriableExceptions).run();
        }
        catch (SQLException e) {
            // Good
        }

        // Should be called 1 time with no heals.
        assertThat(runs.get()).isEqualTo(1);
        Assertions.assertThat(heals.get()).isEqualTo(0);
    }

    @Test
    void shouldRetryAsManyTimesAsRequestedForMatchingSuppliedRetriableExceptions() throws InterruptedException, SQLException {
        getTwoTimesFailingButRetriable(10, retriableExceptions).run();

        // Runnable should fail 2 times and 3rd time it should succeed.
        assertThat(runs.get()).isEqualTo(3);
        Assertions.assertThat(heals.get()).isEqualTo(2);
    }

    @Test
    void shouldRetryAsManyTimesAsRequestedForMatchingSuppliedRetriableCause() throws InterruptedException, SQLException {
        getTwoTimesFailingWithRetriableCause(10, retriableExceptions).run();

        // Runnable should fail 2 times and 3rd time it should succeed.
        assertThat(runs.get()).isEqualTo(3);
        Assertions.assertThat(heals.get()).isEqualTo(2);
    }

    @Test
    void shouldRetryAsManyTimesAsRequestedWhenAlwaysFailsForMatchingSuppliedRetriableExceptions() throws InterruptedException {
        try {
            getAlwaysFailingButRetriable(5, retriableExceptions).run();
        }
        catch (SQLException e) {
            // Good
        }

        // Should be called 6 times - 1 call + 5 retries.
        assertThat(runs.get()).isEqualTo(6);
        Assertions.assertThat(heals.get()).isEqualTo(5);
    }

    private RetryingRunnable<SQLException> getNeverFailing(int retries) {
        return RetryingRunnable.<SQLException> builder()
                .retries(retries)
                .doRun(runs::incrementAndGet)
                .doAutoHeal(heals::incrementAndGet)
                .delayStrategy(DelayStrategy.linear(Duration.ofMillis(100)))
                .build();
    }

    private RetryingRunnable<SQLException> getAlwaysFailing(int retries) {
        return getAlwaysFailing(retries, null);
    }

    private RetryingRunnable<SQLException> getAlwaysFailing(int retries,
                                                            List<Class<? extends Exception>> retriableExceptions) {
        return RetryingRunnable.<SQLException> builder()
                .retries(retries)
                .doRun(() -> {
                    runs.incrementAndGet();
                    throw new SQLException("Good try, but I always fail");
                })
                .doAutoHeal(heals::incrementAndGet)
                .delayStrategy(DelayStrategy.linear(Duration.ofMillis(100)))
                .retriableExceptions(retriableExceptions)
                .build();
    }

    private RetryingRunnable<SQLException> getAlwaysFailingButRetriable(int retries,
                                                                        List<Class<? extends Exception>> retriableExceptions) {
        return RetryingRunnable.<SQLException> builder()
                .retries(retries)
                .doRun(() -> {
                    runs.incrementAndGet();
                    throw new SQLRecoverableException("Good try, but I always fail");
                })
                .doAutoHeal(heals::incrementAndGet)
                .delayStrategy(DelayStrategy.linear(Duration.ofMillis(100)))
                .retriableExceptions(retriableExceptions)
                .build();
    }

    private RetryingRunnable<SQLException> getTwoTimesFailing(int retries) {
        return getTwoTimesFailing(retries, null);
    }

    private RetryingRunnable<SQLException> getTwoTimesFailing(int retries,
                                                              List<Class<? extends Exception>> retriableExceptions) {
        return RetryingRunnable.<SQLException> builder()
                .retries(retries)
                .doRun(() -> {
                    if (runs.incrementAndGet() <= 2) {
                        throw new SQLException(String.format("Good try, but I fail this time (call #%s)", runs));
                    }
                })
                .doAutoHeal(heals::incrementAndGet)
                .delayStrategy(DelayStrategy.linear(Duration.ofMillis(100)))
                .retriableExceptions(retriableExceptions)
                .build();
    }

    private RetryingRunnable<SQLException> getTwoTimesFailingButRetriable(int retries,
                                                                          List<Class<? extends Exception>> retriableExceptions) {
        return RetryingRunnable.<SQLException> builder()
                .retries(retries)
                .doRun(() -> {
                    if (runs.incrementAndGet() <= 2) {
                        throw new SQLRecoverableException(String.format("Good try, but I fail this time (call #%s)", runs));
                    }
                })
                .doAutoHeal(heals::incrementAndGet)
                .delayStrategy(DelayStrategy.linear(Duration.ofMillis(100)))
                .retriableExceptions(retriableExceptions)
                .build();
    }

    private RetryingRunnable<SQLException> getTwoTimesFailingWithRetriableCause(int retries,
                                                                                List<Class<? extends Exception>> retriableExceptions) {
        return RetryingRunnable.<SQLException> builder()
                .retries(retries)
                .doRun(() -> {
                    if (runs.incrementAndGet() <= 2) {
                        Exception cause = new SQLRecoverableException(String.format("Good try, but I fail this time (call #%s)", runs));
                        throw new DebeziumException(cause);
                    }
                })
                .doAutoHeal(heals::incrementAndGet)
                .delayStrategy(DelayStrategy.linear(Duration.ofMillis(100)))
                .retriableExceptions(retriableExceptions)
                .build();
    }
}
