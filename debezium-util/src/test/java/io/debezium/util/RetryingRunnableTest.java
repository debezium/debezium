/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.doc.FixFor;
import io.debezium.function.ThrowingRunnable;

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
            getAlwaysFailingButRetriable(5, retriableExceptions, DelayStrategy.linear(Duration.ofMillis(100)), heals::incrementAndGet).run();
        }
        catch (SQLException e) {
            // Good
        }

        // Should be called 6 times - 1 call + 5 retries.
        assertThat(runs.get()).isEqualTo(6);
        Assertions.assertThat(heals.get()).isEqualTo(5);
    }

    @FixFor("debezium/dbz#2329")
    @Test
    void shouldRetryAsManyTimesAsRequestedHonoringDelayStrategyWhenNoAutoHealIsConfigured() throws InterruptedException {

        long startTime = System.currentTimeMillis();
        try {
            getAlwaysFailingButRetriable(2, retriableExceptions, DelayStrategy.exponential(Duration.ofMillis(100), Duration.ofMillis(1000)), null).run();
        }
        catch (SQLException e) {
            // Good
        }
        long endTime = System.currentTimeMillis();
        // Should be called 3 times - 1 call + 2 retries.
        assertThat(runs.get()).isEqualTo(3);
        Assertions.assertThat(endTime - startTime).isGreaterThanOrEqualTo(Duration.ofMillis(300).toMillis());
    }

    @FixFor("debezium/dbz#2498")
    @Test
    void shouldNotRetryWhenRetriableWhenPredicateRejectsException() throws InterruptedException, SQLException {
        try {
            RetryingRunnable.<SQLException> builder()
                    .retries(5)
                    .retriableWhen(e -> false)
                    .doRun(() -> {
                        runs.incrementAndGet();
                        throw new SQLRecoverableException("Good try, but I always fail");
                    })
                    .doAutoHeal(heals::incrementAndGet)
                    .delayStrategy(DelayStrategy.linear(Duration.ofMillis(100)))
                    .build()
                    .run();
        }
        catch (SQLRecoverableException e) {
            // Good
        }

        // Should be called only 1 time: the predicate rejects the exception, and with a predicate configured
        // the empty default type list no longer means "retry all".
        assertThat(runs.get()).isEqualTo(1);
        Assertions.assertThat(heals.get()).isEqualTo(0);
    }

    @FixFor("debezium/dbz#2498")
    @Test
    void shouldRetryAsManyTimesAsRequestedWhenRetriableWhenPredicateAcceptsException() throws InterruptedException, SQLException {
        RetryingRunnable.<SQLException> builder()
                .retries(10)
                .retriableWhen(e -> true)
                .doRun(() -> {
                    if (runs.incrementAndGet() <= 2) {
                        throw new SQLException("Good try, but I fail this time");
                    }
                })
                .doAutoHeal(heals::incrementAndGet)
                .delayStrategy(DelayStrategy.linear(Duration.ofMillis(100)))
                .build()
                .run();

        // Runnable should fail 2 times and 3rd time it should succeed.
        assertThat(runs.get()).isEqualTo(3);
        Assertions.assertThat(heals.get()).isEqualTo(2);
    }

    @FixFor("debezium/dbz#2498")
    @Test
    void shouldRetryOnlySubsetOfExceptionsAcceptedByRetriableWhenPredicate() throws InterruptedException {
        // Fails with a retriable SQLRecoverableException first, then with a non-retriable SQLException.
        try {
            RetryingRunnable.<SQLException> builder()
                    .retries(10)
                    .retriableWhen(e -> e instanceof SQLRecoverableException)
                    .doRun(() -> {
                        if (runs.incrementAndGet() == 1) {
                            throw new SQLRecoverableException("Good try, but I fail this time");
                        }
                        throw new SQLException("Not retriable this time");
                    })
                    .doAutoHeal(heals::incrementAndGet)
                    .delayStrategy(DelayStrategy.linear(Duration.ofMillis(100)))
                    .build()
                    .run();
        }
        catch (SQLException e) {
            // Good
        }

        // First failure retried, second propagated immediately by the predicate.
        assertThat(runs.get()).isEqualTo(2);
        Assertions.assertThat(heals.get()).isEqualTo(1);
    }

    @FixFor("debezium/dbz#2498")
    @Test
    void shouldRetryWhenRetriableExceptionsMatchEvenIfRetriableWhenPredicateRejectsException() throws InterruptedException, SQLException {
        try {
            RetryingRunnable.<SQLException> builder()
                    .retries(5)
                    .retriableExceptions(SQLRecoverableException.class)
                    .retriableWhen(e -> false)
                    .doRun(() -> {
                        runs.incrementAndGet();
                        throw new SQLRecoverableException("Good try, but I always fail");
                    })
                    .doAutoHeal(heals::incrementAndGet)
                    .delayStrategy(DelayStrategy.linear(Duration.ofMillis(100)))
                    .build()
                    .run();
        }
        catch (SQLRecoverableException e) {
            // Good
        }

        // The predicate rejects the exception, but the type list accepts it: the conditions are OR-ed, so it is retried.
        assertThat(runs.get()).isEqualTo(6);
        Assertions.assertThat(heals.get()).isEqualTo(5);
    }

    @FixFor("debezium/dbz#2498")
    @Test
    void shouldRetryWhenRetriableExceptionsMatchCauseEvenIfRetriableWhenPredicateRejectsWrapperException() throws InterruptedException {
        try {
            RetryingRunnable.<SQLException> builder()
                    .retries(5)
                    .retriableExceptions(SQLRecoverableException.class)
                    .retriableWhen(e -> e instanceof SQLRecoverableException)
                    .doRun(() -> {
                        runs.incrementAndGet();
                        throw new SQLException("wrapped", new SQLRecoverableException("retriable cause"));
                    })
                    .doAutoHeal(heals::incrementAndGet)
                    .delayStrategy(DelayStrategy.linear(Duration.ofMillis(100)))
                    .build()
                    .run();
        }
        catch (SQLException e) {
            // Good
        }

        // The predicate sees only the outer SQLException and rejects it, but the type list descends the cause
        // chain and matches the SQLRecoverableException cause: the conditions are OR-ed, so it is retried.
        assertThat(runs.get()).isEqualTo(6);
        Assertions.assertThat(heals.get()).isEqualTo(5);
    }

    @FixFor("debezium/dbz#2498")
    @Test
    void shouldNotUnwrapCauseChainWhenTestingRetriableWhenPredicate() throws InterruptedException {
        try {
            RetryingRunnable.<SQLException> builder()
                    .retries(10)
                    .retriableWhen(e -> e instanceof SQLRecoverableException)
                    .doRun(() -> {
                        runs.incrementAndGet();
                        throw new SQLException("wrapped", new SQLRecoverableException("retriable cause"));
                    })
                    .doAutoHeal(heals::incrementAndGet)
                    .delayStrategy(DelayStrategy.linear(Duration.ofMillis(100)))
                    .build()
                    .run();
        }
        catch (SQLException e) {
            // Good
        }

        // The outer SQLException is not an SQLRecoverableException and the predicate must not
        // look at the cause chain, unlike the retriableExceptions type matching.
        assertThat(runs.get()).isEqualTo(1);
        Assertions.assertThat(heals.get()).isEqualTo(0);
    }

    @FixFor("debezium/dbz#2498")
    @Test
    void shouldPropagateOriginalExceptionWhenRetriableWhenPredicateThrows() {
        assertThatThrownBy(() -> RetryingRunnable.<SQLException> builder()
                .retries(10)
                .retriableWhen(e -> {
                    throw new IllegalStateException("predicate failure");
                })
                .doRun(() -> {
                    runs.incrementAndGet();
                    throw new SQLRecoverableException("Good try, but I always fail");
                })
                .doAutoHeal(heals::incrementAndGet)
                .delayStrategy(DelayStrategy.linear(Duration.ofMillis(100)))
                .build()
                .run())
                .isInstanceOf(SQLRecoverableException.class)
                .hasMessage("Good try, but I always fail");

        // When the predicate itself threw, the original exception is treated as non-retriable and propagated immediately.
        assertThat(runs.get()).isEqualTo(1);
        Assertions.assertThat(heals.get()).isEqualTo(0);
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
                                                                        List<Class<? extends Exception>> retriableExceptions, DelayStrategy delayStrategy,
                                                                        ThrowingRunnable<SQLException> autoHeal) {
        RetryingRunnable.Builder<SQLException> builder = RetryingRunnable.<SQLException> builder()
                .retries(retries)
                .retriableExceptions(retriableExceptions)
                .doRun(() -> {
                    runs.incrementAndGet();
                    throw new SQLRecoverableException("Good try, but I always fail");
                });

        if (delayStrategy != null) {
            builder.delayStrategy(delayStrategy);
        }

        if (autoHeal != null) {
            builder.doAutoHeal(autoHeal);
        }

        return builder.build();
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
