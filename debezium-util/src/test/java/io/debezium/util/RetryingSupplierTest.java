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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;

/**
 * Tests RetryingSupplier.
 * Refactored and inspired by: io.debezium.util.RetryingRunnableTest
 */
public class RetryingSupplierTest {

    private final List<Class<? extends Exception>> retriableExceptions = List.of(SQLRecoverableException.class);
    private final AtomicInteger runs = new AtomicInteger();
    private final AtomicInteger heals = new AtomicInteger();
    private final AtomicInteger sleeps = new AtomicInteger();

    private final DelayStrategy countingDelay = criteria -> {
        if (criteria) {
            sleeps.incrementAndGet();
        }
        return criteria;
    };

    @BeforeEach
    void init() {
        runs.set(0);
        heals.set(0);
        sleeps.set(0);
    }

    @Test
    void shouldReturnValueWithoutRetryWhenNeverFailing() throws InterruptedException, SQLException {
        assertThat(getNeverFailing(10).get()).isEqualTo(1);
        assertThat(runs.get()).isEqualTo(1);
        assertThat(heals.get()).isEqualTo(0);
    }

    @Test
    void shouldIgnoreInfiniteRetryWhenSupplierDoesNotFail() throws InterruptedException, SQLException {
        assertThat(getNeverFailing(-1).get()).isEqualTo(1);
        assertThat(runs.get()).isEqualTo(1);
    }

    @Test
    void shouldReturnValueAfterRetries() throws InterruptedException, SQLException {
        assertThat(getTwoTimesFailing(10, null).get()).isEqualTo(3);

        // Supplier should fail 2 times and 3rd time it should succeed.
        assertThat(runs.get()).isEqualTo(3);
        assertThat(heals.get()).isEqualTo(2);
    }

    @Test
    void shouldRetryAsManyTimesAsRequestedWhenAlwaysFails() {
        assertThatThrownBy(() -> getAlwaysFailing(5, null).get()).isInstanceOf(SQLException.class);

        // Should be called 6 times - 1 call + 5 retries.
        assertThat(runs.get()).isEqualTo(6);
        assertThat(heals.get()).isEqualTo(5);
    }

    @Test
    void shouldNotRetryWhenRetriesAreDisabled() {
        assertThatThrownBy(() -> getAlwaysFailing(0, null).get()).isInstanceOf(SQLException.class);

        assertThat(runs.get()).isEqualTo(1);
        assertThat(heals.get()).isEqualTo(0);
    }

    @Test
    void shouldNotRetryForNonMatchingSuppliedRetriableExceptions() {
        assertThatThrownBy(() -> getAlwaysFailing(5, retriableExceptions).get()).isInstanceOf(SQLException.class);

        assertThat(runs.get()).isEqualTo(1);
        assertThat(heals.get()).isEqualTo(0);
    }

    @Test
    void shouldRetryForMatchingSuppliedRetriableCause() throws InterruptedException, SQLException {
        assertThat(getTwoTimesFailingWithRetriableCause(10).get()).isEqualTo(3);

        assertThat(runs.get()).isEqualTo(3);
        assertThat(heals.get()).isEqualTo(2);
    }

    @Test
    void shouldApplyDelayForEveryFailureWithoutAutoHeal() throws InterruptedException, SQLException {
        assertThat(RetryingSupplier.<Integer, SQLException> builder()
                .retries(10)
                .doGet(this::failTwiceThenReturn)
                .delayStrategy(countingDelay)
                .build()
                .get()).isEqualTo(3);

        assertThat(runs.get()).isEqualTo(3);
        assertThat(sleeps.get()).isEqualTo(2);
    }

    @Test
    void shouldSkipDelayWhenAutoHealSucceeds() throws InterruptedException, SQLException {
        assertThat(RetryingSupplier.<Integer, SQLException> builder()
                .retries(10)
                .doGet(this::failTwiceThenReturn)
                .doAutoHeal(heals::incrementAndGet)
                .delayStrategy(countingDelay)
                .build()
                .get()).isEqualTo(3);

        assertThat(heals.get()).isEqualTo(2);
        assertThat(sleeps.get()).isEqualTo(0);
    }

    @Test
    void shouldApplyDelayWhenAutoHealFails() throws InterruptedException, SQLException {
        assertThat(RetryingSupplier.<Integer, SQLException> builder()
                .retries(10)
                .doGet(this::failTwiceThenReturn)
                .doAutoHeal(() -> {
                    heals.incrementAndGet();
                    throw new SQLException("Heal failed");
                })
                .delayStrategy(countingDelay)
                .build()
                .get()).isEqualTo(3);

        assertThat(heals.get()).isEqualTo(2);
        assertThat(sleeps.get()).isEqualTo(2);
    }

    private int failTwiceThenReturn() throws SQLException {
        int call = runs.incrementAndGet();
        if (call <= 2) {
            throw new SQLException(String.format("Good try, but I fail this time (call #%s)", call));
        }
        return call;
    }

    private RetryingSupplier<Integer, SQLException> getNeverFailing(int retries) {
        return RetryingSupplier.<Integer, SQLException> builder()
                .retries(retries)
                .doGet(runs::incrementAndGet)
                .doAutoHeal(heals::incrementAndGet)
                .delayStrategy(DelayStrategy.linear(Duration.ofMillis(100)))
                .build();
    }

    private RetryingSupplier<Integer, SQLException> getAlwaysFailing(int retries,
                                                                     List<Class<? extends Exception>> retriableExceptions) {
        return RetryingSupplier.<Integer, SQLException> builder()
                .retries(retries)
                .doGet(() -> {
                    runs.incrementAndGet();
                    throw new SQLException("Good try, but I always fail");
                })
                .doAutoHeal(heals::incrementAndGet)
                .delayStrategy(DelayStrategy.linear(Duration.ofMillis(100)))
                .retriableExceptions(retriableExceptions)
                .build();
    }

    private RetryingSupplier<Integer, SQLException> getTwoTimesFailing(int retries,
                                                                       List<Class<? extends Exception>> retriableExceptions) {
        return RetryingSupplier.<Integer, SQLException> builder()
                .retries(retries)
                .doGet(this::failTwiceThenReturn)
                .doAutoHeal(heals::incrementAndGet)
                .delayStrategy(DelayStrategy.linear(Duration.ofMillis(100)))
                .retriableExceptions(retriableExceptions)
                .build();
    }

    private RetryingSupplier<Integer, SQLException> getTwoTimesFailingWithRetriableCause(int retries) {
        return RetryingSupplier.<Integer, SQLException> builder()
                .retries(retries)
                .doGet(() -> {
                    int call = runs.incrementAndGet();
                    if (call <= 2) {
                        throw new DebeziumException(new SQLRecoverableException(
                                String.format("Good try, but I fail this time (call #%s)", call)));
                    }
                    return call;
                })
                .doAutoHeal(heals::incrementAndGet)
                .delayStrategy(DelayStrategy.linear(Duration.ofMillis(100)))
                .retriableExceptions(retriableExceptions)
                .build();
    }
}
