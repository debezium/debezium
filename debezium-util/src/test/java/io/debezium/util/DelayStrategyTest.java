/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.Test;

/**
 * Tests the jittered exponential {@link DelayStrategy}.
 */
public class DelayStrategyTest {

    private static final Duration INITIAL = Duration.ofMillis(20);
    private static final Duration MAX = Duration.ofMillis(80);

    @Test
    void shouldRejectNonPositiveInitialDelay() {
        assertThatThrownBy(() -> DelayStrategy.exponentialWithJitter(Duration.ZERO, MAX, 2.0, 0.25))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectMaxDelayLowerThanInitialDelay() {
        assertThatThrownBy(() -> DelayStrategy.exponentialWithJitter(MAX, INITIAL, 2.0, 0.25))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectBackOffMultiplierNotGreaterThanOne() {
        assertThatThrownBy(() -> DelayStrategy.exponentialWithJitter(INITIAL, MAX, 1.0, 0.25))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectJitterFactorOutsideValidRange() {
        assertThatThrownBy(() -> DelayStrategy.exponentialWithJitter(INITIAL, MAX, 2.0, -0.1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DelayStrategy.exponentialWithJitter(INITIAL, MAX, 2.0, 1.0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldNotSleepWhenCriteriaIsNotMet() {
        final DelayStrategy strategy = DelayStrategy.exponentialWithJitter(INITIAL, MAX, 2.0, 0.25);
        assertThat(strategy.sleepWhen(false)).isFalse();
    }

    @Test
    void shouldSleepAtLeastTheJitteredLowerBoundAndStayAtMaximum() {
        // Jitter randomizes each sleep within +/- 25% of the current delay, so only the
        // lower bound is asserted; upper bounds would be flaky on loaded CI workers.
        final DelayStrategy strategy = DelayStrategy.exponentialWithJitter(INITIAL, MAX, 2.0, 0.25);
        assertThat(elapsedMs(strategy)).isGreaterThanOrEqualTo(15); // 20 - 25%
        assertThat(elapsedMs(strategy)).isGreaterThanOrEqualTo(30); // 40 - 25%
        assertThat(elapsedMs(strategy)).isGreaterThanOrEqualTo(60); // 80 - 25%
        assertThat(elapsedMs(strategy)).isGreaterThanOrEqualTo(60); // stays at the maximum
    }

    @Test
    void shouldFollowThePlainExponentialProgressionWhenJitterIsZero() {
        final DelayStrategy strategy = DelayStrategy.exponentialWithJitter(INITIAL, MAX, 2.0, 0.0);
        assertThat(elapsedMs(strategy)).isGreaterThanOrEqualTo(20);
        assertThat(elapsedMs(strategy)).isGreaterThanOrEqualTo(40);
        assertThat(elapsedMs(strategy)).isGreaterThanOrEqualTo(80);
        assertThat(elapsedMs(strategy)).isGreaterThanOrEqualTo(80);
    }

    @Test
    void shouldResetProgressionWhenCriteriaIsNotMet() {
        // A wide gap between the steps (20ms vs 200ms) keeps the upper-bound assertion
        // meaningful without being flaky on loaded CI workers.
        final DelayStrategy strategy = DelayStrategy.exponentialWithJitter(Duration.ofMillis(20), Duration.ofMillis(2000), 10.0, 0.0);
        assertThat(elapsedMs(strategy)).isGreaterThanOrEqualTo(20);
        assertThat(elapsedMs(strategy)).isGreaterThanOrEqualTo(200);
        assertThat(strategy.sleepWhen(false)).isFalse();
        final long restarted = elapsedMs(strategy);
        assertThat(restarted).isGreaterThanOrEqualTo(20);
        assertThat(restarted).isLessThan(200);
    }

    private long elapsedMs(DelayStrategy strategy) {
        final long start = System.nanoTime();
        assertThat(strategy.sleepWhen(true)).isTrue();
        return (System.nanoTime() - start) / 1_000_000;
    }
}
