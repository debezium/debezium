/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;

/**
 * Unit test for {@code Temporals}.
 *
 * @author Gunnar Morling
 */
public class TemporalsTest {

    @Test
    public void maxHandlesSameUnit() {
        Duration hundredMillis = Duration.ofMillis(100);
        Duration thousandMillis = Duration.ofMillis(1000);
        assertThat(Temporals.max(hundredMillis, thousandMillis)).isEqualTo(thousandMillis);
    }

    @Test
    public void maxHandlesDifferentUnits() {
        Duration sixtyOneMinutes = Duration.ofMinutes(61);
        Duration oneHour = Duration.ofHours(1);
        assertThat(Temporals.max(sixtyOneMinutes, oneHour)).isEqualTo(sixtyOneMinutes);
    }

    @Test
    public void maxHandlesEqualValue() {
        Duration oneMilli = Duration.ofMillis(1);
        Duration oneMillionNanos = Duration.ofNanos(1_000_000);
        assertThat(Temporals.max(oneMilli, oneMillionNanos)).isEqualTo(oneMilli);
        assertThat(Temporals.max(oneMilli, oneMillionNanos)).isEqualTo(oneMillionNanos);
    }

    @Test
    public void maxAndMinHandleSubSecondDurationsWithSameWholeSeconds() {
        // Duration.compareTo returns the nanos difference (not a normalized 1) when the whole-second
        // counts are equal, so comparing against == 1 gets both max and min backwards here.
        Duration fiveHundredMillis = Duration.ofMillis(500);
        Duration twoHundredMillis = Duration.ofMillis(200);
        assertThat(Temporals.max(fiveHundredMillis, twoHundredMillis)).isEqualTo(fiveHundredMillis);
        assertThat(Temporals.min(fiveHundredMillis, twoHundredMillis)).isEqualTo(twoHundredMillis);
    }

    @Test
    public void minCapsValueJustAboveTheLimit() {
        // Mirrors ChangeEventQueue capping poll.interval.ms at 5000ms: values 5001-5999 share the
        // same whole seconds as 5000, so the cap leaked before comparing against > 0.
        assertThat(Temporals.min(Duration.ofMillis(5001), Duration.ofMillis(5000)))
                .isEqualTo(Duration.ofMillis(5000));
    }
}
