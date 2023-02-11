/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.Test;

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
}
