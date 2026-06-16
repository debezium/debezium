/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDate;
import java.time.LocalDateTime;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

public class NanoTimestampTest {

    @Test
    @FixFor("debezium/dbz#2075")
    public void shouldConvertWithinRangeValues() {
        // epoch
        assertThat(NanoTimestamp.toEpochNanos(LocalDateTime.of(1970, 1, 1, 0, 0, 0), null))
                .isEqualTo(0L);

        // 2001-09-09T01:46:40Z (round number, well within range)
        assertThat(NanoTimestamp.toEpochNanos(LocalDateTime.of(2001, 9, 9, 1, 46, 40), null))
                .isEqualTo(1_000_000_000_000_000_000L);

        // Just inside the upper boundary: 2262-04-11T23:47:16.854775807Z
        long upperBoundary = NanoTimestamp.toEpochNanos(
                LocalDateTime.of(2262, 4, 11, 23, 47, 16, 854_775_807),
                null);
        assertThat(upperBoundary).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    @FixFor("debezium/dbz#2075")
    public void shouldFailFastForDateAboveNanosecondRange() {
        // 9999-12-31 — common end-of-time sentinel that overflows INT64 nanos
        assertThatThrownBy(() -> NanoTimestamp.toEpochNanos(LocalDate.of(9999, 12, 31), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outside the representable range")
                .hasMessageContaining("9999-12-31")
                .hasCauseInstanceOf(ArithmeticException.class);
    }

    @Test
    @FixFor("debezium/dbz#2075")
    public void shouldFailFastForDateBelowNanosecondRange() {
        // 1000-01-01 — well before 1677-09-21 lower bound
        assertThatThrownBy(() -> NanoTimestamp.toEpochNanos(LocalDate.of(1000, 1, 1), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outside the representable range")
                .hasCauseInstanceOf(ArithmeticException.class);
    }

    @Test
    @FixFor("debezium/dbz#2075")
    public void shouldFailFastForTimestampJustAboveUpperBoundary() {
        // One nanosecond past the upper boundary 2262-04-11T23:47:16.854775807Z
        LocalDateTime justOver = LocalDateTime.of(2262, 4, 11, 23, 47, 16, 854_775_808);
        assertThatThrownBy(() -> NanoTimestamp.toEpochNanos(justOver, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outside the representable range")
                .hasCauseInstanceOf(ArithmeticException.class);
    }

    @Test
    @FixFor("debezium/dbz#2075")
    public void shouldFailFastForFarFutureDate() {
        // 2286-11-20 — the value that motivated this fix (silent wrap to 1702 in Debezium <= 3.6)
        assertThatThrownBy(() -> NanoTimestamp.toEpochNanos(LocalDate.of(2286, 11, 20), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outside the representable range")
                .hasCauseInstanceOf(ArithmeticException.class);
    }
}
