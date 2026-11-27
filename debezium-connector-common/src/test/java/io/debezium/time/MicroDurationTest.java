/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

public class MicroDurationTest {

    @Test
    @FixFor("debezium/dbz#3199")
    public void shouldNotLoseMicrosecondForFractionalSeconds() {
        assertThat(MicroDuration.durationMicros(0, 0, 0, 0, 0, 0.000249, MicroDuration.DAYS_PER_MONTH_AVG)).isEqualTo(249L);
        assertThat(MicroDuration.durationMicros(0, 0, 0, 0, 0, 0.000489, MicroDuration.DAYS_PER_MONTH_AVG)).isEqualTo(489L);
        assertThat(MicroDuration.durationMicros(0, 0, 0, 0, 0, -0.000249, MicroDuration.DAYS_PER_MONTH_AVG)).isEqualTo(-249L);
        assertThat(MicroDuration.durationMicros(0, 0, 1, 0, 0, 0.000249, MicroDuration.DAYS_PER_MONTH_AVG)).isEqualTo(86_400_000_249L);
    }

    @Test
    @FixFor("debezium/dbz#3199")
    public void shouldConvertEveryMicrosecondOfASecond() {
        for (int micros = 0; micros < 1_000_000; micros++) {
            final double seconds = Double.parseDouble("0.%06d".formatted(micros));
            assertThat(MicroDuration.durationMicros(0, 0, 0, 0, 0, seconds, MicroDuration.DAYS_PER_MONTH_AVG)).isEqualTo((long) micros);
        }
    }

    @Test
    public void shouldConvertAllUnits() {
        final long seconds = 31_557_600L + 86_400L + 2 * 3_600L + 3 * 60L + 4L;
        assertThat(MicroDuration.durationMicros(1, 0, 1, 2, 3, 4, 5, MicroDuration.DAYS_PER_MONTH_AVG)).isEqualTo(seconds * 1_000_000L + 5L);
    }
}
