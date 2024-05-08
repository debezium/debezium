/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.temporal.ChronoUnit;

import org.junit.Test;

import io.debezium.doc.FixFor;

public class NanoDurationTest {

    @Test
    @FixFor("DBZ-1405")
    public void testNanoDuration() {
        long years = 6 * 12 * 30 * ChronoUnit.DAYS.getDuration().toNanos();
        long months = 7 * 30 * ChronoUnit.DAYS.getDuration().toNanos();
        long days = 1 * ChronoUnit.DAYS.getDuration().toNanos();
        long hours = 2 * ChronoUnit.HOURS.getDuration().toNanos();
        long minutes = 3 * ChronoUnit.MINUTES.getDuration().toNanos();
        long seconds = 4 * ChronoUnit.SECONDS.getDuration().toNanos();
        long nanos = 5;

        assertThat(NanoDuration.durationNanos(6, 7, 1, 2, 3, 4, 5)).isEqualTo(years + months + days + hours + minutes + seconds + nanos);
    }
}
