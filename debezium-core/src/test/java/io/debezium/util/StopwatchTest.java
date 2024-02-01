/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.debezium.doc.FixFor;

public class StopwatchTest {

    @FixFor("DBZ-7436")
    @Test
    public void whenCallingDurationsOnAStartedStopwatchItShouldNotFailWithANPE() {

        Stopwatch reusable = Stopwatch.reusable();

        reusable.start();

        String reusableStopWatchResults = reusable.durations().toString();

        Stopwatch.StopwatchSet multiple = Stopwatch.multiple();

        Stopwatch stopwatch = multiple.create();

        String multipleStopWatchResults = stopwatch.durations().toString();

        Stopwatch accumulating = Stopwatch.accumulating();

        String accumulatingStopWatchResults = accumulating.durations().toString();

        assertThat(reusableStopWatchResults).isEqualTo("  0.00000s total;   0 samples;  0.00000s avg;  0.00000s min;  0.00000s max");
        assertThat(multipleStopWatchResults).isEqualTo("  0.00000s total;   0 samples;  0.00000s avg;  0.00000s min;  0.00000s max");
        assertThat(accumulatingStopWatchResults).isEqualTo("  0.00000s total;   0 samples;  0.00000s avg;  0.00000s min;  0.00000s max");
    }
}
