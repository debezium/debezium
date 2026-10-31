/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.doc.FixFor;
import io.debezium.util.Clock;

class WalSearchProgressTrackerTest {

    private static final Duration INTERVAL = Duration.ofSeconds(60);

    private final AtomicLong now = new AtomicLong(1_000_000L);
    private final Clock clock = now::get;

    @Test
    @FixFor("debezium/dbz#1337")
    void shouldNotWarnBeforeIntervalElapses() {
        final var tracker = new WalSearchProgressTracker(clock, INTERVAL);

        advance(Duration.ofSeconds(59));

        assertThat(tracker.shouldWarn()).isFalse();
        assertThat(tracker.lastReceivedLsn()).isNull();
    }

    @Test
    @FixFor("debezium/dbz#1337")
    void shouldWarnWhenNoLsnArrivesDuringInterval() {
        final var tracker = new WalSearchProgressTracker(clock, INTERVAL);

        advance(INTERVAL);

        assertThat(tracker.shouldWarn()).isTrue();
        assertThat(tracker.timeWithoutProgress()).isEqualTo(INTERVAL);
        assertThat(tracker.elapsed()).isEqualTo(INTERVAL);
    }

    @Test
    @FixFor("debezium/dbz#1337")
    void shouldResetIntervalWhenLsnAdvances() {
        final var tracker = new WalSearchProgressTracker(clock, INTERVAL);

        advance(Duration.ofSeconds(50));
        tracker.lsnReceived(Lsn.valueOf(100L));
        advance(Duration.ofSeconds(50));

        assertThat(tracker.shouldWarn()).isFalse();
        assertThat(tracker.lastReceivedLsn()).isEqualTo(Lsn.valueOf(100L));
        assertThat(tracker.timeWithoutProgress()).isEqualTo(Duration.ofSeconds(50));
        assertThat(tracker.elapsed()).isEqualTo(Duration.ofSeconds(100));

        advance(Duration.ofSeconds(10));
        assertThat(tracker.shouldWarn()).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1337")
    void shouldNotTreatSameOrOlderLsnAsProgress() {
        final var tracker = new WalSearchProgressTracker(clock, INTERVAL);

        tracker.lsnReceived(Lsn.valueOf(100L));
        advance(Duration.ofSeconds(50));
        tracker.lsnReceived(Lsn.valueOf(100L));
        tracker.lsnReceived(Lsn.valueOf(90L));
        tracker.lsnReceived(null);
        advance(Duration.ofSeconds(10));

        assertThat(tracker.shouldWarn()).isTrue();
        assertThat(tracker.lastReceivedLsn()).isEqualTo(Lsn.valueOf(100L));
    }

    @Test
    @FixFor("debezium/dbz#1337")
    void shouldRepeatWarningAtEachInterval() {
        final var tracker = new WalSearchProgressTracker(clock, INTERVAL);

        advance(INTERVAL);
        assertThat(tracker.shouldWarn()).isTrue();
        assertThat(tracker.shouldWarn()).isFalse();

        advance(Duration.ofSeconds(30));
        assertThat(tracker.shouldWarn()).isFalse();

        advance(Duration.ofSeconds(30));
        assertThat(tracker.shouldWarn()).isTrue();
        assertThat(tracker.timeWithoutProgress()).isEqualTo(INTERVAL.multipliedBy(2));
    }

    @Test
    @FixFor("debezium/dbz#1337")
    void shouldNeverWarnWhenDisabled() {
        final var tracker = new WalSearchProgressTracker(clock, Duration.ZERO);

        advance(Duration.ofHours(1));

        assertThat(tracker.shouldWarn()).isFalse();
    }

    private void advance(Duration duration) {
        now.addAndGet(duration.toMillis());
    }
}
