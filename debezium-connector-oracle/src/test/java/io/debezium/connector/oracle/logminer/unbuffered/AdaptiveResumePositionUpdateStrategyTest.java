/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.unbuffered;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogFile;
import io.debezium.doc.FixFor;
import io.debezium.util.Clock;

/**
 * Unit tests for {@link AdaptiveResumePositionUpdateStrategy}.
 *
 * @author Chris Cranford
 */
public class AdaptiveResumePositionUpdateStrategyTest {

    private static final long INTERVAL_MS = 60_000L;
    private static final long MIN_INTERVAL_MS = 1_000L;

    private final TestClock clock = new TestClock();

    @Test
    @FixFor("debezium/dbz#1354")
    public void shouldWaitForIntervalBeforeFirstUpdate() {
        final var strategy = createStrategy(Configuration.create());
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(100), List.of())).isFalse();

        // The first update is not suppressed by an unchanged commit position, there is no prior result
        clock.advance(INTERVAL_MS + 1);
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(100), List.of())).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1354")
    public void shouldUpdateBeforeIntervalOnFirstUpdateWhenThresholdReached() {
        final var strategy = createStrategy(Configuration.create()
                .with(OracleConnectorConfig.LOG_MINING_RESUME_POSITION_THRESHOLD_COMMITS, 1));

        clock.advance(MIN_INTERVAL_MS + 1);
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(100), List.of())).isFalse();

        strategy.onCommit(1);
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(100), List.of())).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1354")
    public void shouldUpdateWhenIntervalExpires() {
        final var strategy = createStrategyAfterFirstUpdate(Configuration.create());

        clock.advance(INTERVAL_MS / 2);
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(200), List.of())).isFalse();

        clock.advance(INTERVAL_MS);
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(200), List.of())).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1354")
    public void shouldNotUpdateWhenCommitScnUnchangedEvenAfterIntervalExpires() {
        final var strategy = createStrategyAfterFirstUpdate(Configuration.create());

        clock.advance(INTERVAL_MS * 2);
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(100), List.of())).isFalse();

        // Once the commit position moves, the expired interval applies again
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(101), List.of())).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1354")
    public void shouldUpdateWhenLogCountThresholdReached() {
        final var strategy = createStrategyAfterFirstUpdate(Configuration.create()
                .with(OracleConnectorConfig.LOG_MINING_RESUME_POSITION_THRESHOLD_LOG_COUNT, 3));

        clock.advance(MIN_INTERVAL_MS + 1);

        // The first log ends before the last calculation and is not counted
        final List<LogFile> twoLogs = List.of(
                archive(1, 10, 50, 100),
                archive(2, 50, 150, 100),
                redo(3, 150, 100, true));
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(200), twoLogs)).isFalse();

        final List<LogFile> threeLogs = List.of(
                archive(1, 50, 150, 100),
                archive(2, 150, 180, 100),
                redo(3, 180, 100, true));
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(200), threeLogs)).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1354")
    public void shouldUpdateWhenLogBytesThresholdReached() {
        final var strategy = createStrategyAfterFirstUpdate(Configuration.create()
                .with(OracleConnectorConfig.LOG_MINING_RESUME_POSITION_THRESHOLD_LOG_BYTES, 1_000L));

        clock.advance(MIN_INTERVAL_MS + 1);

        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(200),
                List.of(archive(1, 50, 150, 400), redo(2, 150, 500, true)))).isFalse();

        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(200),
                List.of(archive(1, 50, 150, 400), redo(2, 150, 600, true)))).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1354")
    public void shouldUpdateWhenScnDistanceThresholdReached() {
        final var strategy = createStrategyAfterFirstUpdate(Configuration.create()
                .with(OracleConnectorConfig.LOG_MINING_RESUME_POSITION_THRESHOLD_SCN_DISTANCE, 1_000L));

        clock.advance(MIN_INTERVAL_MS + 1);

        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(1_099), List.of())).isFalse();
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(1_100), List.of())).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1354")
    public void shouldNotRepeatLogCountUpdateWhileLongTransactionHoldsResumeScn() {
        final var strategy = createStrategyAfterFirstUpdate(Configuration.create()
                .with(OracleConnectorConfig.LOG_MINING_RESUME_POSITION_THRESHOLD_LOG_COUNT, 10));

        // A long-running transaction holds the resume position at 100, while many logs follow it
        final List<LogFile> logs = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            logs.add(archive(i + 1, 100 + i * 100L, 200 + i * 100L, 100));
        }
        logs.add(redo(21, 2_100, 100, true));

        clock.advance(MIN_INTERVAL_MS + 1);
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(2_000), logs)).isTrue();
        strategy.onUpdated(Scn.valueOf(2_000));

        // Other transactions keep committing, but no new logs follow the last calculation, and the
        // calculation would return the same resume position again
        for (int i = 1; i <= 5; i++) {
            clock.advance(MIN_INTERVAL_MS + 1);
            assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(2_000 + i * 10L), logs)).isFalse();
        }

        // Once enough new logs follow the last calculation, the threshold applies again
        final List<LogFile> moreLogs = new ArrayList<>(logs.subList(0, 20));
        for (int i = 0; i < 10; i++) {
            moreLogs.add(archive(22 + i, 2_100 + i * 100L, 2_200 + i * 100L, 100));
        }
        moreLogs.add(redo(32, 3_100, 100, true));
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(3_000), moreLogs)).isTrue();
        strategy.onUpdated(Scn.valueOf(3_000));

        // The interval still applies, as it did before
        clock.advance(INTERVAL_MS + 1);
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(3_010), moreLogs)).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1354")
    public void shouldNotRepeatScnDistanceUpdateWhileLongTransactionHoldsResumeScn() {
        final var strategy = createStrategyAfterFirstUpdate(Configuration.create()
                .with(OracleConnectorConfig.LOG_MINING_RESUME_POSITION_THRESHOLD_SCN_DISTANCE, 1_000L));

        clock.advance(MIN_INTERVAL_MS + 1);
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(5_000), List.of())).isTrue();
        strategy.onUpdated(Scn.valueOf(5_000));

        // The resume position does not move, the distance is measured from the last calculation
        clock.advance(MIN_INTERVAL_MS + 1);
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(5_999), List.of())).isFalse();
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(6_000), List.of())).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1354")
    public void shouldUpdateWhenCommitCountThresholdReached() {
        final var strategy = createStrategyAfterFirstUpdate(Configuration.create()
                .with(OracleConnectorConfig.LOG_MINING_RESUME_POSITION_THRESHOLD_COMMITS, 3));

        clock.advance(MIN_INTERVAL_MS + 1);

        strategy.onCommit(1);
        strategy.onCommit(1);
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(200), List.of())).isFalse();

        strategy.onCommit(1);
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(200), List.of())).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1354")
    public void shouldUpdateWhenTransactionEventsThresholdReached() {
        final var strategy = createStrategyAfterFirstUpdate(Configuration.create()
                .with(OracleConnectorConfig.LOG_MINING_RESUME_POSITION_THRESHOLD_TRANSACTION_EVENTS, 500));

        clock.advance(MIN_INTERVAL_MS + 1);

        strategy.onCommit(499);
        strategy.onCommit(10);
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(200), List.of())).isFalse();

        strategy.onCommit(500);
        strategy.onCommit(1);
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(200), List.of())).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1354")
    public void shouldNotUpdateBeforeMinimumIntervalEvenWhenHeuristicFires() {
        final var strategy = createStrategyAfterFirstUpdate(Configuration.create()
                .with(OracleConnectorConfig.LOG_MINING_RESUME_POSITION_THRESHOLD_COMMITS, 1));

        strategy.onCommit(1);
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(200), List.of())).isFalse();

        clock.advance(MIN_INTERVAL_MS + 1);
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(200), List.of())).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1354")
    public void shouldNotUseHeuristicsWhenThresholdsAreZero() {
        final var strategy = createStrategyAfterFirstUpdate(Configuration.create()
                .with(OracleConnectorConfig.LOG_MINING_RESUME_POSITION_THRESHOLD_LOG_COUNT, 0)
                .with(OracleConnectorConfig.LOG_MINING_RESUME_POSITION_THRESHOLD_LOG_BYTES, 0)
                .with(OracleConnectorConfig.LOG_MINING_RESUME_POSITION_THRESHOLD_SCN_DISTANCE, 0)
                .with(OracleConnectorConfig.LOG_MINING_RESUME_POSITION_THRESHOLD_COMMITS, 0)
                .with(OracleConnectorConfig.LOG_MINING_RESUME_POSITION_THRESHOLD_TRANSACTION_EVENTS, 0));

        clock.advance(MIN_INTERVAL_MS + 1);

        for (int i = 0; i < 1_000; i++) {
            strategy.onCommit(1_000_000);
        }

        final List<LogFile> logs = List.of(
                archive(1, 50, 150, Long.MAX_VALUE / 4),
                archive(2, 150, 250, Long.MAX_VALUE / 4),
                redo(3, 250, Long.MAX_VALUE / 4, true));
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(10_000_000), logs)).isFalse();

        clock.advance(INTERVAL_MS);
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(10_000_000), logs)).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1354")
    public void shouldResetCountersWhenUpdated() {
        final var strategy = createStrategyAfterFirstUpdate(Configuration.create()
                .with(OracleConnectorConfig.LOG_MINING_RESUME_POSITION_THRESHOLD_COMMITS, 2)
                .with(OracleConnectorConfig.LOG_MINING_RESUME_POSITION_THRESHOLD_TRANSACTION_EVENTS, 100));

        clock.advance(MIN_INTERVAL_MS + 1);

        strategy.onCommit(100);
        strategy.onCommit(1);
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(200), List.of())).isTrue();
        strategy.onUpdated(Scn.valueOf(200));

        clock.advance(MIN_INTERVAL_MS + 1);
        strategy.onCommit(1);
        assertThat(strategy.shouldUpdate(Scn.valueOf(200), Scn.valueOf(300), List.of())).isFalse();
    }

    private AdaptiveResumePositionUpdateStrategy createStrategy(Configuration.Builder builder) {
        final Configuration config = builder
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .with(OracleConnectorConfig.LOG_MINING_RESUME_POSITION_INTERVAL_MS, INTERVAL_MS)
                .with(OracleConnectorConfig.LOG_MINING_RESUME_POSITION_MIN_INTERVAL_MS, MIN_INTERVAL_MS)
                .build();
        return new AdaptiveResumePositionUpdateStrategy(new OracleConnectorConfig(config), clock);
    }

    private AdaptiveResumePositionUpdateStrategy createStrategyAfterFirstUpdate(Configuration.Builder builder) {
        final AdaptiveResumePositionUpdateStrategy strategy = createStrategy(builder);
        clock.advance(INTERVAL_MS + 1);
        assertThat(strategy.shouldUpdate(Scn.valueOf(100), Scn.valueOf(100), List.of())).isTrue();
        strategy.onUpdated(Scn.valueOf(100));
        return strategy;
    }

    private static LogFile archive(int sequence, long firstScn, long nextScn, long bytes) {
        return LogFile.forArchive("archive" + sequence, Scn.valueOf(firstScn), Scn.valueOf(nextScn),
                BigInteger.valueOf(sequence), 1, bytes, false, false);
    }

    private static LogFile redo(int sequence, long firstScn, long bytes, boolean current) {
        return LogFile.forRedo("redo" + sequence, Scn.valueOf(firstScn), Scn.valueOf(Long.MAX_VALUE),
                BigInteger.valueOf(sequence), current, 1, bytes);
    }

    /**
     * A clock whose time only moves when the test advances it.
     */
    private static class TestClock implements Clock {
        private long millis = 1_000_000L;

        void advance(long delta) {
            millis += delta;
        }

        @Override
        public long currentTimeInMillis() {
            return millis;
        }
    }
}
