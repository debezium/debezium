/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static io.debezium.connector.oracle.OracleConnectorConfig.LogMiningBatchSizeWindowScale.CURRENT_WRITE;
import static io.debezium.connector.oracle.OracleConnectorConfig.LogMiningBatchSizeWindowScale.EXPONENTIAL;
import static io.debezium.connector.oracle.OracleConnectorConfig.LogMiningBatchSizeWindowScale.LINEAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningBatchSizeWindowScale;
import io.debezium.connector.oracle.RedoThreadState;
import io.debezium.connector.oracle.Scn;
import io.debezium.doc.FixFor;

/**
 * Unit tests for the {@link LogMinerRangeContext} class, which is responsible for computing the mining
 * upper boundary based on a given start position and connector configuration.
 *
 * @author Chris Cranford
 */
public class LogMinerRangeContextTest {

    private static final int DEFAULT_BATCH_SIZE = 20_000;
    private static final int DEFAULT_BATCH_MIN = 1_000;
    private static final int DEFAULT_BATCH_MAX = 100_000;
    private static final int DEFAULT_BATCH_INCREMENT = 20_000;
    private static final int DEFAULT_REDO_ADJUSTMENT = 1;
    private static final long DEFAULT_SLEEP_TIME_MS = 1_000L;
    private static final long DEFAULT_SLEEP_TIME_MIN_MS = 0L;
    private static final long DEFAULT_SLEEP_TIME_MAX_MS = 3_000L;
    private static final long DEFAULT_SLEEP_TIME_INCREMENT_MS = 500L;

    private OracleConnection connection;
    private OracleConnectorConfig connectorConfig;
    private LogMinerStreamingChangeEventSourceMetrics metrics;

    @AfterEach
    public void afterEach() {
        connection = null;
        connectorConfig = null;
        metrics = null;
    }

    // -------------------------------------------------------------------------
    // Group 1: Basic boundary calculation
    // -------------------------------------------------------------------------

    @Test
    @FixFor("dbz#1713")
    public void shouldCapUpperBoundaryToLastAvailableArchiveLogScn() throws Exception {
        // archive-only: maximumReadScn = archiveMax = 1500
        // lower=1000, batchSize=20000 => initial upper=21000 >= 1500 => capped to 1500
        final OracleConnectorConfig config = createConnectorConfig(
                DEFAULT_BATCH_SIZE, DEFAULT_BATCH_MIN, DEFAULT_BATCH_MAX, DEFAULT_BATCH_INCREMENT,
                LINEAR, Duration.ZERO, DEFAULT_REDO_ADJUSTMENT, true);
        final LogMinerRangeContext context = setupMocks(config);

        final Scn result = context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(1500L), Scn.valueOf(999999L));
        assertThat(result).isEqualTo(Scn.valueOf(1500L));
    }

    @Test
    @FixFor("dbz#1713")
    public void shouldReturnBoundaryWhenWithinMaxReadScnInNonArchiveMode() throws Exception {
        // lower=1000, batchSize=20000 => initial upper=21000 < currentScn=100000 => not capped
        final LogMinerRangeContext context = setupMocks(defaultConnectorConfig());

        final Scn result = context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(100000L));
        assertThat(result).isEqualTo(Scn.valueOf(21000L));
    }

    @Test
    @FixFor("dbz#1713")
    public void shouldCapUpperBoundaryToCurrentScnWhenExceeded() throws Exception {
        // lower=90000, batchSize=20000 => initial upper=110000 >= currentScn=95000 => capped
        final LogMinerRangeContext context = setupMocks(defaultConnectorConfig());

        final Scn result = context.calculateUpperBoundary(Scn.valueOf(90000L), Scn.valueOf(500L), Scn.valueOf(95000L));
        assertThat(result).isEqualTo(Scn.valueOf(95000L));
    }

    // -------------------------------------------------------------------------
    // Group 2: Batch size management
    // -------------------------------------------------------------------------

    @Test
    @FixFor("dbz#1713")
    public void shouldIncrementBatchSizeWhenBehindCurrentWrite() throws Exception {
        // Call 1: batchSize=20000 => upper=21000, batchSize increments to 40000 for next call
        // Call 2: batchSize=40000, NOT at max (40000 != 100000) => no window scale => upper=61001
        final LogMinerRangeContext context = setupMocks(defaultConnectorConfig());

        final Scn result1 = context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(500000L));
        assertThat(result1).isEqualTo(Scn.valueOf(21000L));

        final Scn result2 = context.calculateUpperBoundary(Scn.valueOf(21001L), Scn.valueOf(500L), Scn.valueOf(500000L));
        assertThat(result2).isEqualTo(Scn.valueOf(61001L));
    }

    @Test
    @FixFor("dbz#1713")
    public void shouldDecrementBatchSizeWhenCaughtUp() throws Exception {
        // batchSizeDefault=40000: caught up => decrementBatchSize => max(40000-20000, 1000) = 20000
        final OracleConnectorConfig config = createConnectorConfig(
                40000, DEFAULT_BATCH_MIN, DEFAULT_BATCH_MAX, DEFAULT_BATCH_INCREMENT,
                LINEAR, Duration.ZERO, DEFAULT_REDO_ADJUSTMENT, false);
        final LogMinerRangeContext context = setupMocks(config);

        context.calculateUpperBoundary(Scn.valueOf(90000L), Scn.valueOf(500L), Scn.valueOf(95000L));

        verify(metrics).setBatchSize(20000);
    }

    @Test
    @FixFor("dbz#1713")
    public void shouldNotDecrementBatchSizeBelowMinimum() throws Exception {
        // batchSizeDefault=batchSizeMin=1000: lower=90000, upper=91000, currentScn=90500 => caught up
        // decrementBatchSize checks 1000 > 1000 (false) => no-op, batchSize stays 1000
        // metrics.setBatchSize(1000) is still called with the unchanged minimum
        final OracleConnectorConfig config = createConnectorConfig(
                DEFAULT_BATCH_MIN, DEFAULT_BATCH_MIN, DEFAULT_BATCH_MAX, DEFAULT_BATCH_INCREMENT,
                LINEAR, Duration.ZERO, DEFAULT_REDO_ADJUSTMENT, false);
        final LogMinerRangeContext context = setupMocks(config);

        context.calculateUpperBoundary(Scn.valueOf(90000L), Scn.valueOf(500L), Scn.valueOf(90500L));

        // Once from the constructor (initializing default), once from the caught-up path (no-op decrement)
        verify(metrics, times(2)).setBatchSize(1000);
    }

    @Test
    @FixFor("dbz#1713")
    public void shouldNotIncrementBatchSizeAboveMaximum() throws Exception {
        // batchSizeDefault=80000, batchSizeIncrement=100000 (oversized) to verify capping at batchSizeMax=100000
        // Call 1: behind => incrementBatchSize: min(80000+100000, 100000)=100000
        // Call 2: at max with previousUpperBounds set, LINEAR ticks=1: effectiveBatch = 100000+(100000*1) = 200000
        // If batchSize were incorrectly 180000: effectiveBatch would be 180000+(100000*1) = 280000
        final OracleConnectorConfig config = createConnectorConfig(
                80000, DEFAULT_BATCH_MIN, 100000, 100000,
                LINEAR, Duration.ZERO, DEFAULT_REDO_ADJUSTMENT, false);
        final LogMinerRangeContext context = setupMocks(config);

        context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(500000L));
        context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(500000L));

        verify(metrics).setBatchSize(200000);
    }

    @Test
    @FixFor("dbz#1713")
    public void shouldResetTicksOnCatchUp() throws Exception {
        // Start at max batch size to enable window scale from the second call onwards.
        // After a catch-up call, ticks resets and the next window-scale call starts from ticks=1.
        final OracleConnectorConfig config = createConnectorConfig(
                DEFAULT_BATCH_MAX, DEFAULT_BATCH_MIN, DEFAULT_BATCH_MAX, DEFAULT_BATCH_INCREMENT,
                LINEAR, Duration.ZERO, DEFAULT_REDO_ADJUSTMENT, false);
        final LogMinerRangeContext context = setupMocks(config);

        // Call 1: previousUpperBounds=NULL => no window scale => returns 101000, sets previousUpperBounds
        context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(9999999L));

        // Call 2: previousUpperBounds set, LINEAR ticks=>1 => returns 1000+200000=201000
        context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(9999999L));

        // Call 3: caught up (upper=300000 >= currentScn=205000) => ticks reset to 0
        when(connection.getRedoThreadState()).thenReturn(createOpenRedoThreadState(Scn.valueOf(210000L)));
        context.calculateUpperBoundary(Scn.valueOf(200000L), Scn.valueOf(500L), Scn.valueOf(205000L));

        // Call 4: ticks was reset; previousUpperBounds=205000, isUsingMaxBatchSize=true, LINEAR
        // ticks starts from 0 => increments to 1 => effectiveBatch=100000+(100000*1)=200000
        when(connection.getRedoThreadState()).thenReturn(createOpenRedoThreadState(Scn.valueOf(Long.MAX_VALUE / 2)));
        final Scn result4 = context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(9999999L));
        assertThat(result4).isEqualTo(Scn.valueOf(201000L));
    }

    // -------------------------------------------------------------------------
    // Group 3: Window scale strategies
    // -------------------------------------------------------------------------

    @Test
    @FixFor("dbz#1713")
    public void shouldNotApplyWindowScaleOnFirstCallWhenPreviousBoundsIsNull() throws Exception {
        // batchSizeDefault=batchSizeMax so isUsingMaxBatchSize()=true from the start,
        // but previousUpperBounds is NULL => window scale is NOT applied on the first call.
        // Without window scale: upper = lower + batchSize = 1000 + 100000 = 101000
        // With LINEAR scale (ticks=1): would return 201000 instead
        final OracleConnectorConfig config = createConnectorConfig(
                DEFAULT_BATCH_MAX, DEFAULT_BATCH_MIN, DEFAULT_BATCH_MAX, DEFAULT_BATCH_INCREMENT,
                LINEAR, Duration.ZERO, DEFAULT_REDO_ADJUSTMENT, false);
        final LogMinerRangeContext context = setupMocks(config);

        final Scn result = context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(9999999L));
        assertThat(result).isEqualTo(Scn.valueOf(101000L));
    }

    @Test
    @FixFor("dbz#1713")
    public void shouldJumpToCurrentWriteWithCurrentWriteWindowScale() throws Exception {
        // Call 1: previousUpperBounds=NULL => no window scale => returns 101000, sets previousUpperBounds
        // Call 2: previousUpperBounds set, CURRENT_WRITE => upper jumps directly to currentScn=500000
        final OracleConnectorConfig config = createConnectorConfig(
                DEFAULT_BATCH_MAX, DEFAULT_BATCH_MIN, DEFAULT_BATCH_MAX, DEFAULT_BATCH_INCREMENT,
                CURRENT_WRITE, Duration.ZERO, DEFAULT_REDO_ADJUSTMENT, false);
        final LogMinerRangeContext context = setupMocks(config);

        context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(500000L));

        final Scn result = context.calculateUpperBoundary(Scn.valueOf(101001L), Scn.valueOf(500L), Scn.valueOf(500000L));
        assertThat(result).isEqualTo(Scn.valueOf(500000L));
    }

    @Test
    @FixFor("dbz#1713")
    public void shouldScaleLinearlyWithLinearWindowScale() throws Exception {
        // Call 1: previousUpperBounds=NULL => no window scale => returns 101000
        // Call 2: LINEAR ticks=>1, effectiveBatch = batchSize + (batchSizeMax * 1) = 100000+100000=200000
        // upper = 101001 + 200000 = 301001
        final OracleConnectorConfig config = createConnectorConfig(
                DEFAULT_BATCH_MAX, DEFAULT_BATCH_MIN, DEFAULT_BATCH_MAX, DEFAULT_BATCH_INCREMENT,
                LINEAR, Duration.ZERO, DEFAULT_REDO_ADJUSTMENT, false);
        final LogMinerRangeContext context = setupMocks(config);

        context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(9999999L));

        final Scn result = context.calculateUpperBoundary(Scn.valueOf(101001L), Scn.valueOf(500L), Scn.valueOf(9999999L));
        assertThat(result).isEqualTo(Scn.valueOf(301001L));
    }

    @Test
    @FixFor("dbz#1713")
    public void shouldScaleExponentiallyWithExponentialWindowScale() throws Exception {
        // Call 1: previousUpperBounds=NULL => no window scale => returns 101000
        // Call 2: EXPONENTIAL ticks=>1, effectiveBatch = batchSizeMax << 1 = 100000*2 = 200000
        // upper = 101001 + 200000 = 301001
        final OracleConnectorConfig config = createConnectorConfig(
                DEFAULT_BATCH_MAX, DEFAULT_BATCH_MIN, DEFAULT_BATCH_MAX, DEFAULT_BATCH_INCREMENT,
                EXPONENTIAL, Duration.ZERO, DEFAULT_REDO_ADJUSTMENT, false);
        final LogMinerRangeContext context = setupMocks(config);

        context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(9999999L));

        final Scn result = context.calculateUpperBoundary(Scn.valueOf(101001L), Scn.valueOf(500L), Scn.valueOf(9999999L));
        assertThat(result).isEqualTo(Scn.valueOf(301001L));
    }

    @Test
    @FixFor("dbz#1713")
    public void shouldCapLinearScaleToMaxReadScnWhenExceeded() throws Exception {
        // Call 1: lower=1000, currentScn=9999999 => sets previousUpperBounds=101000
        // Call 2: lower=1000, currentScn=180000 (small enough that LINEAR scale overshoots)
        // initial upper = 101000 < 180000 => else branch
        // LINEAR ticks=>1, effectiveBatch=200000, effective upper=201000 >= 180000 => capped to 180000
        final OracleConnectorConfig config = createConnectorConfig(
                DEFAULT_BATCH_MAX, DEFAULT_BATCH_MIN, DEFAULT_BATCH_MAX, DEFAULT_BATCH_INCREMENT,
                LINEAR, Duration.ZERO, DEFAULT_REDO_ADJUSTMENT, false);
        final LogMinerRangeContext context = setupMocks(config);

        context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(9999999L));

        final Scn result = context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(180000L));
        assertThat(result).isEqualTo(Scn.valueOf(180000L));
    }

    @Test
    @FixFor("dbz#1713")
    public void shouldCapExponentialScaleToMaxReadScnWhenExceeded() throws Exception {
        // Call 1: lower=1000, currentScn=9999999 => sets previousUpperBounds=101000
        // Call 2: lower=1000, currentScn=180000
        // EXPONENTIAL ticks=>1, effectiveBatch=200000, effective upper=201000 >= 180000 => capped
        final OracleConnectorConfig config = createConnectorConfig(
                DEFAULT_BATCH_MAX, DEFAULT_BATCH_MIN, DEFAULT_BATCH_MAX, DEFAULT_BATCH_INCREMENT,
                EXPONENTIAL, Duration.ZERO, DEFAULT_REDO_ADJUSTMENT, false);
        final LogMinerRangeContext context = setupMocks(config);

        context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(9999999L));

        final Scn result = context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(180000L));
        assertThat(result).isEqualTo(Scn.valueOf(180000L));
    }

    @Test
    @FixFor("dbz#1713")
    public void shouldIncrementTicksWithEachWindowScaleCalculation() throws Exception {
        // Each successive window-scale call increments ticks by 1, increasing the effective batch size
        final OracleConnectorConfig config = createConnectorConfig(
                DEFAULT_BATCH_MAX, DEFAULT_BATCH_MIN, DEFAULT_BATCH_MAX, DEFAULT_BATCH_INCREMENT,
                LINEAR, Duration.ZERO, DEFAULT_REDO_ADJUSTMENT, false);
        final LogMinerRangeContext context = setupMocks(config);

        // Call 1: no window scale (previousUpperBounds=NULL) => 1000+100000=101000
        final Scn r1 = context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(Long.MAX_VALUE / 2));
        assertThat(r1).isEqualTo(Scn.valueOf(101000L));

        // Call 2: ticks=1, effective=100000+(100000*1)=200000 => 1000+200000=201000
        final Scn r2 = context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(Long.MAX_VALUE / 2));
        assertThat(r2).isEqualTo(Scn.valueOf(201000L));

        // Call 3: ticks=2, effective=100000+(100000*2)=300000 => 301000
        final Scn r3 = context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(Long.MAX_VALUE / 2));
        assertThat(r3).isEqualTo(Scn.valueOf(301000L));

        // Call 4: ticks=3, effective=100000+(100000*3)=400000 => 401000
        final Scn r4 = context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(Long.MAX_VALUE / 2));
        assertThat(r4).isEqualTo(Scn.valueOf(401000L));
    }

    @Test
    @FixFor("dbz#1713")
    public void shouldCapTicksAt30ToPreventOverflow() throws Exception {
        // After 30 window-scale calls ticks is capped at 30; subsequent calls return the same result
        final OracleConnectorConfig config = createConnectorConfig(
                DEFAULT_BATCH_MAX, DEFAULT_BATCH_MIN, DEFAULT_BATCH_MAX, DEFAULT_BATCH_INCREMENT,
                LINEAR, Duration.ZERO, DEFAULT_REDO_ADJUSTMENT, false);
        final LogMinerRangeContext context = setupMocks(config);

        // Call 1: sets previousUpperBounds (no window scale, ticks stays 0)
        context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(Long.MAX_VALUE / 2));

        // Window-scale calls 1–29: ticks advances from 0 to 29
        for (int i = 1; i <= 29; i++) {
            context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(Long.MAX_VALUE / 2));
        }

        // Call at ticks=29=>30 (reaches cap): effective = 100000 + (100000 * 30) = 3100000, upper = 3101000
        final Scn resultAtCap = context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(Long.MAX_VALUE / 2));

        // Call at ticks=30=>30 (capped, stays at 30): same effective batch size, same result
        final Scn resultPastCap = context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(Long.MAX_VALUE / 2));

        assertThat(resultAtCap).isEqualTo(resultPastCap);
        assertThat(resultAtCap).isEqualTo(Scn.valueOf(3101000L));
    }

    // -------------------------------------------------------------------------
    // Group 4: Redo thread validation
    // -------------------------------------------------------------------------

    @Test
    @FixFor("dbz#1713")
    public void shouldReturnNullWhenNoOpenRedoThreadsAvailable() throws Exception {
        final LogMinerRangeContext context = setupMocks(defaultConnectorConfig());
        when(connection.getRedoThreadState()).thenReturn(createEmptyRedoThreadState());

        final Scn result = context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(100000L));
        assertThat(result).isEqualTo(Scn.NULL);
    }

    @Test
    @FixFor("dbz#1713")
    public void shouldCapUpperBoundaryToMinimumOpenRedoThreadScn() throws Exception {
        // upper=21000, lastRedoScn=15000, adjustment=1 => effective=14999
        // 14999 < 21000 => cap; 14999 > 1000 (lower) => valid => returns 14999
        final LogMinerRangeContext context = setupMocks(defaultConnectorConfig());
        when(connection.getRedoThreadState()).thenReturn(createOpenRedoThreadState(Scn.valueOf(15000L)));

        final Scn result = context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(100000L));
        assertThat(result).isEqualTo(Scn.valueOf(14999L));
    }

    @Test
    @FixFor("dbz#1713")
    public void shouldReturnNullWhenRedoThreadScnIsBelowLowerBoundary() throws Exception {
        // lower=10000, initial upper=30000 (10000+20000)
        // lastRedoScn=9000, adjustment=1 => effective=8999
        // 8999 < 30000 => cap; 8999 < 10000 (lower) => startup corner case => returns NULL
        final LogMinerRangeContext context = setupMocks(defaultConnectorConfig());
        when(connection.getRedoThreadState()).thenReturn(createOpenRedoThreadState(Scn.valueOf(9000L)));

        final Scn result = context.calculateUpperBoundary(Scn.valueOf(10000L), Scn.valueOf(500L), Scn.valueOf(100000L));
        assertThat(result).isEqualTo(Scn.NULL);
    }

    @Test
    @FixFor("dbz#1713")
    public void shouldApplyConfigurableRedoThreadScnAdjustment() throws Exception {
        // redoAdjustment=5: lastRedoScn=15000, adjustment=5 => effective=14995
        // 14995 < 21000 => cap; 14995 > 1000 (lower) => valid => returns 14995
        final OracleConnectorConfig config = createConnectorConfig(
                DEFAULT_BATCH_SIZE, DEFAULT_BATCH_MIN, DEFAULT_BATCH_MAX, DEFAULT_BATCH_INCREMENT,
                LINEAR, Duration.ZERO, 5, false);
        final LogMinerRangeContext context = setupMocks(config);
        when(connection.getRedoThreadState()).thenReturn(createOpenRedoThreadState(Scn.valueOf(15000L)));

        final Scn result = context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(100000L));
        assertThat(result).isEqualTo(Scn.valueOf(14995L));
    }

    @Test
    @FixFor("dbz#1713")
    public void shouldNotCapUpperBoundaryWhenRedoThreadScnIsAboveUpper() throws Exception {
        // upper=21000, lastRedoScn=999999 => effective=999998 > 21000 => no cap => returns 21000
        final LogMinerRangeContext context = setupMocks(defaultConnectorConfig());
        when(connection.getRedoThreadState()).thenReturn(createOpenRedoThreadState(Scn.valueOf(999999L)));

        final Scn result = context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(100000L));
        assertThat(result).isEqualTo(Scn.valueOf(21000L));
    }

    // -------------------------------------------------------------------------
    // Group 5: SCN Deviation
    // -------------------------------------------------------------------------

    @Test
    @FixFor("dbz#1713")
    public void shouldIgnoreDeviationInArchiveLogOnlyMode() throws Exception {
        // Even when deviation is configured, archive-only mode bypasses the deviation calculation entirely
        final OracleConnectorConfig config = createConnectorConfig(
                DEFAULT_BATCH_SIZE, DEFAULT_BATCH_MIN, DEFAULT_BATCH_MAX, DEFAULT_BATCH_INCREMENT,
                LINEAR, Duration.ofMillis(5000), DEFAULT_REDO_ADJUSTMENT, true);
        final LogMinerRangeContext context = setupMocks(config);

        context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(1500L), Scn.valueOf(999999L));

        verify(connection, never()).getScnAdjustedByTime(any(), any());
    }

    @Test
    @FixFor("dbz#1713")
    public void shouldApplyDeviationWhenUpperBoundsAlreadySatisfiesDeviation() throws Exception {
        // The upper boundary is already 10s behind current time, which exceeds the 5s deviation.
        // In this case the deviation is satisfied and getScnAdjustedByTime is not called.
        final OracleConnectorConfig config = createConnectorConfig(
                DEFAULT_BATCH_SIZE, DEFAULT_BATCH_MIN, DEFAULT_BATCH_MAX, DEFAULT_BATCH_INCREMENT,
                LINEAR, Duration.ofMillis(5000), DEFAULT_REDO_ADJUSTMENT, false);
        final LogMinerRangeContext context = setupMocks(config);

        final Scn currentScn = Scn.valueOf(50000L);
        final Instant upperInstant = Instant.now().minusSeconds(10);
        final Instant currentInstant = Instant.now();

        when(connection.getCurrentScn()).thenReturn(currentScn);
        // First call: getScnToTimestamp(currentScn), second call: getScnToTimestamp(upperBoundary=21000)
        when(connection.getScnToTimestamp(any())).thenReturn(Optional.of(currentInstant)).thenReturn(Optional.of(upperInstant));

        final Scn result = context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(100000L));
        assertThat(result).isEqualTo(Scn.valueOf(21000L));
        verify(connection, never()).getScnAdjustedByTime(any(), any());
    }

    @Test
    @FixFor("dbz#1713")
    public void shouldCalculateDeviatedUpperBoundaryFromJdbcCall() throws Exception {
        // The upper boundary is only 2s behind, less than the 5s deviation.
        // getScnAdjustedByTime is called and returns a deviated SCN (15000) that is above lower boundary.
        final Duration deviation = Duration.ofMillis(5000);
        final OracleConnectorConfig config = createConnectorConfig(
                DEFAULT_BATCH_SIZE, DEFAULT_BATCH_MIN, DEFAULT_BATCH_MAX, DEFAULT_BATCH_INCREMENT,
                LINEAR, deviation, DEFAULT_REDO_ADJUSTMENT, false);
        final LogMinerRangeContext context = setupMocks(config);

        final Scn currentScn = Scn.valueOf(50000L);
        final Scn deviatedScn = Scn.valueOf(15000L);
        final Instant upperInstant = Instant.now().minusSeconds(2);
        final Instant currentInstant = Instant.now();

        when(connection.getCurrentScn()).thenReturn(currentScn);
        when(connection.getScnToTimestamp(any())).thenReturn(Optional.of(currentInstant)).thenReturn(Optional.of(upperInstant));
        when(connection.getScnAdjustedByTime(any(), any())).thenReturn(deviatedScn);

        // deviatedScn=15000 > lower=1000 => returns deviated SCN
        final Scn result = context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(100000L));
        assertThat(result).isEqualTo(Scn.valueOf(15000L));
    }

    @Test
    @FixFor("dbz#1713")
    public void shouldContinueWithOriginalBoundaryWhenDeviationCalculationFails() throws Exception {
        // When getScnToTimestamp throws a SQLException, getDeviatedMaxScn returns Optional.empty().
        // calculateDeviatedEndScn treats this as "outside undo space" and falls back to the original upper boundary.
        final OracleConnectorConfig config = createConnectorConfig(
                DEFAULT_BATCH_SIZE, DEFAULT_BATCH_MIN, DEFAULT_BATCH_MAX, DEFAULT_BATCH_INCREMENT,
                LINEAR, Duration.ofMillis(5000), DEFAULT_REDO_ADJUSTMENT, false);
        final LogMinerRangeContext context = setupMocks(config);

        when(connection.getCurrentScn()).thenReturn(Scn.valueOf(50000L));
        when(connection.getScnToTimestamp(any())).thenThrow(new SQLException("simulated error"));

        // Falls back to the original upper boundary
        final Scn result = context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(100000L));
        assertThat(result).isEqualTo(Scn.valueOf(21000L));
    }

    @Test
    @FixFor("dbz#1713")
    public void shouldRetainOriginalBoundaryWhenDeviatedScnIsOutsideMiningRange() throws Exception {
        // The deviated SCN (500) is at or below the lower boundary (1000).
        // calculateDeviatedEndScn returns Optional.empty(), so calculateUpperBoundary keeps the original upper boundary.
        final Duration deviation = Duration.ofMillis(5000);
        final OracleConnectorConfig config = createConnectorConfig(
                DEFAULT_BATCH_SIZE, DEFAULT_BATCH_MIN, DEFAULT_BATCH_MAX, DEFAULT_BATCH_INCREMENT,
                LINEAR, deviation, DEFAULT_REDO_ADJUSTMENT, false);
        final LogMinerRangeContext context = setupMocks(config);

        final Instant upperInstant = Instant.now().minusSeconds(2);
        final Instant currentInstant = Instant.now();
        // Deviated SCN falls below the lower boundary (1000), triggering the "outside mining range" path
        final Scn deviatedScn = Scn.valueOf(500L);

        when(connection.getCurrentScn()).thenReturn(Scn.valueOf(50000L));
        when(connection.getScnToTimestamp(any())).thenReturn(Optional.of(currentInstant)).thenReturn(Optional.of(upperInstant));
        when(connection.getScnAdjustedByTime(any(), any())).thenReturn(deviatedScn);

        final Scn result = context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(100000L));
        assertThat(result).isEqualTo(Scn.valueOf(21000L));
    }

    // -------------------------------------------------------------------------
    // Group 6: Special sanity checks
    // -------------------------------------------------------------------------

    @Test
    @FixFor("dbz#1713")
    public void shouldReturnNullWhenFinalUpperBoundaryEqualsLowerBoundary() throws Exception {
        // lower=1000, initial upper=21000
        // lastRedoScn=1001, adjustment=1 => effective=1000 (= lower boundary)
        // 1000 < 21000 => cap; 1000 == 1000 (lower), not less than => upperBoundary = 1000
        // Final check: 1000 <= 1000 => returns NULL
        final LogMinerRangeContext context = setupMocks(defaultConnectorConfig());
        when(connection.getRedoThreadState()).thenReturn(createOpenRedoThreadState(Scn.valueOf(1001L)));

        final Scn result = context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(100000L));
        assertThat(result).isEqualTo(Scn.NULL);
    }

    @Test
    @FixFor("dbz#1713")
    public void shouldStorePreviousUpperBoundsAfterSuccessfulCall() throws Exception {
        // After a successful call, previousUpperBounds is stored.
        // This causes window scale to activate on the second call (when batchSize == batchSizeMax).
        final OracleConnectorConfig config = createConnectorConfig(
                DEFAULT_BATCH_MAX, DEFAULT_BATCH_MIN, DEFAULT_BATCH_MAX, DEFAULT_BATCH_INCREMENT,
                LINEAR, Duration.ZERO, DEFAULT_REDO_ADJUSTMENT, false);
        final LogMinerRangeContext context = setupMocks(config);

        // Call 1: previousUpperBounds=NULL => window scale NOT applied => 1000+100000=101000
        final Scn result1 = context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(9999999L));
        assertThat(result1).isEqualTo(Scn.valueOf(101000L));

        // Call 2: previousUpperBounds=101000 (set after call 1) => window scale IS applied
        // ticks=>1, effective=200000 => 1000+200000=201000 (not 101000 again)
        final Scn result2 = context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(9999999L));
        assertThat(result2).isEqualTo(Scn.valueOf(201000L));
    }

    // -------------------------------------------------------------------------
    // Group 7: Sleep time management
    // -------------------------------------------------------------------------

    @Test
    @FixFor("dbz#1713")
    public void shouldIncrementSleepTimeWhenCaughtUp() throws Exception {
        // Caught up => incrementSleepTime(): 1000 + 500 = 1500
        final LogMinerRangeContext context = setupMocks(defaultConnectorConfig());

        context.calculateUpperBoundary(Scn.valueOf(90000L), Scn.valueOf(500L), Scn.valueOf(95000L));

        verify(metrics).setSleepTime(DEFAULT_SLEEP_TIME_MS + DEFAULT_SLEEP_TIME_INCREMENT_MS);
    }

    @Test
    @FixFor("dbz#1713")
    public void shouldDecrementSleepTimeWhenBehindNormally() throws Exception {
        // Behind, not at max batch size => decrementSleepTime(): 1000 - 500 = 500
        final LogMinerRangeContext context = setupMocks(defaultConnectorConfig());

        context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(100000L));

        verify(metrics).setSleepTime(DEFAULT_SLEEP_TIME_MS - DEFAULT_SLEEP_TIME_INCREMENT_MS);
    }

    @Test
    @FixFor("dbz#1713")
    public void shouldNotChangeSleepTimeForWindowScaleLeap() throws Exception {
        // Behind with window scale leap (CURRENT_WRITE) => no sleep time change on the leap call
        final OracleConnectorConfig config = createConnectorConfig(
                DEFAULT_BATCH_MAX, DEFAULT_BATCH_MIN, DEFAULT_BATCH_MAX, DEFAULT_BATCH_INCREMENT,
                CURRENT_WRITE, Duration.ZERO, DEFAULT_REDO_ADJUSTMENT, false);
        final LogMinerRangeContext context = setupMocks(config);

        // Call 1: no previousUpperBounds => normal behind path => decrementSleepTime() called (1000=>500)
        context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(500000L));

        // Call 2: previousUpperBounds set, CURRENT_WRITE leap => NO sleep time change
        context.calculateUpperBoundary(Scn.valueOf(101001L), Scn.valueOf(500L), Scn.valueOf(500000L));

        // setSleepTime(500) called exactly once (from call 1 only, not from call 2)
        verify(metrics, times(1)).setSleepTime(DEFAULT_SLEEP_TIME_MS - DEFAULT_SLEEP_TIME_INCREMENT_MS);
    }

    @Test
    @FixFor("dbz#1713")
    public void shouldNotIncrementSleepTimeBeyondMaximum() throws Exception {
        // sleepTimeMax=1500ms, increment=1000ms: first increment hits max, second is a no-op
        final OracleConnectorConfig config = defaultConnectorConfig();
        when(config.getLogMiningSleepTimeMax()).thenReturn(Duration.ofMillis(1500));
        when(config.getLogMiningSleepTimeIncrement()).thenReturn(Duration.ofMillis(1000));

        final LogMinerRangeContext context = setupMocks(config);

        // Call 1: caught up => sleepTime = min(1000+1000, 1500) = 1500 => metrics.setSleepTime(1500)
        context.calculateUpperBoundary(Scn.valueOf(90000L), Scn.valueOf(500L), Scn.valueOf(95000L));

        // Call 2: caught up again => sleepTime=1500=max => incrementSleepTime() is a no-op
        context.calculateUpperBoundary(Scn.valueOf(90000L), Scn.valueOf(500L), Scn.valueOf(95000L));

        verify(metrics, times(1)).setSleepTime(1500L);
    }

    @Test
    @FixFor("dbz#1713")
    public void shouldNotDecrementSleepTimeBelowMinimum() throws Exception {
        // sleepTimeMin=500ms, increment=1000ms: first decrement hits min, second is a no-op
        final OracleConnectorConfig config = defaultConnectorConfig();
        when(config.getLogMiningSleepTimeMin()).thenReturn(Duration.ofMillis(500));
        when(config.getLogMiningSleepTimeIncrement()).thenReturn(Duration.ofMillis(1000));
        final LogMinerRangeContext context = setupMocks(config);

        // Call 1: behind => sleepTime = max(1000-1000, 500) = 500 => metrics.setSleepTime(500)
        context.calculateUpperBoundary(Scn.valueOf(1000L), Scn.valueOf(500L), Scn.valueOf(100000L));

        // Call 2: behind again => sleepTime=500=min => decrementSleepTime() is a no-op
        context.calculateUpperBoundary(Scn.valueOf(21001L), Scn.valueOf(500L), Scn.valueOf(100000L));

        verify(metrics, times(1)).setSleepTime(500L);
    }

    private LogMinerRangeContext setupMocks(OracleConnectorConfig config) throws SQLException {
        connectorConfig = config;
        connection = mock(OracleConnection.class);
        metrics = mock(LogMinerStreamingChangeEventSourceMetrics.class);
        // Default redo thread with a very high SCN so it does not cap upper boundaries in most tests
        when(connection.getRedoThreadState()).thenReturn(createOpenRedoThreadState(Scn.valueOf(Long.MAX_VALUE / 2)));

        return new LogMinerRangeContext(connectorConfig, connection, metrics);
    }

    @SuppressWarnings("SameParameterValue")
    private OracleConnectorConfig createConnectorConfig(int batchSizeDefault, int batchSizeMin, int batchSizeMax,
                                                        int batchSizeIncrement,
                                                        LogMiningBatchSizeWindowScale windowScale,
                                                        Duration deviation, int redoAdjustment, boolean archiveOnlyMode) {
        OracleConnectorConfig config = mock(OracleConnectorConfig.class);
        when(config.getLogMiningBatchSizeDefault()).thenReturn(batchSizeDefault);
        when(config.getLogMiningBatchSizeMin()).thenReturn(batchSizeMin);
        when(config.getLogMiningBatchSizeMax()).thenReturn(batchSizeMax);
        when(config.getLogMiningBatchSizeIncrement()).thenReturn(batchSizeIncrement);
        when(config.getLogMiningBatchSizeWindowScale()).thenReturn(windowScale);
        when(config.getLogMiningMaxScnDeviation()).thenReturn(deviation);
        when(config.getLogMiningRedoThreadScnAdjustment()).thenReturn(redoAdjustment);
        when(config.isArchiveLogOnlyMode()).thenReturn(archiveOnlyMode);
        when(config.getLogMiningSleepTimeDefault()).thenReturn(Duration.ofMillis(DEFAULT_SLEEP_TIME_MS));
        when(config.getLogMiningSleepTimeMin()).thenReturn(Duration.ofMillis(DEFAULT_SLEEP_TIME_MIN_MS));
        when(config.getLogMiningSleepTimeMax()).thenReturn(Duration.ofMillis(DEFAULT_SLEEP_TIME_MAX_MS));
        when(config.getLogMiningSleepTimeIncrement()).thenReturn(Duration.ofMillis(DEFAULT_SLEEP_TIME_INCREMENT_MS));
        return config;
    }

    private OracleConnectorConfig defaultConnectorConfig() {
        return createConnectorConfig(DEFAULT_BATCH_SIZE, DEFAULT_BATCH_MIN, DEFAULT_BATCH_MAX,
                DEFAULT_BATCH_INCREMENT, LINEAR, Duration.ZERO, DEFAULT_REDO_ADJUSTMENT, false);
    }

    private RedoThreadState createOpenRedoThreadState(Scn lastRedoScn) {
        return RedoThreadState.builder()
                .thread()
                .threadId(1)
                .status("OPEN")
                .lastRedoScn(lastRedoScn)
                .build()
                .build();
    }

    private RedoThreadState createEmptyRedoThreadState() {
        return RedoThreadState.builder().build();
    }
}
