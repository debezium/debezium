/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.RedoThreadState;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.LogFileCollector.LogFilesResult;
import io.debezium.connector.oracle.logminer.LogFileSessionSelector.SessionLogSelection;
import io.debezium.doc.FixFor;

/**
 * Unit tests for {@link CappedLogFileSessionSelector}.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER)
public class CappedLogFileSessionSelectorTest {

    private static final long ONE_GB = 1024L * 1024L * 1024L;
    private static final Scn UPPER_BOUNDS = Scn.valueOf(1000);

    // threshold: 2 logs * 1 GB = 2 GB per thread
    private final CappedLogFileSessionSelector selector = new CappedLogFileSessionSelector(2, ONE_GB);

    @Test
    @FixFor("dbz#1713")
    void testSingleThreadAllLogsWithinThresholdAllLogsReturned() {
        // arc1(500MB) + arc2(500MB) + redo(500MB): total 1.5 GB < 2 GB, all fit
        // last log is online redo => allThreadsMineOnline=true => all original logs, bounds unchanged
        List<LogFile> logs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB / 2),
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB / 2),
                createRedoLog("redo1.log", 300, 3, 1));

        SessionLogSelection result = selector.selectLogsForSession(
                new LogFilesResult(logs, singleThreadOpen()), UPPER_BOUNDS);

        assertThat(result.logFiles()).containsExactlyInAnyOrderElementsOf(logs);
        assertThat(result.effectiveUpperBounds()).isEqualTo(UPPER_BOUNDS);
    }

    @Test
    @FixFor("dbz#1713")
    void testSingleThreadExceedsThresholdLastCappedIsArchiveCappedAndTightenedBounds() {
        // arc1(1GB) hits 1 GB, arc2(1GB) hits 2 GB => threshold met, loop breaks after arc2
        // arc3 and redo excluded; last capped = arc2(archive, nextScn=300)
        // allThreadsMineOnline=false, effectiveUpperBounds tightened to 300
        List<LogFile> logs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createArchiveLog("arc3.log", 300, 400, 3, 1, ONE_GB),
                createRedoLog("redo1.log", 400, 4, 1));

        SessionLogSelection result = selector.selectLogsForSession(
                new LogFilesResult(logs, singleThreadOpen()), UPPER_BOUNDS);

        assertThat(result.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc1.log", "arc2.log");
        assertThat(result.effectiveUpperBounds()).isEqualTo(Scn.valueOf(300));
    }

    @Test
    @FixFor("dbz#1713")
    void testSingleThreadThresholdReachedWithOnlineRedoAsLastAllLogsReturnedOriginalBounds() {
        // arc1(1GB) + redo(1GB): threshold reached with redo as the last log
        // last is online redo => allThreadsMineOnline=true => all original logs, bounds unchanged
        List<LogFile> logs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createRedoLog("redo1.log", 200, 2, 1));

        SessionLogSelection result = selector.selectLogsForSession(
                new LogFilesResult(logs, singleThreadOpen()), UPPER_BOUNDS);

        assertThat(result.logFiles()).containsExactlyInAnyOrderElementsOf(logs);
        assertThat(result.effectiveUpperBounds()).isEqualTo(UPPER_BOUNDS);
    }

    @Test
    @FixFor("dbz#1713")
    void testSingleThreadArchiveBoundsNotTightenedWhenAlreadyBelowOriginal() {
        // arc1(1GB) + arc2(1GB): capped at arc2(nextScn=300)
        // original upper bounds is already 250 (< arc2.nextScn=300), so bounds stays at 250
        List<LogFile> logs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createRedoLog("redo1.log", 300, 3, 1));

        SessionLogSelection result = selector.selectLogsForSession(
                new LogFilesResult(logs, singleThreadOpen()), Scn.valueOf(250));

        assertThat(result.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc1.log", "arc2.log");
        assertThat(result.effectiveUpperBounds()).isEqualTo(Scn.valueOf(250));
    }

    @Test
    @FixFor("dbz#1713")
    void testTwoThreadsThread1HasArchiveLastBoundsTightenedToThread1() {
        // Thread 1: arc1(1GB) + arc2(1GB, nextScn=300) => capped, last=arc2(archive)
        // Thread 2: arc1(1GB) + redo(1GB) => capped at redo, last=redo(online)
        // Thread 1 sets allThreadsMineOnline=false and effectiveUpperBounds=300
        List<LogFile> logs = List.of(
                createArchiveLog("t1_arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("t1_arc2.log", 200, 300, 2, 1, ONE_GB),
                createArchiveLog("t1_arc3.log", 300, 400, 3, 1, ONE_GB),
                createRedoLog("t1_redo.log", 400, 4, 1),
                createArchiveLog("t2_arc1.log", 100, 250, 1, 2, ONE_GB),
                createRedoLog("t2_redo.log", 250, 2, 2));

        SessionLogSelection result = selector.selectLogsForSession(
                new LogFilesResult(logs, twoThreadsOpen()), UPPER_BOUNDS);

        assertThat(result.logFiles()).extracting(LogFile::getFileName)
                .containsExactlyInAnyOrder("t1_arc1.log", "t1_arc2.log", "t2_arc1.log", "t2_redo.log");
        assertThat(result.effectiveUpperBounds()).isEqualTo(Scn.valueOf(300));
    }

    @Test
    @FixFor("dbz#1713")
    void testTwoThreadsBothOnlineLastAllLogsReturnedOriginalBounds() {
        // Thread 1: arc1(1GB) + redo => last=redo(online)
        // Thread 2: arc1(1GB) + redo => last=redo(online)
        // allThreadsMineOnline=true => all original logs, bounds unchanged
        List<LogFile> logs = List.of(
                createArchiveLog("t1_arc1.log", 100, 200, 1, 1, ONE_GB),
                createRedoLog("t1_redo.log", 200, 2, 1),
                createArchiveLog("t2_arc1.log", 100, 200, 1, 2, ONE_GB),
                createRedoLog("t2_redo.log", 200, 2, 2));

        SessionLogSelection result = selector.selectLogsForSession(
                new LogFilesResult(logs, twoThreadsOpen()), UPPER_BOUNDS);

        assertThat(result.logFiles()).containsExactlyInAnyOrderElementsOf(logs);
        assertThat(result.effectiveUpperBounds()).isEqualTo(UPPER_BOUNDS);
    }

    @Test
    @FixFor("dbz#1713")
    void testTwoThreadsBothArchiveLastBoundsTightenedToFirstThreadProcessed() {
        // Thread 1: arc1(1GB) + arc2(1GB, nextScn=300) => capped, last=arc2(archive)
        // Thread 2: arc1(1GB) + arc2(1GB, nextScn=350) => capped, last=arc2(archive)
        // Thread 1 sets allThreadsMineOnline=false, effectiveUpperBounds=300
        // Thread 2 is skipped (allThreadsMineOnline already false)
        List<LogFile> logs = List.of(
                createArchiveLog("t1_arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("t1_arc2.log", 200, 300, 2, 1, ONE_GB),
                createRedoLog("t1_redo.log", 300, 3, 1),
                createArchiveLog("t2_arc1.log", 100, 250, 1, 2, ONE_GB),
                createArchiveLog("t2_arc2.log", 250, 350, 2, 2, ONE_GB),
                createRedoLog("t2_redo.log", 350, 3, 2));

        SessionLogSelection result = selector.selectLogsForSession(
                new LogFilesResult(logs, twoThreadsOpen()), UPPER_BOUNDS);

        assertThat(result.logFiles()).extracting(LogFile::getFileName)
                .containsExactlyInAnyOrder("t1_arc1.log", "t1_arc2.log", "t2_arc1.log", "t2_arc2.log");
        assertThat(result.effectiveUpperBounds()).isEqualTo(Scn.valueOf(300));
    }

    @Test
    @FixFor("dbz#1713")
    void testOpenThreadWithNoLogsInResultThrowsException() {
        // Thread 1 is open but no logs exist for it in the result set
        List<LogFile> logs = List.of(
                createArchiveLog("t2_arc1.log", 100, 200, 1, 2, ONE_GB));

        assertThatThrownBy(() -> selector.selectLogsForSession(
                new LogFilesResult(logs, twoThreadsOpen()), UPPER_BOUNDS))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("1");
    }

    @Test
    @FixFor("dbz#1713")
    void testSingleThreadArchiveOnlyWithinThresholdBoundsTightenedToLastArchive() {
        // No online redo present (archive-log-only mode scenario); all archives fit within threshold
        // last log is archive => allThreadsMineOnline=false, bounds tightened to last archive nextScn
        List<LogFile> logs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB / 2),
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB / 2));

        SessionLogSelection result = selector.selectLogsForSession(
                new LogFilesResult(logs, singleThreadOpen()), UPPER_BOUNDS);

        assertThat(result.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc1.log", "arc2.log");
        assertThat(result.effectiveUpperBounds()).isEqualTo(Scn.valueOf(300));
    }

    @Test
    @FixFor("dbz#1713")
    void testClosedThreadWithNoLogsDoesNotThrow() {
        // Thread 2 is CLOSED and has no logs; the null guard is inside isOpen(), so no exception
        // Thread 1 is OPEN and mines online redo => allThreadsMineOnline=true, all logs returned
        List<LogFile> logs = List.of(
                createArchiveLog("t1_arc1.log", 100, 200, 1, 1, ONE_GB / 2),
                createRedoLog("t1_redo.log", 200, 2, 1));

        SessionLogSelection result = selector.selectLogsForSession(
                new LogFilesResult(logs, openAndClosedThread()), UPPER_BOUNDS);

        assertThat(result.logFiles()).containsExactlyInAnyOrderElementsOf(logs);
        assertThat(result.effectiveUpperBounds()).isEqualTo(UPPER_BOUNDS);
    }

    @Test
    @FixFor("dbz#1713")
    void testRepeatedIdenticalCapResultGrowsByOneEachIteration() {
        // Two identical calls with the same log set: second call should produce an expanded cap (3 logs)
        // arc1(1GB) + arc2(1GB) + arc3(1GB) + redo; threshold=2GB, capped at arc1+arc2 on first call
        List<LogFile> logs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createArchiveLog("arc3.log", 300, 400, 3, 1, ONE_GB),
                createRedoLog("redo1.log", 400, 4, 1));
        LogFilesResult result = new LogFilesResult(logs, singleThreadOpen());

        // First call: arc1+arc2 (threshold 2GB met), no previous to compare against
        SessionLogSelection first = selector.selectLogsForSession(result, UPPER_BOUNDS);
        assertThat(first.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc1.log", "arc2.log");

        // Second call: same log set => logsPerRedoThread grows to 3 => arc1+arc2+arc3
        SessionLogSelection second = selector.selectLogsForSession(result, UPPER_BOUNDS);
        assertThat(second.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc1.log", "arc2.log", "arc3.log");
        assertThat(second.effectiveUpperBounds()).isEqualTo(Scn.valueOf(400));
    }

    @Test
    @FixFor("dbz#1713")
    void testChangedCapResultResetsToMinimum() {
        // First call with 3 archives; watermark then advances bringing new logs; count must reset
        List<LogFile> initialLogs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createArchiveLog("arc3.log", 300, 400, 3, 1, ONE_GB),
                createRedoLog("redo1.log", 400, 4, 1));

        // First call: capped at arc1+arc2
        selector.selectLogsForSession(new LogFilesResult(initialLogs, singleThreadOpen()), UPPER_BOUNDS);
        // Second call (same): grows to 3 => arc1+arc2+arc3
        selector.selectLogsForSession(new LogFilesResult(initialLogs, singleThreadOpen()), UPPER_BOUNDS);

        // Third call: new log set (arc4 added, arc1 gone => capped set differs from previous)
        // logsPerRedoThread resets to minimumLogsPerRedoThread=2 => arc2+arc3 (top 400)
        // dbz#2326: the previous call already mined to 400, so the window is extended past the
        // previously mined boundary => arc2+arc3+arc4, reading up to 500
        List<LogFile> advancedLogs = List.of(
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createArchiveLog("arc3.log", 300, 400, 3, 1, ONE_GB),
                createArchiveLog("arc4.log", 400, 500, 4, 1, ONE_GB),
                createRedoLog("redo1.log", 500, 5, 1));

        SessionLogSelection third = selector.selectLogsForSession(
                new LogFilesResult(advancedLogs, singleThreadOpen()), UPPER_BOUNDS);
        assertThat(third.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc2.log", "arc3.log", "arc4.log");
        assertThat(third.effectiveUpperBounds()).isEqualTo(Scn.valueOf(500));
    }

    @Test
    @FixFor("dbz#1713")
    void testRepeatedIdenticalCapResultGrowsMultipleSteps() {
        // Three identical calls: counter grows 2->3->4 across three iterations
        // arc1(1GB)+arc2(1GB)+arc3(1GB)+arc4(1GB)+redo; threshold starts at 2GB
        List<LogFile> logs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createArchiveLog("arc3.log", 300, 400, 3, 1, ONE_GB),
                createArchiveLog("arc4.log", 400, 500, 4, 1, ONE_GB),
                createRedoLog("redo1.log", 500, 5, 1));
        LogFilesResult result = new LogFilesResult(logs, singleThreadOpen());

        // Call 1: no previous, threshold=2GB => arc1+arc2
        selector.selectLogsForSession(result, UPPER_BOUNDS);

        // Call 2: same cap => grow to 3 => arc1+arc2+arc3
        selector.selectLogsForSession(result, UPPER_BOUNDS);

        // Call 3: still same as call-2 result => grow to 4 => arc1+arc2+arc3+arc4
        SessionLogSelection third = selector.selectLogsForSession(result, UPPER_BOUNDS);
        assertThat(third.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc1.log", "arc2.log", "arc3.log", "arc4.log");
        assertThat(third.effectiveUpperBounds()).isEqualTo(Scn.valueOf(500));
    }

    @Test
    @FixFor("dbz#1713")
    void testChangedCapResultAtMinimumNoRecompute() {
        // Log set changes on second call but logsPerRedoThread was never incremented (still at minimum).
        // The else-if branch (logsPerRedoThread > minimum) is not entered, so no recompute happens.
        List<LogFile> initialLogs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createArchiveLog("arc3.log", 300, 400, 3, 1, ONE_GB),
                createRedoLog("redo1.log", 400, 4, 1));

        // Call 1: capped at arc1+arc2 (threshold 2GB)
        selector.selectLogsForSession(new LogFilesResult(initialLogs, singleThreadOpen()), UPPER_BOUNDS);

        // Call 2: new log set (arc1 gone, arc4 added); cap differs from previous but counter was never grown
        List<LogFile> advancedLogs = List.of(
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createArchiveLog("arc3.log", 300, 400, 3, 1, ONE_GB),
                createArchiveLog("arc4.log", 400, 500, 4, 1, ONE_GB),
                createRedoLog("redo1.log", 500, 5, 1));

        SessionLogSelection second = selector.selectLogsForSession(
                new LogFilesResult(advancedLogs, singleThreadOpen()), UPPER_BOUNDS);
        // logsPerRedoThread stays at minimum (2); initial compute at 2GB threshold is used as-is
        assertThat(second.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc2.log", "arc3.log");
        assertThat(second.effectiveUpperBounds()).isEqualTo(Scn.valueOf(400));
    }

    @Test
    @FixFor("dbz#1713")
    void testReGrowthAfterReset() {
        // Grow -> reset -> then re-grow from minimum on subsequent identical iterations
        List<LogFile> initialLogs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createArchiveLog("arc3.log", 300, 400, 3, 1, ONE_GB),
                createRedoLog("redo1.log", 400, 4, 1));
        LogFilesResult initialResult = new LogFilesResult(initialLogs, singleThreadOpen());

        // Call 1: capped at arc1+arc2
        selector.selectLogsForSession(initialResult, UPPER_BOUNDS);
        // Call 2: same => grow to 3 => arc1+arc2+arc3
        selector.selectLogsForSession(initialResult, UPPER_BOUNDS);

        // Call 3: new logs; capped at arc2+arc3 (top 400), extended past the previously mined
        // boundary (400) => arc2+arc3+arc4, reading up to 500
        List<LogFile> advancedLogs = List.of(
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createArchiveLog("arc3.log", 300, 400, 3, 1, ONE_GB),
                createArchiveLog("arc4.log", 400, 500, 4, 1, ONE_GB),
                createRedoLog("redo1.log", 500, 5, 1));
        LogFilesResult advancedResult = new LogFilesResult(advancedLogs, singleThreadOpen());
        selector.selectLogsForSession(advancedResult, UPPER_BOUNDS);

        // Call 4 (dbz#2326): advancing past the previously mined boundary (500) requires the
        // online redo log; the thread now mines online, so all logs are used with the original
        // upper boundary rather than re-reading only the already-mined archive range
        SessionLogSelection fourth = selector.selectLogsForSession(advancedResult, UPPER_BOUNDS);
        assertThat(fourth.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc2.log", "arc3.log", "arc4.log", "redo1.log");
        assertThat(fourth.effectiveUpperBounds()).isEqualTo(UPPER_BOUNDS);
    }

    @Test
    @FixFor("dbz#1713")
    void testTwoThreadsBothArchiveLastBoundsTightenedToSmallestNextScn() {
        // Thread 1: arc1(1GB)+arc2(1GB, nextScn=300) => capped, last=arc2(archive)
        // Thread 2: arc1(1GB)+arc2(1GB, nextScn=200) => capped, last=arc2(archive)
        // Both threads end on archive; effectiveUpperBounds must be the minimum across both (200, not 300)
        List<LogFile> logs = List.of(
                createArchiveLog("t1_arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("t1_arc2.log", 200, 300, 2, 1, ONE_GB),
                createRedoLog("t1_redo.log", 300, 3, 1),
                createArchiveLog("t2_arc1.log", 50, 150, 1, 2, ONE_GB),
                createArchiveLog("t2_arc2.log", 150, 200, 2, 2, ONE_GB),
                createRedoLog("t2_redo.log", 200, 3, 2));

        SessionLogSelection result = selector.selectLogsForSession(
                new LogFilesResult(logs, twoThreadsOpen()), UPPER_BOUNDS);

        assertThat(result.logFiles()).extracting(LogFile::getFileName)
                .containsExactlyInAnyOrder("t1_arc1.log", "t1_arc2.log", "t2_arc1.log", "t2_arc2.log");
        assertThat(result.effectiveUpperBounds()).isEqualTo(Scn.valueOf(200));
    }

    @Test
    @FixFor("dbz#2326")
    void testCappedBoundaryNeverRegressesWithPinnedWatermark() {
        // A long-running transaction pins the lower watermark, so every collection returns the
        // same log list. Each selection must still advance the upper boundary past the previous
        // one; a boundary that repeats produces a session that re-reads redo and emits nothing.
        CappedLogFileSessionSelector pinnedSelector = new CappedLogFileSessionSelector(1, ONE_GB);

        List<LogFile> logs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createArchiveLog("arc3.log", 300, 400, 3, 1, ONE_GB),
                createRedoLog("redo1.log", 400, 4, 1));
        LogFilesResult result = new LogFilesResult(logs, singleThreadOpen());

        // Call 1: capped at arc1, reading up to 200
        SessionLogSelection first = pinnedSelector.selectLogsForSession(result, UPPER_BOUNDS);
        assertThat(first.logFiles()).extracting(LogFile::getFileName).containsExactly("arc1.log");
        assertThat(first.effectiveUpperBounds()).isEqualTo(Scn.valueOf(200));

        // Call 2: byte cap alone would repeat arc1/200; the window extends past the mined boundary
        SessionLogSelection second = pinnedSelector.selectLogsForSession(result, UPPER_BOUNDS);
        assertThat(second.logFiles()).extracting(LogFile::getFileName).containsExactly("arc1.log", "arc2.log");
        assertThat(second.effectiveUpperBounds()).isEqualTo(Scn.valueOf(300));

        // Call 3: advances again
        SessionLogSelection third = pinnedSelector.selectLogsForSession(result, UPPER_BOUNDS);
        assertThat(third.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc1.log", "arc2.log", "arc3.log");
        assertThat(third.effectiveUpperBounds()).isEqualTo(Scn.valueOf(400));

        // Call 4: advancing past 400 requires the online redo log; thread now mines online
        SessionLogSelection fourth = pinnedSelector.selectLogsForSession(result, UPPER_BOUNDS);
        assertThat(fourth.logFiles()).containsExactlyInAnyOrderElementsOf(logs);
        assertThat(fourth.effectiveUpperBounds()).isEqualTo(UPPER_BOUNDS);
    }

    @Test
    @FixFor("dbz#2326")
    void testOnlineModeRatchetsBoundaryForSubsequentCappedSelections() {
        // An online-mode pass mines to the online upper boundary; a later capped selection from
        // the still-pinned watermark must not regress below it (the dbz#2326 point-3 trace where
        // "Using capped logs, reading up to <scn>" repeated a boundary below an earlier online pass).
        CappedLogFileSessionSelector pinnedSelector = new CappedLogFileSessionSelector(1, ONE_GB);

        // Call 1: online mode, mined up to 500
        List<LogFile> initialLogs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB / 2),
                createRedoLog("redo1.log", 200, 2, 1));
        SessionLogSelection first = pinnedSelector.selectLogsForSession(
                new LogFilesResult(initialLogs, singleThreadOpen()), Scn.valueOf(500));
        assertThat(first.effectiveUpperBounds()).isEqualTo(Scn.valueOf(500));

        // Call 2: log switches occurred; the byte cap alone would select arc1 (top 200), far below
        // the mined boundary. The window extends until its top passes 500.
        List<LogFile> advancedLogs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createArchiveLog("arc3.log", 300, 400, 3, 1, ONE_GB),
                createArchiveLog("arc4.log", 400, 550, 4, 1, ONE_GB),
                createArchiveLog("arc5.log", 550, 700, 5, 1, ONE_GB),
                createRedoLog("redo1.log", 700, 6, 1));
        SessionLogSelection second = pinnedSelector.selectLogsForSession(
                new LogFilesResult(advancedLogs, singleThreadOpen()), UPPER_BOUNDS);
        assertThat(second.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc1.log", "arc2.log", "arc3.log", "arc4.log");
        assertThat(second.effectiveUpperBounds()).isEqualTo(Scn.valueOf(550));
    }

    @Test
    @FixFor("dbz#2326")
    void testWindowEndingOnNonCurrentOnlineLogExtendsThroughCurrent() {
        // The byte cap lands on a non-current online redo log. Capping the boundary there would
        // throttle online throughput for no benefit; once the window has reached the online logs,
        // it extends through the CURRENT log and the session reads to the online upper boundary.
        List<LogFile> logs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createNonCurrentRedoLog("redo_active.log", 200, 300, 2, 1),
                createRedoLog("redo_current.log", 300, 3, 1));

        SessionLogSelection result = selector.selectLogsForSession(
                new LogFilesResult(logs, singleThreadOpen()), UPPER_BOUNDS);

        assertThat(result.logFiles()).containsExactlyInAnyOrderElementsOf(logs);
        assertThat(result.effectiveUpperBounds()).isEqualTo(UPPER_BOUNDS);
    }

    @Test
    @FixFor("dbz#2326")
    void testTwoThreadFloorExtensionOnlyExtendsThreadsBelowMinedBoundary() {
        // RAC: after mining to 220, thread 2's byte-capped top (220) is at the mined boundary and
        // must extend; thread 1's top (250) is already beyond it and keeps its byte-capped window.
        CappedLogFileSessionSelector pinnedSelector = new CappedLogFileSessionSelector(1, ONE_GB);

        List<LogFile> initialLogs = List.of(
                createArchiveLog("t1_arc1.log", 100, 250, 1, 1, ONE_GB),
                createRedoLog("t1_redo.log", 250, 2, 1),
                createArchiveLog("t2_arc1.log", 100, 220, 1, 2, ONE_GB),
                createRedoLog("t2_redo.log", 220, 2, 2));

        // Call 1: capped at each thread's first archive; boundary = min(250, 220) = 220
        SessionLogSelection first = pinnedSelector.selectLogsForSession(
                new LogFilesResult(initialLogs, twoThreadsOpen()), UPPER_BOUNDS);
        assertThat(first.effectiveUpperBounds()).isEqualTo(Scn.valueOf(220));

        // Call 2: thread 2 switched a new archive; only its window extends past the mined boundary
        List<LogFile> advancedLogs = List.of(
                createArchiveLog("t1_arc1.log", 100, 250, 1, 1, ONE_GB),
                createRedoLog("t1_redo.log", 250, 2, 1),
                createArchiveLog("t2_arc1.log", 100, 220, 1, 2, ONE_GB),
                createArchiveLog("t2_arc2.log", 220, 330, 2, 2, ONE_GB),
                createRedoLog("t2_redo.log", 330, 3, 2));

        SessionLogSelection second = pinnedSelector.selectLogsForSession(
                new LogFilesResult(advancedLogs, twoThreadsOpen()), UPPER_BOUNDS);
        assertThat(second.logFiles()).extracting(LogFile::getFileName)
                .containsExactlyInAnyOrder("t1_arc1.log", "t2_arc1.log", "t2_arc2.log");
        assertThat(second.effectiveUpperBounds()).isEqualTo(Scn.valueOf(250));
    }

    @Test
    @FixFor("dbz#1589")
    void testConsistentThroughScnCapsBoundsAndFiltersLogs() {
        // The collector truncated consistency at SCN 300 (SEQ3 missing); the redo log (SEQ4)
        // starts beyond the boundary and must be excluded, and bounds capped to 300.
        List<LogFile> logs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB / 2),
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB / 2),
                createRedoLog("redo1.log", 400, 4, 1));

        SessionLogSelection result = selector.selectLogsForSession(
                new LogFilesResult(logs, singleThreadOpen(), Scn.valueOf(300)), UPPER_BOUNDS);

        assertThat(result.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc1.log", "arc2.log");
        assertThat(result.effectiveUpperBounds()).isEqualTo(Scn.valueOf(300));
    }

    @Test
    @FixFor("dbz#1589")
    void testOpenThreadWithAllLogsBeyondConsistentBoundaryDoesNotThrow() {
        // Thread 1 truncated at SCN 150 (SEQ2 missing); thread 2's logs all start at or beyond
        // the boundary and are filtered out entirely. The open-thread sanity check must not
        // throw in that case; thread 2 simply contributes nothing to this session.
        List<LogFile> logs = List.of(
                createArchiveLog("t1_arc1.log", 100, 150, 1, 1, ONE_GB),
                createRedoLog("t1_redo.log", 200, 3, 1),
                createArchiveLog("t2_arc1.log", 160, 250, 1, 2, ONE_GB),
                createRedoLog("t2_redo.log", 250, 2, 2));

        SessionLogSelection result = selector.selectLogsForSession(
                new LogFilesResult(logs, twoThreadsOpen(), Scn.valueOf(150)), UPPER_BOUNDS);

        assertThat(result.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("t1_arc1.log");
        assertThat(result.effectiveUpperBounds()).isEqualTo(Scn.valueOf(150));
    }

    @Test
    @FixFor("dbz#1589")
    void testConsistentThroughScnTakesPrecedenceOverMinedBoundaryFloor() {
        // The floor from previously mined boundaries must never force mining past a log sequence
        // gap; the collector's consistent-through boundary always wins. Once the gap heals, the
        // floor resumes from the highest previously mined boundary, not the gap-capped one.
        CappedLogFileSessionSelector pinnedSelector = new CappedLogFileSessionSelector(1, ONE_GB);

        // Call 1: no gap; online mode mines through 500
        List<LogFile> initialLogs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB / 2),
                createRedoLog("redo1.log", 200, 2, 1));
        SessionLogSelection first = pinnedSelector.selectLogsForSession(
                new LogFilesResult(initialLogs, singleThreadOpen()), Scn.valueOf(500));
        assertThat(first.effectiveUpperBounds()).isEqualTo(Scn.valueOf(500));

        // Call 2: SEQ3 went missing and consistency is truncated at 300, below the mined
        // boundary; the gap cap wins and the window cannot extend across the gap
        List<LogFile> gappedLogs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createRedoLog("redo1.log", 400, 4, 1));
        SessionLogSelection second = pinnedSelector.selectLogsForSession(
                new LogFilesResult(gappedLogs, singleThreadOpen(), Scn.valueOf(300)), UPPER_BOUNDS);
        assertThat(second.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc1.log", "arc2.log");
        assertThat(second.effectiveUpperBounds()).isEqualTo(Scn.valueOf(300));

        // Call 3: the gap healed; the window extends past the highest mined boundary (500)
        List<LogFile> healedLogs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createArchiveLog("arc3.log", 300, 450, 3, 1, ONE_GB),
                createArchiveLog("arc4.log", 450, 600, 4, 1, ONE_GB),
                createRedoLog("redo1.log", 600, 5, 1));
        SessionLogSelection third = pinnedSelector.selectLogsForSession(
                new LogFilesResult(healedLogs, singleThreadOpen()), UPPER_BOUNDS);
        assertThat(third.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc1.log", "arc2.log", "arc3.log", "arc4.log");
        assertThat(third.effectiveUpperBounds()).isEqualTo(Scn.valueOf(600));
    }

    private static LogFile createArchiveLog(String name, long startScn, long endScn, int seq, int thread, long bytes) {
        return LogFile.forArchive(name, Scn.valueOf(startScn), Scn.valueOf(endScn), BigInteger.valueOf(seq), thread, bytes, false, false);
    }

    private static LogFile createRedoLog(String name, long startScn, int seq, int thread) {
        return LogFile.forRedo(name, Scn.valueOf(startScn), Scn.valueOf(Long.MAX_VALUE), BigInteger.valueOf(seq), true, thread, ONE_GB);
    }

    private static LogFile createNonCurrentRedoLog(String name, long startScn, long endScn, int seq, int thread) {
        return LogFile.forRedo(name, Scn.valueOf(startScn), Scn.valueOf(endScn), BigInteger.valueOf(seq), false, thread, ONE_GB);
    }

    private static RedoThreadState singleThreadOpen() {
        return RedoThreadState.builder()
                .thread()
                .threadId(1)
                .status("OPEN")
                .enabled("PUBLIC")
                .instanceName("ORCLCDB")
                .logGroups(2L)
                .openTime(Instant.now().minus(10, ChronoUnit.MINUTES))
                .checkpointScn(Scn.valueOf(100))
                .checkpointTime(Instant.now().minus(5, ChronoUnit.MINUTES))
                .currentGroupNumber(1L)
                .currentSequenceNumber(1L)
                .enabledScn(Scn.valueOf(50))
                .enabledTime(Instant.now().minus(10, ChronoUnit.MINUTES))
                .disabledScn(Scn.valueOf(0))
                .disabledTime(null)
                .lastRedoScn(Scn.valueOf(1000))
                .lastRedoBlock(1234L)
                .lastRedoSequenceNumber(1L)
                .lastRedoTime(Instant.now())
                .conId(0L)
                .build()
                .build();
    }

    private static RedoThreadState twoThreadsOpen() {
        return RedoThreadState.builder()
                .thread()
                .threadId(1)
                .status("OPEN")
                .enabled("PUBLIC")
                .instanceName("ORCLCDB")
                .logGroups(2L)
                .openTime(Instant.now().minus(10, ChronoUnit.MINUTES))
                .checkpointScn(Scn.valueOf(100))
                .checkpointTime(Instant.now().minus(5, ChronoUnit.MINUTES))
                .currentGroupNumber(1L)
                .currentSequenceNumber(1L)
                .enabledScn(Scn.valueOf(50))
                .enabledTime(Instant.now().minus(10, ChronoUnit.MINUTES))
                .disabledScn(Scn.valueOf(0))
                .disabledTime(null)
                .lastRedoScn(Scn.valueOf(1000))
                .lastRedoBlock(1234L)
                .lastRedoSequenceNumber(1L)
                .lastRedoTime(Instant.now())
                .conId(0L)
                .build()
                .thread()
                .threadId(2)
                .status("OPEN")
                .enabled("PUBLIC")
                .instanceName("ORCLCDB")
                .logGroups(2L)
                .openTime(Instant.now().minus(10, ChronoUnit.MINUTES))
                .checkpointScn(Scn.valueOf(100))
                .checkpointTime(Instant.now().minus(5, ChronoUnit.MINUTES))
                .currentGroupNumber(1L)
                .currentSequenceNumber(1L)
                .enabledScn(Scn.valueOf(50))
                .enabledTime(Instant.now().minus(10, ChronoUnit.MINUTES))
                .disabledScn(Scn.valueOf(0))
                .disabledTime(null)
                .lastRedoScn(Scn.valueOf(1000))
                .lastRedoBlock(1234L)
                .lastRedoSequenceNumber(1L)
                .lastRedoTime(Instant.now())
                .conId(0L)
                .build()
                .build();
    }

    private static RedoThreadState openAndClosedThread() {
        return RedoThreadState.builder()
                .thread()
                .threadId(1)
                .status("OPEN")
                .enabled("PUBLIC")
                .instanceName("ORCLCDB")
                .logGroups(2L)
                .openTime(Instant.now().minus(10, ChronoUnit.MINUTES))
                .checkpointScn(Scn.valueOf(100))
                .checkpointTime(Instant.now().minus(5, ChronoUnit.MINUTES))
                .currentGroupNumber(1L)
                .currentSequenceNumber(1L)
                .enabledScn(Scn.valueOf(50))
                .enabledTime(Instant.now().minus(10, ChronoUnit.MINUTES))
                .disabledScn(Scn.valueOf(0))
                .disabledTime(null)
                .lastRedoScn(Scn.valueOf(1000))
                .lastRedoBlock(1234L)
                .lastRedoSequenceNumber(1L)
                .lastRedoTime(Instant.now())
                .conId(0L)
                .build()
                .thread()
                .threadId(2)
                .status("CLOSED")
                .enabled("PUBLIC")
                .instanceName("ORCLCDB")
                .logGroups(2L)
                .openTime(Instant.now().minus(10, ChronoUnit.MINUTES))
                .checkpointScn(Scn.valueOf(100))
                .checkpointTime(Instant.now().minus(5, ChronoUnit.MINUTES))
                .currentGroupNumber(1L)
                .currentSequenceNumber(1L)
                .enabledScn(Scn.valueOf(50))
                .enabledTime(Instant.now().minus(10, ChronoUnit.MINUTES))
                .disabledScn(Scn.valueOf(200))
                .disabledTime(Instant.now().minus(5, ChronoUnit.MINUTES))
                .lastRedoScn(Scn.valueOf(200))
                .lastRedoBlock(1234L)
                .lastRedoSequenceNumber(1L)
                .lastRedoTime(Instant.now().minus(5, ChronoUnit.MINUTES))
                .conId(0L)
                .build()
                .build();
    }
}