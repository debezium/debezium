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
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.RedoThreadState;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.LogFileCollector.LogFilesResult;
import io.debezium.connector.oracle.logminer.LogFileSessionSelector.SessionLogSelection;
import io.debezium.doc.FixFor;
import io.debezium.junit.logging.LogInterceptor;

import ch.qos.logback.classic.Level;

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
    private final CappedLogFileSessionSelector selector = new CappedLogFileSessionSelector(2, ONE_GB, Scn.NULL);

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
    @FixFor({ "dbz#1713", "dbz#2326" })
    void testWatermarkAdvanceBudgetCarriedForward() {
        // First call with 3 archives; watermark then advances bringing new logs.
        // The budget does not reset on watermark shift; it only resets when all threads read online.
        List<LogFile> initialLogs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createArchiveLog("arc3.log", 300, 400, 3, 1, ONE_GB),
                createRedoLog("redo1.log", 400, 4, 1));

        // Call 1: capped at arc1+arc2
        selector.selectLogsForSession(new LogFilesResult(initialLogs, singleThreadOpen()), UPPER_BOUNDS);
        // Call 2 (same): grows to 3 => arc1+arc2+arc3
        selector.selectLogsForSession(new LogFilesResult(initialLogs, singleThreadOpen()), UPPER_BOUNDS);

        // Call 3: watermark advanced (arc1 gone, arc4 added); budget stays at 3,
        // so budget set is arc2+arc3+arc4, then the extension is a no-op (already past 400)
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
    @FixFor({ "dbz#1713", "dbz#2326" })
    void testWatermarkAdvanceAtMinimumBudgetExtendsPastPreviousBoundary() {
        // Log set changes on second call but logsPerRedoThread was never incremented (still at minimum).
        // Budget set differs from previous so no growth occurs; extension ensures no regression.
        List<LogFile> initialLogs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createArchiveLog("arc3.log", 300, 400, 3, 1, ONE_GB),
                createRedoLog("redo1.log", 400, 4, 1));

        // Call 1: capped at arc1+arc2 (threshold 2GB), mined up to 300
        selector.selectLogsForSession(new LogFilesResult(initialLogs, singleThreadOpen()), UPPER_BOUNDS);

        // Call 2: watermark advanced (arc1 gone, arc4 added); budget set is arc2+arc3 (top 400),
        // which already passes the previously mined boundary of 300, so no extension needed
        List<LogFile> advancedLogs = List.of(
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createArchiveLog("arc3.log", 300, 400, 3, 1, ONE_GB),
                createArchiveLog("arc4.log", 400, 500, 4, 1, ONE_GB),
                createRedoLog("redo1.log", 500, 5, 1));

        SessionLogSelection second = selector.selectLogsForSession(
                new LogFilesResult(advancedLogs, singleThreadOpen()), UPPER_BOUNDS);
        assertThat(second.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc2.log", "arc3.log");
        assertThat(second.effectiveUpperBounds()).isEqualTo(Scn.valueOf(400));
    }

    @Test
    @FixFor({ "dbz#1713", "dbz#2326" })
    void testBudgetCarriedThroughWatermarkAdvanceThenResetsAtOnlineRedo() {
        // Grow while pinned, watermark advances (budget carries forward), eventually reach
        // online redo which triggers the reset back to minimum.
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

        // Call 3: watermark advanced (arc1 gone, arc4 added); budget stays at 3,
        // so budget set is arc2+arc3+arc4, reading up to 500
        List<LogFile> advancedLogs = List.of(
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createArchiveLog("arc3.log", 300, 400, 3, 1, ONE_GB),
                createArchiveLog("arc4.log", 400, 500, 4, 1, ONE_GB),
                createRedoLog("redo1.log", 500, 5, 1));
        LogFilesResult advancedResult = new LogFilesResult(advancedLogs, singleThreadOpen());
        SessionLogSelection third = selector.selectLogsForSession(advancedResult, UPPER_BOUNDS);
        assertThat(third.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc2.log", "arc3.log", "arc4.log");
        assertThat(third.effectiveUpperBounds()).isEqualTo(Scn.valueOf(500));

        // Call 4: same budget set => grow to 4; budget now covers past the online redo boundary,
        // extension reaches online redo => all threads mine online, budget resets to minimum
        SessionLogSelection fourth = selector.selectLogsForSession(advancedResult, UPPER_BOUNDS);
        assertThat(fourth.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc2.log", "arc3.log", "arc4.log", "redo1.log");
        assertThat(fourth.effectiveUpperBounds()).isEqualTo(UPPER_BOUNDS);

        // Call 5: new logs after full catch-up; budget was reset to 2, so budget set is arc3+arc4
        List<LogFile> caughtUpLogs = List.of(
                createArchiveLog("arc3.log", 300, 400, 3, 1, ONE_GB),
                createArchiveLog("arc4.log", 400, 500, 4, 1, ONE_GB),
                createArchiveLog("arc5.log", 500, 600, 5, 1, ONE_GB),
                createRedoLog("redo1.log", 600, 6, 1));
        SessionLogSelection fifth = selector.selectLogsForSession(
                new LogFilesResult(caughtUpLogs, singleThreadOpen()), UPPER_BOUNDS);
        // Budget is 2, budget set is arc3+arc4 (top 500); extension pushes past the previously
        // mined boundary (UPPER_BOUNDS=1000) => includes arc5+redo, all threads online
        assertThat(fifth.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc3.log", "arc4.log", "arc5.log", "redo1.log");
        assertThat(fifth.effectiveUpperBounds()).isEqualTo(UPPER_BOUNDS);
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
        CappedLogFileSessionSelector pinnedSelector = new CappedLogFileSessionSelector(1, ONE_GB, Scn.NULL);

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
        CappedLogFileSessionSelector pinnedSelector = new CappedLogFileSessionSelector(1, ONE_GB, Scn.NULL);

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
        // RAC: after mining to 220, the watermark advances (thread 1's starting log changes) so
        // the budget set differs and growth does not fire. Thread 2's byte-capped top (220) is
        // at the mined boundary and must extend; thread 1's top (350) is already beyond it.
        CappedLogFileSessionSelector pinnedSelector = new CappedLogFileSessionSelector(1, ONE_GB, Scn.NULL);

        List<LogFile> initialLogs = List.of(
                createArchiveLog("t1_arc1.log", 100, 250, 1, 1, ONE_GB),
                createRedoLog("t1_redo.log", 250, 2, 1),
                createArchiveLog("t2_arc1.log", 100, 220, 1, 2, ONE_GB),
                createRedoLog("t2_redo.log", 220, 2, 2));

        // Call 1: capped at each thread's first archive; boundary = min(250, 220) = 220
        SessionLogSelection first = pinnedSelector.selectLogsForSession(
                new LogFilesResult(initialLogs, twoThreadsOpen()), UPPER_BOUNDS);
        assertThat(first.effectiveUpperBounds()).isEqualTo(Scn.valueOf(220));

        // Call 2: thread 1 advanced (arc1 gone, arc2 appeared); thread 2 has a new archive log
        // but its budget-capped window still starts at t2_arc1. Budget set differs from call 1
        // (thread 1 changed), so no growth. Extension only pushes thread 2 past 220.
        List<LogFile> advancedLogs = List.of(
                createArchiveLog("t1_arc2.log", 250, 350, 2, 1, ONE_GB),
                createRedoLog("t1_redo.log", 350, 3, 1),
                createArchiveLog("t2_arc1.log", 100, 220, 1, 2, ONE_GB),
                createArchiveLog("t2_arc2.log", 220, 330, 2, 2, ONE_GB),
                createRedoLog("t2_redo.log", 330, 3, 2));

        SessionLogSelection second = pinnedSelector.selectLogsForSession(
                new LogFilesResult(advancedLogs, twoThreadsOpen()), UPPER_BOUNDS);
        assertThat(second.logFiles()).extracting(LogFile::getFileName)
                .containsExactlyInAnyOrder("t1_arc2.log", "t2_arc1.log", "t2_arc2.log");
        assertThat(second.effectiveUpperBounds()).isEqualTo(Scn.valueOf(330));
    }

    @Test
    @FixFor("dbz#2326")
    void testCascadingPinsBudgetCarriesAndContinuesGrowing() {
        // Transaction A pins the watermark at arc1; budget grows to find A's commit in arc3.
        // A commits but transaction B pins at arc2; the watermark shifts from arc1 to arc2.
        // The budget carries forward (no reset) and continues growing from the carried value
        // to find B's commit, avoiding wasted re-climb iterations from minimum.
        CappedLogFileSessionSelector pinnedSelector = new CappedLogFileSessionSelector(1, ONE_GB, Scn.NULL);

        // Calls 1-3: pinned at arc1, budget grows 1 -> 2 -> 3
        List<LogFile> pinnedLogs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createArchiveLog("arc3.log", 300, 400, 3, 1, ONE_GB),
                createArchiveLog("arc4.log", 400, 500, 4, 1, ONE_GB),
                createArchiveLog("arc5.log", 500, 600, 5, 1, ONE_GB),
                createRedoLog("redo1.log", 600, 6, 1));
        LogFilesResult pinnedResult = new LogFilesResult(pinnedLogs, singleThreadOpen());

        // Call 1: budget=1, set={arc1}
        pinnedSelector.selectLogsForSession(pinnedResult, UPPER_BOUNDS);
        // Call 2: same => grow to 2, set={arc1,arc2}
        pinnedSelector.selectLogsForSession(pinnedResult, UPPER_BOUNDS);
        // Call 3: same => grow to 3, set={arc1,arc2,arc3}, boundary=400
        SessionLogSelection third = pinnedSelector.selectLogsForSession(pinnedResult, UPPER_BOUNDS);
        assertThat(third.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc1.log", "arc2.log", "arc3.log");
        assertThat(third.effectiveUpperBounds()).isEqualTo(Scn.valueOf(400));

        // Transaction A commits; watermark shifts to arc2 (transaction B pins there).
        // Budget carries forward at 3; budget set={arc2,arc3,arc4} differs from previous => no growth
        List<LogFile> shiftedLogs = List.of(
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createArchiveLog("arc3.log", 300, 400, 3, 1, ONE_GB),
                createArchiveLog("arc4.log", 400, 500, 4, 1, ONE_GB),
                createArchiveLog("arc5.log", 500, 600, 5, 1, ONE_GB),
                createRedoLog("redo1.log", 600, 6, 1));
        LogFilesResult shiftedResult = new LogFilesResult(shiftedLogs, singleThreadOpen());

        SessionLogSelection fourth = pinnedSelector.selectLogsForSession(shiftedResult, UPPER_BOUNDS);
        assertThat(fourth.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc2.log", "arc3.log", "arc4.log");
        assertThat(fourth.effectiveUpperBounds()).isEqualTo(Scn.valueOf(500));

        // Call 5: still pinned at arc2, budget set unchanged => grow to 4, set={arc2,arc3,arc4,arc5}
        SessionLogSelection fifth = pinnedSelector.selectLogsForSession(shiftedResult, UPPER_BOUNDS);
        assertThat(fifth.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc2.log", "arc3.log", "arc4.log", "arc5.log");
        assertThat(fifth.effectiveUpperBounds()).isEqualTo(Scn.valueOf(600));

        // Call 6: grow to 5, reaches online redo => all threads online, budget resets
        SessionLogSelection sixth = pinnedSelector.selectLogsForSession(shiftedResult, UPPER_BOUNDS);
        assertThat(sixth.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc2.log", "arc3.log", "arc4.log", "arc5.log", "redo1.log");
        assertThat(sixth.effectiveUpperBounds()).isEqualTo(UPPER_BOUNDS);
    }

    @Test
    @FixFor("dbz#2326")
    void testForcedSwitchTailLogsDoNotSuppressGrowth() {
        // Forced log switches (ARCHIVE_LAG_TARGET) produce new small archive logs at the tail
        // while the watermark is pinned. The budget set is unchanged (tail logs are beyond the
        // byte threshold) so growth fires correctly on each iteration.
        CappedLogFileSessionSelector pinnedSelector = new CappedLogFileSessionSelector(1, ONE_GB, Scn.NULL);

        // Call 1: budget=1, set={arc1}; boundary=200
        List<LogFile> initialLogs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createRedoLog("redo1.log", 300, 3, 1));
        SessionLogSelection first = pinnedSelector.selectLogsForSession(
                new LogFilesResult(initialLogs, singleThreadOpen()), UPPER_BOUNDS);
        assertThat(first.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc1.log");
        assertThat(first.effectiveUpperBounds()).isEqualTo(Scn.valueOf(200));

        // Call 2: forced switch added a small runt (arc3, 27MB) at the tail; the full log list
        // changed but the budget set is still {arc1} (1GB threshold met at arc1) => growth fires,
        // budget=2, set={arc1,arc2}; boundary=300
        List<LogFile> switchedLogs1 = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createArchiveLog("arc3.log", 300, 350, 3, 1, 27 * 1024 * 1024L),
                createRedoLog("redo1.log", 350, 4, 1));
        SessionLogSelection second = pinnedSelector.selectLogsForSession(
                new LogFilesResult(switchedLogs1, singleThreadOpen()), UPPER_BOUNDS);
        assertThat(second.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc1.log", "arc2.log");
        assertThat(second.effectiveUpperBounds()).isEqualTo(Scn.valueOf(300));

        // Call 3: another forced switch added arc4 (29MB); budget set still {arc1,arc2} (2GB
        // threshold met at arc2) => growth fires, budget=3. At 3GB threshold the small runts
        // don't fill the gap, so the budget reaches redo1 and all threads mine online.
        List<LogFile> switchedLogs2 = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createArchiveLog("arc3.log", 300, 350, 3, 1, 27 * 1024 * 1024L),
                createArchiveLog("arc4.log", 350, 380, 4, 1, 29 * 1024 * 1024L),
                createRedoLog("redo1.log", 380, 5, 1));
        SessionLogSelection third = pinnedSelector.selectLogsForSession(
                new LogFilesResult(switchedLogs2, singleThreadOpen()), UPPER_BOUNDS);
        assertThat(third.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc1.log", "arc2.log", "arc3.log", "arc4.log", "redo1.log");
        assertThat(third.effectiveUpperBounds()).isEqualTo(UPPER_BOUNDS);
    }

    @Test
    @FixFor("dbz#2326")
    void testSteadyOnlineModeDoesNotOscillateGrowthAndReset() {
        // In steady all-online mode the budget set is identical between iterations, which grew the
        // log count only for the all-online branch to reset it within the same call - a grow/reset
        // DEBUG pair and a redundant budget recomputation on every iteration. After an online pass
        // the growth baseline is cleared, so neither message should be emitted.
        final LogInterceptor interceptor = new LogInterceptor(CappedLogFileSessionSelector.class);
        interceptor.setLoggerLevel(CappedLogFileSessionSelector.class, Level.DEBUG);

        List<LogFile> logs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createRedoLog("redo1.log", 200, 2, 1));
        LogFilesResult result = new LogFilesResult(logs, singleThreadOpen());

        for (int i = 0; i < 3; i++) {
            SessionLogSelection selection = selector.selectLogsForSession(result, UPPER_BOUNDS);
            assertThat(selection.logFiles()).containsExactlyInAnyOrderElementsOf(logs);
            assertThat(selection.effectiveUpperBounds()).isEqualTo(UPPER_BOUNDS);
        }

        assertThat(interceptor.containsMessage("growing log count")).isFalse();
        assertThat(interceptor.containsMessage("resetting log count")).isFalse();
    }

    @Test
    @FixFor("dbz#2326")
    void testGrowthReengagesAfterLeavingOnlineMode() {
        // An all-online pass clears the growth baseline; the first capped iteration afterward
        // establishes a new baseline, and growth engages from the second identical capped set.
        List<LogFile> onlineLogs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createRedoLog("redo1.log", 200, 2, 1));
        selector.selectLogsForSession(new LogFilesResult(onlineLogs, singleThreadOpen()), Scn.valueOf(250));

        // A burst of switches leaves a capped backlog pinned at arc1
        List<LogFile> cappedLogs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createArchiveLog("arc3.log", 300, 400, 3, 1, ONE_GB),
                createArchiveLog("arc4.log", 400, 500, 4, 1, ONE_GB),
                createRedoLog("redo1.log", 500, 5, 1));
        LogFilesResult cappedResult = new LogFilesResult(cappedLogs, singleThreadOpen());

        // First capped call: budget=2 => arc1+arc2, no growth (baseline was cleared by the
        // online pass); the window is already past the previously mined boundary of 250
        SessionLogSelection first = selector.selectLogsForSession(cappedResult, UPPER_BOUNDS);
        assertThat(first.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc1.log", "arc2.log");
        assertThat(first.effectiveUpperBounds()).isEqualTo(Scn.valueOf(300));

        // Second capped call with the same set: growth fires => arc1+arc2+arc3
        SessionLogSelection second = selector.selectLogsForSession(cappedResult, UPPER_BOUNDS);
        assertThat(second.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc1.log", "arc2.log", "arc3.log");
        assertThat(second.effectiveUpperBounds()).isEqualTo(Scn.valueOf(400));
    }

    @Test
    @FixFor("dbz#2326")
    void testSeededBoundaryDerivesWindowWidthAndGrowthContinuesFromIt() {
        // Restart mid-pin: the boundary seeded from the restored offsets re-derives the window
        // width on the first selection, so the session resumes at the pre-restart width and
        // extends past the already-mined ground instead of re-climbing from the minimum.
        CappedLogFileSessionSelector seededSelector = new CappedLogFileSessionSelector(1, ONE_GB, Scn.valueOf(400));

        List<LogFile> logs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createArchiveLog("arc3.log", 300, 400, 3, 1, ONE_GB),
                createArchiveLog("arc4.log", 400, 500, 4, 1, ONE_GB),
                createArchiveLog("arc5.log", 500, 600, 5, 1, ONE_GB),
                createRedoLog("redo1.log", 600, 6, 1));
        LogFilesResult result = new LogFilesResult(logs, singleThreadOpen());

        // Call 1: 3 GB of logs lie below the seeded boundary => derived count 3; the budget window
        // tops at the boundary itself, so the extension pushes one log past it
        SessionLogSelection first = seededSelector.selectLogsForSession(result, UPPER_BOUNDS);
        assertThat(first.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc1.log", "arc2.log", "arc3.log", "arc4.log");
        assertThat(first.effectiveUpperBounds()).isEqualTo(Scn.valueOf(500));

        // Call 2: growth continues from the derived count (3 -> 4), not from the minimum
        SessionLogSelection second = seededSelector.selectLogsForSession(result, UPPER_BOUNDS);
        assertThat(second.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc1.log", "arc2.log", "arc3.log", "arc4.log", "arc5.log");
        assertThat(second.effectiveUpperBounds()).isEqualTo(Scn.valueOf(600));
    }

    @Test
    @FixFor("dbz#2326")
    void testSeedAtResumePositionOrNullSeedBehavesAsUnseeded() {
        List<LogFile> logs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createRedoLog("redo1.log", 300, 3, 1));
        LogFilesResult result = new LogFilesResult(logs, singleThreadOpen());

        // Seed at the resume position: no logs lie below it, derived count stays at minimum
        // and the extension has nothing to push past
        CappedLogFileSessionSelector atResumeSelector = new CappedLogFileSessionSelector(1, ONE_GB, Scn.valueOf(100));
        SessionLogSelection atResume = atResumeSelector.selectLogsForSession(result, UPPER_BOUNDS);
        assertThat(atResume.logFiles()).extracting(LogFile::getFileName).containsExactly("arc1.log");
        assertThat(atResume.effectiveUpperBounds()).isEqualTo(Scn.valueOf(200));

        // A none/null seed (no commits recorded in the offsets yet) is ignored entirely
        CappedLogFileSessionSelector nullSeedSelector = new CappedLogFileSessionSelector(1, ONE_GB, Scn.NULL);
        SessionLogSelection nullSeed = nullSeedSelector.selectLogsForSession(result, UPPER_BOUNDS);
        assertThat(nullSeed.logFiles()).extracting(LogFile::getFileName).containsExactly("arc1.log");
        assertThat(nullSeed.effectiveUpperBounds()).isEqualTo(Scn.valueOf(200));
    }

    @Test
    @FixFor("dbz#2326")
    void testSeededBoundaryWithinOnlineRedoRunsOnlinePass() {
        // The connector was mining online redo before the restart; the seeded boundary lies within
        // the current log, so the first selection reaches the online redo and runs an online pass
        CappedLogFileSessionSelector seededSelector = new CappedLogFileSessionSelector(1, ONE_GB, Scn.valueOf(800));

        List<LogFile> logs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createRedoLog("redo1.log", 200, 2, 1));

        SessionLogSelection result = seededSelector.selectLogsForSession(
                new LogFilesResult(logs, singleThreadOpen()), UPPER_BOUNDS);
        assertThat(result.logFiles()).containsExactlyInAnyOrderElementsOf(logs);
        assertThat(result.effectiveUpperBounds()).isEqualTo(UPPER_BOUNDS);
    }

    @Test
    @FixFor("dbz#2326")
    void testSeedDerivationUsesWidestSpanAcrossThreads() {
        // RAC: the derived count reflects the widest per-thread byte span below the seeded
        // boundary, so no thread's window collapses below its pre-restart width
        CappedLogFileSessionSelector seededSelector = new CappedLogFileSessionSelector(1, ONE_GB, Scn.valueOf(500));

        List<LogFile> logs = List.of(
                createArchiveLog("t1_arc1.log", 100, 300, 1, 1, ONE_GB),
                createArchiveLog("t1_arc2.log", 300, 500, 2, 1, ONE_GB),
                createArchiveLog("t1_arc3.log", 500, 700, 3, 1, ONE_GB),
                createRedoLog("t1_redo.log", 700, 4, 1),
                createArchiveLog("t2_arc1.log", 100, 450, 1, 2, ONE_GB),
                createArchiveLog("t2_arc2.log", 450, 650, 2, 2, ONE_GB),
                createRedoLog("t2_redo.log", 650, 3, 2));

        // Both threads have 2 GB below the boundary => derived count 2. Thread 1's budget window
        // tops exactly at the boundary and extends one log past it; thread 2 is already beyond.
        SessionLogSelection result = seededSelector.selectLogsForSession(
                new LogFilesResult(logs, twoThreadsOpen()), UPPER_BOUNDS);
        assertThat(result.logFiles()).extracting(LogFile::getFileName)
                .containsExactlyInAnyOrder("t1_arc1.log", "t1_arc2.log", "t1_arc3.log", "t2_arc1.log", "t2_arc2.log");
        assertThat(result.effectiveUpperBounds()).isEqualTo(Scn.valueOf(650));
    }

    @Test
    @FixFor("dbz#2326")
    void testSeededWidthSpansForcedSwitchRuntsInOneSession() {
        // The Finding 2 payoff: with forced-switch runts below the seeded boundary, the derived
        // byte width covers all of them plus a full log beyond in the first session, where an
        // unseeded restart would crawl one log per session from the minimum
        CappedLogFileSessionSelector seededSelector = new CappedLogFileSessionSelector(1, ONE_GB, Scn.valueOf(500));

        List<LogFile> logs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("runt2.log", 200, 300, 2, 1, 50 * 1024 * 1024L),
                createArchiveLog("runt3.log", 300, 400, 3, 1, 50 * 1024 * 1024L),
                createArchiveLog("arc4.log", 400, 500, 4, 1, ONE_GB),
                createArchiveLog("arc5.log", 500, 600, 5, 1, ONE_GB),
                createArchiveLog("arc6.log", 600, 700, 6, 1, ONE_GB),
                createRedoLog("redo1.log", 700, 7, 1));

        // ~2.1 GB below the boundary => derived count 3; the 3 GB budget window already tops at
        // 600, a full log past the previously mined ground, with no extension needed
        SessionLogSelection result = seededSelector.selectLogsForSession(
                new LogFilesResult(logs, singleThreadOpen()), UPPER_BOUNDS);
        assertThat(result.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc1.log", "runt2.log", "runt3.log", "arc4.log", "arc5.log");
        assertThat(result.effectiveUpperBounds()).isEqualTo(Scn.valueOf(600));
    }

    @Test
    @FixFor("dbz#2326")
    void testStallGrowsLinearlyWhenBoundaryBarelyAhead() {
        // Steady pin where the mined boundary sits just past the budget window: the derived
        // width offers nothing beyond the +1 floor, so growth stays exactly linear
        final LogInterceptor interceptor = new LogInterceptor(CappedLogFileSessionSelector.class);
        interceptor.setLoggerLevel(CappedLogFileSessionSelector.class, Level.DEBUG);

        List<LogFile> logs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createArchiveLog("arc3.log", 300, 400, 3, 1, ONE_GB),
                createRedoLog("redo1.log", 400, 4, 1));
        LogFilesResult result = new LogFilesResult(logs, singleThreadOpen());

        // Call 1: capped at arc1+arc2, mined up to 300
        selector.selectLogsForSession(result, UPPER_BOUNDS);

        // Call 2: stall; 2 GB lie below the mined boundary => derived 2, the floor grows to 3
        SessionLogSelection second = selector.selectLogsForSession(result, UPPER_BOUNDS);
        assertThat(second.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc1.log", "arc2.log", "arc3.log");
        assertThat(second.effectiveUpperBounds()).isEqualTo(Scn.valueOf(400));
        assertThat(interceptor.containsMessage("growing log count per redo thread to 3")).isTrue();
    }

    @Test
    @FixFor("dbz#2326")
    void testStallJumpsToDerivedCountInSingleStep() {
        // A pin re-engages after an online pass recorded a far-ahead boundary: the first stall
        // derives the full width to the mined ground in one step instead of climbing one log
        // per session toward it
        final LogInterceptor interceptor = new LogInterceptor(CappedLogFileSessionSelector.class);
        interceptor.setLoggerLevel(CappedLogFileSessionSelector.class, Level.DEBUG);
        CappedLogFileSessionSelector pinnedSelector = new CappedLogFileSessionSelector(1, ONE_GB, Scn.NULL);

        // Call 1: online pass mining up to 500; boundary 500 recorded, growth baseline cleared
        List<LogFile> onlineLogs = List.of(
                createArchiveLog("arc0.log", 100, 150, 1, 1, ONE_GB / 2),
                createRedoLog("redo1.log", 150, 2, 1));
        pinnedSelector.selectLogsForSession(new LogFilesResult(onlineLogs, singleThreadOpen()), Scn.valueOf(500));

        // Call 2: a burst left a pinned backlog; budget=1 => {arc1}, the extension pushes past 500
        List<LogFile> backlogLogs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createArchiveLog("arc3.log", 300, 400, 3, 1, ONE_GB),
                createArchiveLog("arc4.log", 400, 500, 4, 1, ONE_GB),
                createArchiveLog("arc5.log", 500, 600, 5, 1, ONE_GB),
                createArchiveLog("arc6.log", 600, 700, 6, 1, ONE_GB),
                createRedoLog("redo1.log", 700, 7, 1));
        LogFilesResult backlogResult = new LogFilesResult(backlogLogs, singleThreadOpen());

        SessionLogSelection second = pinnedSelector.selectLogsForSession(backlogResult, UPPER_BOUNDS);
        assertThat(second.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc1.log", "arc2.log", "arc3.log", "arc4.log", "arc5.log");
        assertThat(second.effectiveUpperBounds()).isEqualTo(Scn.valueOf(600));

        // Call 3: stall; 5 GB lie below the mined boundary of 600 => budget jumps 1 -> 5 in one
        // step (linear growth would have logged 2)
        SessionLogSelection third = pinnedSelector.selectLogsForSession(backlogResult, UPPER_BOUNDS);
        assertThat(third.logFiles()).extracting(LogFile::getFileName)
                .containsExactly("arc1.log", "arc2.log", "arc3.log", "arc4.log", "arc5.log", "arc6.log");
        assertThat(third.effectiveUpperBounds()).isEqualTo(Scn.valueOf(700));
        assertThat(interceptor.containsMessage("growing log count per redo thread to 5")).isTrue();
        assertThat(interceptor.containsMessage("growing log count per redo thread to 2")).isFalse();
    }

    @Test
    @FixFor("dbz#2326")
    void testStallGrowthClampedAtMaximumWhileBoundaryStillAdvances() {
        // The derived width is clamped at the growth ceiling so a session's window stays within
        // the query timeout; while clamped, the boundary still advances via the extension
        final LogInterceptor interceptor = new LogInterceptor(CappedLogFileSessionSelector.class);
        interceptor.setLoggerLevel(CappedLogFileSessionSelector.class, Level.DEBUG);
        CappedLogFileSessionSelector pinnedSelector = new CappedLogFileSessionSelector(1, ONE_GB, Scn.NULL);
        final Scn upperBounds = Scn.valueOf(5000);

        // Call 1: online pass mining up to 1850
        List<LogFile> onlineLogs = List.of(
                createArchiveLog("arc0.log", 100, 150, 1, 1, ONE_GB / 2),
                createRedoLog("redo1.log", 150, 2, 1));
        pinnedSelector.selectLogsForSession(new LogFilesResult(onlineLogs, singleThreadOpen()), Scn.valueOf(1850));

        // Call 2: a 25-archive pinned backlog; budget=1 => {arc1}, the extension pushes past
        // 1850 => arc1..arc18, mined up to 1900
        List<LogFile> backlogLogs = new ArrayList<>();
        for (int i = 1; i <= 25; i++) {
            backlogLogs.add(createArchiveLog("arc" + i + ".log", 100 * i, 100 * (i + 1), i, 1, ONE_GB));
        }
        backlogLogs.add(createRedoLog("redo1.log", 2600, 26, 1));
        LogFilesResult backlogResult = new LogFilesResult(backlogLogs, singleThreadOpen());

        SessionLogSelection second = pinnedSelector.selectLogsForSession(backlogResult, upperBounds);
        assertThat(second.logFiles()).hasSize(18);
        assertThat(second.effectiveUpperBounds()).isEqualTo(Scn.valueOf(1900));

        // Call 3: stall; 18 GB lie below the mined boundary but the derived count clamps at 16;
        // the budget covers arc1..arc16 and the extension adds arc17..arc19
        SessionLogSelection third = pinnedSelector.selectLogsForSession(backlogResult, upperBounds);
        assertThat(third.logFiles()).hasSize(19);
        assertThat(third.effectiveUpperBounds()).isEqualTo(Scn.valueOf(2000));
        assertThat(interceptor.containsMessage("growing log count per redo thread to 16")).isTrue();
        assertThat(interceptor.containsMessage("growing log count per redo thread to 17")).isFalse();
        assertThat(interceptor.containsMessage("growing log count per redo thread to 18")).isFalse();

        // Call 4: still stalled; the budget holds at the ceiling yet the boundary advances
        SessionLogSelection fourth = pinnedSelector.selectLogsForSession(backlogResult, upperBounds);
        assertThat(fourth.logFiles()).hasSize(20);
        assertThat(fourth.effectiveUpperBounds()).isEqualTo(Scn.valueOf(2100));
        assertThat(interceptor.containsMessage("growing log count per redo thread to 17")).isFalse();
    }

    @Test
    @FixFor("dbz#2326")
    void testAllOnlineResetStillAppliesAfterDerivedGrowth() {
        // The all-online reset must survive derived growth: once a grown budget reaches the
        // online redo, the count resets to the minimum and the growth baseline clears
        final LogInterceptor interceptor = new LogInterceptor(CappedLogFileSessionSelector.class);
        interceptor.setLoggerLevel(CappedLogFileSessionSelector.class, Level.DEBUG);
        CappedLogFileSessionSelector pinnedSelector = new CappedLogFileSessionSelector(1, ONE_GB, Scn.NULL);

        // Call 1: online pass mining up to 500
        List<LogFile> onlineLogs = List.of(
                createArchiveLog("arc0.log", 100, 150, 1, 1, ONE_GB / 2),
                createRedoLog("redo1.log", 150, 2, 1));
        pinnedSelector.selectLogsForSession(new LogFilesResult(onlineLogs, singleThreadOpen()), Scn.valueOf(500));

        // Calls 2-3: pinned backlog; budget climbs to the derived width of 5 as in the jump test
        List<LogFile> backlogLogs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1, ONE_GB),
                createArchiveLog("arc2.log", 200, 300, 2, 1, ONE_GB),
                createArchiveLog("arc3.log", 300, 400, 3, 1, ONE_GB),
                createArchiveLog("arc4.log", 400, 500, 4, 1, ONE_GB),
                createArchiveLog("arc5.log", 500, 600, 5, 1, ONE_GB),
                createArchiveLog("arc6.log", 600, 700, 6, 1, ONE_GB),
                createRedoLog("redo1.log", 700, 7, 1));
        LogFilesResult backlogResult = new LogFilesResult(backlogLogs, singleThreadOpen());
        pinnedSelector.selectLogsForSession(backlogResult, UPPER_BOUNDS);
        pinnedSelector.selectLogsForSession(backlogResult, UPPER_BOUNDS);

        // Call 4: stall again; the derived width of 6 reaches the online redo => all threads
        // mine online, the count resets to the minimum
        SessionLogSelection fourth = pinnedSelector.selectLogsForSession(backlogResult, UPPER_BOUNDS);
        assertThat(fourth.logFiles()).containsExactlyInAnyOrderElementsOf(backlogLogs);
        assertThat(fourth.effectiveUpperBounds()).isEqualTo(UPPER_BOUNDS);
        assertThat(interceptor.containsMessage("resetting log count per redo thread to 1")).isTrue();
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
