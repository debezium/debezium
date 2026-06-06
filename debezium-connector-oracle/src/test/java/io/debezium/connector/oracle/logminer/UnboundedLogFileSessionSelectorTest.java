/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.connector.oracle.RedoThreadState;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.LogFileCollector.LogFilesResult;
import io.debezium.connector.oracle.logminer.LogFileSessionSelector.SessionLogSelection;
import io.debezium.doc.FixFor;

/**
 * Unit tests for {@link UnboundedLogFileSessionSelector}.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER)
public class UnboundedLogFileSessionSelectorTest {

    private final UnboundedLogFileSessionSelector selector = new UnboundedLogFileSessionSelector();

    @Test
    @FixFor("dbz#1713")
    void testAllLogsReturnedWithOriginalUpperBounds() {
        List<LogFile> logs = List.of(
                createArchiveLog("arc1.log", 100, 200, 1, 1),
                createArchiveLog("arc2.log", 200, 300, 2, 1),
                createRedoLog("redo1.log", 300, 3, 1));

        SessionLogSelection result = selector.selectLogsForSession(
                new LogFilesResult(logs, emptyState()), Scn.valueOf(500));

        assertThat(result.logFiles()).isEqualTo(logs);
        assertThat(result.effectiveUpperBounds()).isEqualTo(Scn.valueOf(500));
    }

    @Test
    @FixFor("dbz#1713")
    void testEmptyLogListReturnedUnchanged() {
        SessionLogSelection result = selector.selectLogsForSession(
                new LogFilesResult(List.of(), emptyState()), Scn.valueOf(500));

        assertThat(result.logFiles()).isEmpty();
        assertThat(result.effectiveUpperBounds()).isEqualTo(Scn.valueOf(500));
    }

    @Test
    @FixFor("dbz#1713")
    void testUpperBoundsPreservedRegardlessOfLogContent() {
        List<LogFile> logs = List.of(
                createArchiveLog("arc1.log", 100, 900, 1, 1));

        // upper bounds of 400 is less than arc1.nextScn=900; selector must not alter it
        SessionLogSelection result = selector.selectLogsForSession(
                new LogFilesResult(logs, emptyState()), Scn.valueOf(400));

        assertThat(result.effectiveUpperBounds()).isEqualTo(Scn.valueOf(400));
    }

    private static LogFile createArchiveLog(String name, long startScn, long endScn, int seq, int thread) {
        return LogFile.forArchive(name, Scn.valueOf(startScn), Scn.valueOf(endScn), BigInteger.valueOf(seq), thread,
                1024L * 1024L * 1024L, false, false);
    }

    private static LogFile createRedoLog(String name, long startScn, int seq, int thread) {
        return LogFile.forRedo(name, Scn.valueOf(startScn), Scn.valueOf(Long.MAX_VALUE), BigInteger.valueOf(seq), true,
                thread, 1024L * 1024L * 1024L);
    }

    private static RedoThreadState emptyState() {
        return RedoThreadState.builder()
                .thread()
                .threadId(1)
                .status("OPEN")
                .enabled("PUBLIC")
                .instanceName("ORCLCDB")
                .logGroups(2L)
                .openTime(Instant.now().minus(10, ChronoUnit.MINUTES))
                .checkpointScn(Scn.valueOf(50))
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
}