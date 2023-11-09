/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogFile;
import io.debezium.connector.oracle.logminer.LogMinerHelper;
import io.debezium.embedded.AbstractConnectorTest;

/**
 * Unit tests for the {@link LogMinerHelper} class.
 *
 * @author Chris Cranford
 */
public class LogMinerHelperTest extends AbstractConnectorTest {

    @Test
    public void testStandaloneLogStateWithOneThreadArchiveLogGap() throws Exception {
        // The test scenario is (gap, arc process 2 fell behind)
        // ARC1 - 100 to NOW - SEQ4
        // ARC1 - 080 to 090 - SEQ2
        // SEQ3 for ARC1 is missing
        // Expectation: Return false, wait needed.
        List<LogFile> files = new ArrayList<>();
        files.add(createRedoLog("redo1.log", 100, 4, 1));
        files.add(createArchiveLog("archive1.log", 80, 90, 2, 1));
        assertThat(LogMinerHelper.hasLogFilesStartingBeforeOrAtScn(files, Scn.valueOf(101))).isFalse();
    }

    @Test
    public void testStandaloneLogStateWithNoGaps() throws Exception {
        // The test scenario is (no gaps, mix of archive and redo logs with high volatility)
        // ARC1 - 100 to NOW - SEQ4
        // ARC1 - 080 to 090 - SEQ2
        // ARC1 - 090 to 100 - SEQ3
        // Expectation: Return true, no wait needed.
        List<LogFile> files = new ArrayList<>();
        files.add(createRedoLog("redo1.log", 100, 4, 1));
        files.add(createArchiveLog("archive1.log", 80, 90, 2, 1));
        files.add(createArchiveLog("archive0.log", 90, 100, 3, 1));
        assertThat(LogMinerHelper.hasLogFilesStartingBeforeOrAtScn(files, Scn.valueOf(101))).isTrue();
    }

    @Test
    public void testStandaloneLogStateWithJustOnlineLogs() throws Exception {
        // The test scenario is (no gaps, just online redo logs)
        // ARC1 - 100 to NOW - SEQ4
        // Expectation: Return true, no wait needed.
        List<LogFile> files = new ArrayList<>();
        files.add(createRedoLog("redo1.log", 100, 4, 1));
        assertThat(LogMinerHelper.hasLogFilesStartingBeforeOrAtScn(files, Scn.valueOf(101))).isTrue();
    }

    @Test
    public void testStandaloneLogStateWithMixOfArchiveAndRedoNoGaps() throws Exception {
        // The test scenario is (no gaps, mix of archive and redo logs)
        // ARC1 - 100 to NOW - SEQ4
        // ARC1 - 080 to 090 - SEQ2
        // Expectation: Return true, no wait needed.
        List<LogFile> files = new ArrayList<>();
        files.add(createRedoLog("redo1.log", 100, 4, 1));
        files.add(createArchiveLog("archive1.log", 80, 90, 3, 1));
        assertThat(LogMinerHelper.hasLogFilesStartingBeforeOrAtScn(files, Scn.valueOf(101))).isTrue();
    }

    @Test
    public void testRacLogStateWithOneThreadArchiveLogGap() throws Exception {
        // The test scenario is (gap, arc process 2 fell behind)
        // ARC1 - 100 to NOW - SEQ4
        // ARC2 - 080 to NOW - SEQ1
        // ARC1 - 080 to 090 - SEQ2
        // SEQ3 for ARC1 is missing
        // Expectation: Return false, wait needed.
        List<LogFile> files = new ArrayList<>();
        files.add(createRedoLog("redo1.log", 100, 4, 1));
        files.add(createRedoLog("redo2.log", 80, 1, 2));
        files.add(createArchiveLog("archive1.log", 80, 90, 2, 1));
        assertThat(LogMinerHelper.hasLogFilesStartingBeforeOrAtScn(files, Scn.valueOf(101))).isFalse();
    }

    @Test
    public void testRacLogStateWithNoGaps() throws Exception {
        // The test scenario is (no gaps, mix of archive and redo logs with one node volatile)
        // ARC1 - 100 to NOW - SEQ4
        // ARC2 - 080 to NOW - SEQ1
        // ARC1 - 080 to 090 - SEQ2
        // ARC1 - 090 to 100 - SEQ3
        // Expectation: Return true, no wait needed.
        List<LogFile> files = new ArrayList<>();
        files.add(createRedoLog("redo1.log", 100, 4, 1));
        files.add(createRedoLog("redo2.log", 80, 1, 2));
        files.add(createArchiveLog("archive1.log", 80, 90, 2, 1));
        files.add(createArchiveLog("archive0.log", 90, 100, 3, 1));
        assertThat(LogMinerHelper.hasLogFilesStartingBeforeOrAtScn(files, Scn.valueOf(101))).isTrue();
    }

    @Test
    public void testRacLogStateWithJustOnlineLogs() throws Exception {
        // The test scenario is (no gaps, just online redo logs)
        // ARC1 - 100 to NOW - SEQ4
        // ARC2 - 090 to NOW - SEQ3
        // Expectation: Return true, no wait needed.
        List<LogFile> files = new ArrayList<>();
        files.add(createRedoLog("redo1.log", 100, 4, 1));
        files.add(createRedoLog("redo2.log", 90, 3, 2));
        assertThat(LogMinerHelper.hasLogFilesStartingBeforeOrAtScn(files, Scn.valueOf(101))).isTrue();
    }

    @Test
    public void testRacLogStateWithMixOfArchiveAndRedoNoGaps() throws Exception {
        // The test scenario is (no gaps, mix of archive and redo logs)
        // ARC1 - 100 to NOW - SEQ4
        // ARC2 - 090 to NOW - SEQ3
        // ARC1 - 080 to 090 - SEQ2
        // Expectation: Return true, no wait needed.
        List<LogFile> files = new ArrayList<>();
        files.add(createRedoLog("redo1.log", 100, 4, 1));
        files.add(createRedoLog("redo2.log", 90, 3, 2));
        files.add(createArchiveLog("archive1.log", 80, 90, 2, 1));
        assertThat(LogMinerHelper.hasLogFilesStartingBeforeOrAtScn(files, Scn.valueOf(101))).isTrue();
    }

    @Test
    public void testRacLogStateWithMixOfArchiveAndRedoForBothThreadsNoGap() throws Exception {
        // The test scenario is (no gaps, mix of archive and redo logs on both threads, both equally active)
        // ARC1 - 100 to NOW - SEQ4
        // ARC2 - 090 to NOW - SEQ3
        // ARC1 - 080 to 090 - SEQ2
        // ARC2 - 070 to 080 - SEQ1
        // Expectation: Return true, no wait needed.
        List<LogFile> files = new ArrayList<>();
        files.add(createRedoLog("redo1.log", 100, 4, 1));
        files.add(createRedoLog("redo2.log", 90, 3, 2));
        files.add(createArchiveLog("archive1.log", 80, 90, 2, 1));
        files.add(createArchiveLog("archive2.log", 70, 80, 1, 2));
        assertThat(LogMinerHelper.hasLogFilesStartingBeforeOrAtScn(files, Scn.valueOf(101))).isTrue();
    }

    private static LogFile createRedoLog(String name, int startScn, int sequence, int threadId) {
        return new LogFile(name, Scn.valueOf(startScn), Scn.valueOf(Long.MAX_VALUE),
                BigInteger.valueOf(sequence), LogFile.Type.REDO, true, threadId);
    }

    private static LogFile createArchiveLog(String name, int startScn, int endScn, int sequence, int threadId) {
        return new LogFile(name, Scn.valueOf(startScn), Scn.valueOf(endScn),
                BigInteger.valueOf(sequence), LogFile.Type.ARCHIVE, false, threadId);
    }

}
