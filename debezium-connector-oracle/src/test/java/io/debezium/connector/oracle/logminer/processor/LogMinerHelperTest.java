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
import io.debezium.doc.FixFor;
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
        // NOTE: Oracle RAC sequences are independent across redo thread, so its perfectly valid
        // for two redo threads to reuse the same sequence value.
        List<LogFile> files = new ArrayList<>();
        files.add(createRedoLog("redo1.log", 100, 4, 1));
        files.add(createRedoLog("redo2.log", 90, 3, 2));
        files.add(createArchiveLog("archive1.log", 80, 90, 3, 1));
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
        // NOTE: Oracle RAC sequences are independent across redo thread, so its perfectly valid
        // for two redo threads to reuse the same sequence value.
        List<LogFile> files = new ArrayList<>();
        files.add(createRedoLog("redo1.log", 100, 4, 1));
        files.add(createRedoLog("redo2.log", 90, 3, 2));
        files.add(createArchiveLog("archive1.log", 80, 90, 3, 1));
        files.add(createArchiveLog("archive2.log", 70, 80, 2, 2));
        assertThat(LogMinerHelper.hasLogFilesStartingBeforeOrAtScn(files, Scn.valueOf(101))).isTrue();
    }

    @Test
    @FixFor("DBZ-7158")
    public void testOracleRacWithRealDataRedoThreadsWithIndependentSequenceRanges() throws Exception {
        List<LogFile> files = new ArrayList<>();
        files.add(createArchiveLog("thread_3_seq_56768.164992.1152789389", 10403071210665L, 10403071900071L, 56768, 3));
        files.add(createArchiveLog("thread_3_seq_56769.148871.1152791329", 10403071900071L, 10403072997229L, 56769, 3));
        files.add(createRedoLog("group_303.8411.1106167689", 10403072997229L, 56770, 3));
        files.add(createArchiveLog("thread_4_seq_62925.29497.1152789389", 10403069930249L, 10403071899909L, 62925, 4));
        files.add(createRedoLog("group_401.8418.1106167699", 10403071899909L, 62926, 4));
        files.add(createRedoLog("group_201.8400.1106167673", 10326919867728L, 10326919867733L, 78034, 2, false));
        files.add(createRedoLog("group_202.8401.1106167675", 10326919867733L, 10326919910394L, 78035, 2, false));
        files.add(createRedoLog("group_203.8402.1106167677", 10326919910394L, 10326919923665L, 78036, 2, false));
        files.add(createRedoLog("group_101.8391.1106167659", 10326919867747L, 10326919867788L, 83112, 1, false));
        files.add(createRedoLog("group_102.8392.1106167661", 10326919867788L, 10326919910390L, 83113, 1, false));
        files.add(createRedoLog("group_103.8393.1106167663", 10326919910390L, 10326919923663L, 83114, 1, false));
        assertThat(LogMinerHelper.hasLogFilesStartingBeforeOrAtScn(files, Scn.valueOf("10403071210665"))).isTrue();
    }

    private static LogFile createRedoLog(String name, long startScn, int sequence, int threadId) {
        return createRedoLog(name, startScn, Long.MAX_VALUE, sequence, threadId);
    }

    private static LogFile createRedoLog(String name, long startScn, long endScn, int sequence, int threadId) {
        return createRedoLog(name, startScn, endScn, sequence, threadId, true);
    }

    private static LogFile createRedoLog(String name, long startScn, long endScn, int sequence, int threadId, boolean current) {
        return new LogFile(name, Scn.valueOf(startScn), Scn.valueOf(Long.MAX_VALUE),
                BigInteger.valueOf(sequence), LogFile.Type.REDO, current, threadId);
    }

    private static LogFile createArchiveLog(String name, long startScn, long endScn, int sequence, int threadId) {
        return new LogFile(name, Scn.valueOf(startScn), Scn.valueOf(endScn),
                BigInteger.valueOf(sequence), LogFile.Type.ARCHIVE, false, threadId);
    }

}
