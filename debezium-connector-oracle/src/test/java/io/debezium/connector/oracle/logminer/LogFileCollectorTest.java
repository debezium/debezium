/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.RedoThreadState;
import io.debezium.connector.oracle.RedoThreadState.RedoThread;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;

/**
 * Unit tests for the {@link LogFileCollector} class.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER)
public class LogFileCollectorTest {

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    private int currentQueryRow;

    @Test
    public void testStandaloneLogStateWithOneThreadArchiveLogGap() throws Exception {
        // The test scenario is (gap, arc process 2 fell behind)
        // ARC1 - 100 to NOW - SEQ4
        // ARC1 - 080 to 090 - SEQ2
        // SEQ3 for ARC1 is missing
        // Expectation: Return false, wait needed.
        final List<LogFile> files = new ArrayList<>();
        files.add(createRedoLog("redo1.log", 100, 4, 1));
        files.add(createArchiveLog("archive1.log", 80, 90, 2, 1));

        final RedoThreadState state = getSingleThreadOpenState(Scn.valueOf(50), Scn.valueOf(101));
        assertThat(getLogFileCollector(state).isLogFileListConsistent(Scn.valueOf(101), files, state)).isFalse();
    }

    @Test
    public void testStandaloneLogStateWithNoGaps() throws Exception {
        // The test scenario is (no gaps, mix of archive and redo logs with high volatility)
        // ARC1 - 100 to NOW - SEQ4
        // ARC1 - 080 to 090 - SEQ2
        // ARC1 - 090 to 100 - SEQ3
        // Expectation: Return true, no wait needed.
        final List<LogFile> files = new ArrayList<>();
        files.add(createRedoLog("redo1.log", 100, 4, 1));
        files.add(createArchiveLog("archive1.log", 80, 90, 2, 1));
        files.add(createArchiveLog("archive0.log", 90, 100, 3, 1));

        final RedoThreadState state = getSingleThreadOpenState(Scn.valueOf(50), Scn.valueOf(101));
        assertThat(getLogFileCollector(state).isLogFileListConsistent(Scn.valueOf(101), files, state)).isTrue();
    }

    @Test
    public void testStandaloneLogStateWithJustOnlineLogs() throws Exception {
        // The test scenario is (no gaps, just online redo logs)
        // ARC1 - 100 to NOW - SEQ4
        // Expectation: Return true, no wait needed.
        final List<LogFile> files = new ArrayList<>();
        files.add(createRedoLog("redo1.log", 100, 4, 1));

        final RedoThreadState state = getSingleThreadOpenState(Scn.valueOf(50), Scn.valueOf(101));
        assertThat(getLogFileCollector(state).isLogFileListConsistent(Scn.valueOf(101), files, state)).isTrue();
    }

    @Test
    public void testStandaloneLogStateWithMixOfArchiveAndRedoNoGaps() throws Exception {
        // The test scenario is (no gaps, mix of archive and redo logs)
        // ARC1 - 100 to NOW - SEQ4
        // ARC1 - 080 to 090 - SEQ2
        // Expectation: Return true, no wait needed.
        final List<LogFile> files = new ArrayList<>();
        files.add(createRedoLog("redo1.log", 100, 4, 1));
        files.add(createArchiveLog("archive1.log", 80, 90, 3, 1));

        final RedoThreadState state = getSingleThreadOpenState(Scn.valueOf(50), Scn.valueOf(101));
        assertThat(getLogFileCollector(state).isLogFileListConsistent(Scn.valueOf(101), files, state)).isTrue();
    }

    @Test
    public void testRacLogStateWithOneThreadArchiveLogGap() throws Exception {
        // The test scenario is (gap, arc process 2 fell behind)
        // ARC1 - 100 to NOW - SEQ4
        // ARC2 - 080 to NOW - SEQ1
        // ARC1 - 080 to 090 - SEQ2
        // SEQ3 for ARC1 is missing
        // Expectation: Return false, wait needed.
        final List<LogFile> files = new ArrayList<>();
        files.add(createRedoLog("redo1.log", 100, 4, 1));
        files.add(createRedoLog("redo2.log", 80, 1, 2));
        files.add(createArchiveLog("archive1.log", 80, 90, 2, 1));

        final RedoThreadState state = getSingleThreadOpenState(Scn.valueOf(50), Scn.valueOf(101));
        assertThat(getLogFileCollector(state).isLogFileListConsistent(Scn.valueOf(101), files, state)).isFalse();
    }

    @Test
    public void testRacLogStateWithNoGaps() throws Exception {
        // The test scenario is (no gaps, mix of archive and redo logs with one node volatile)
        // ARC1 - 100 to NOW - SEQ4
        // ARC2 - 080 to NOW - SEQ1
        // ARC1 - 080 to 090 - SEQ2
        // ARC1 - 090 to 100 - SEQ3
        // Expectation: Return true, no wait needed.
        final List<LogFile> files = new ArrayList<>();
        files.add(createRedoLog("redo1.log", 100, 4, 1));
        files.add(createRedoLog("redo2.log", 80, 1, 2));
        files.add(createArchiveLog("archive1.log", 80, 90, 2, 1));
        files.add(createArchiveLog("archive0.log", 90, 100, 3, 1));

        final RedoThreadState state = getTwoThreadOpenState(Scn.valueOf(50), Scn.valueOf(101), Scn.valueOf(50), Scn.valueOf(95));
        assertThat(getLogFileCollector(state).isLogFileListConsistent(Scn.valueOf(101), files, state)).isTrue();
    }

    @Test
    public void testRacLogStateWithJustOnlineLogs() throws Exception {
        // The test scenario is (no gaps, just online redo logs)
        // ARC1 - 100 to NOW - SEQ4
        // ARC2 - 090 to NOW - SEQ3
        // Expectation: Return true, no wait needed.
        final List<LogFile> files = new ArrayList<>();
        files.add(createRedoLog("redo1.log", 100, 4, 1));
        files.add(createRedoLog("redo2.log", 90, 3, 2));

        final RedoThreadState state = getTwoThreadOpenState(Scn.valueOf(50), Scn.valueOf(101), Scn.valueOf(50), Scn.valueOf(95));
        assertThat(getLogFileCollector(state).isLogFileListConsistent(Scn.valueOf(101), files, state)).isTrue();
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
        final List<LogFile> files = new ArrayList<>();
        files.add(createRedoLog("redo1.log", 100, 4, 1));
        files.add(createRedoLog("redo2.log", 90, 3, 2));
        files.add(createArchiveLog("archive1.log", 80, 90, 3, 1));

        final RedoThreadState state = getTwoThreadOpenState(Scn.valueOf(50), Scn.valueOf(101), Scn.valueOf(50), Scn.valueOf(95));
        assertThat(getLogFileCollector(state).isLogFileListConsistent(Scn.valueOf(101), files, state)).isTrue();
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
        final List<LogFile> files = new ArrayList<>();
        files.add(createRedoLog("redo1.log", 100, 4, 1));
        files.add(createRedoLog("redo2.log", 90, 3, 2));
        files.add(createArchiveLog("archive1.log", 80, 90, 3, 1));
        files.add(createArchiveLog("archive2.log", 70, 80, 2, 2));

        final RedoThreadState state = getTwoThreadOpenState(Scn.valueOf(50), Scn.valueOf(101), Scn.valueOf(50), Scn.valueOf(95));
        assertThat(getLogFileCollector(state).isLogFileListConsistent(Scn.valueOf(101), files, state)).isTrue();
    }

    @Test
    @FixFor("DBZ-7158")
    public void testOracleRacWithRealDataRedoThreadsWithIndependentSequenceRanges() throws Exception {
        final List<LogFile> files = new ArrayList<>();
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

        final RedoThreadState state = getFourThreadOpenState(Scn.valueOf(10326919867700L), Scn.valueOf(10326919867700L));
        assertThat(getLogFileCollector(state).isLogFileListConsistent(Scn.valueOf("10403071210665"), files, state)).isTrue();
    }

    @Test
    @FixFor("DBZ-7345")
    public void testOracleLogDeduplicationDoesNotCreateLogSequenceGaps() throws Exception {
        final List<LogFile> archiveLogs = new ArrayList<>();
        archiveLogs.add(createArchiveLog("thread_2_seq_19.102432.1157630513", 10420065077951L, 10420065814784L, 19, 2));
        archiveLogs.add(createArchiveLog("thread_2_seq_20.114925.1157630517", 10420065814784L, 10420065815599L, 20, 2));
        archiveLogs.add(createArchiveLog("thread_2_seq_21.53484.1157630833", 10420065815599L, 10420066540859L, 21, 2));
        archiveLogs.add(createArchiveLog("thread_4_seq_21.59179.1157630515", 10420065076746L, 10420065815199L, 21, 4));
        archiveLogs.add(createArchiveLog("thread_2_seq_22.120906.1157630835", 10420066540859L, 10420066542368L, 22, 2));
        archiveLogs.add(createArchiveLog("thread_4_seq_22.86088.1157630831", 10420065815199L, 10420066540519L, 22, 4));
        archiveLogs.add(createArchiveLog("thread_2_seq_23.54067.1157631169", 10420066542368L, 10420067324882L, 23, 2));
        archiveLogs.add(createArchiveLog("thread_4_seq_23.66567.1157630831", 10420066540519L, 10420066540569L, 23, 4));
        archiveLogs.add(createArchiveLog("thread_2_seq_24.35831.1157631171", 10420067324882L, 10420067342483L, 24, 2));
        archiveLogs.add(createArchiveLog("thread_4_seq_24.131089.1157630833", 10420066540569L, 10420066542115L, 24, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_25.4458.1157630515", 10420065078678L, 10420065815228L, 25, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_25.59227.1157631307", 10420067342483L, 10420067425167L, 25, 2));
        archiveLogs.add(createArchiveLog("thread_4_seq_25.59432.1157630833", 10420066542115L, 10420066542288L, 25, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_26.9315.1157630833", 10420065815228L, 10420066542054L, 26, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_26.84905.1157631323", 10420067425167L, 10420067425169L, 26, 2));
        archiveLogs.add(createArchiveLog("thread_4_seq_26.123919.1157631169", 10420066542288L, 10420067325186L, 26, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_27.135154.1157630837", 10420066542054L, 10420066542916L, 27, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_27.10328.1157631323", 10420067425169L, 10420067428835L, 27, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_27.122959.1157630515", 10420065812775L, 10420065815149L, 27, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_27.120691.1157631173", 10420067325186L, 10420067342632L, 27, 4));

        archiveLogs.add(createArchiveLog("thread_1_seq_28.7595.1157631169", 10420066542916L, 10420067325434L, 28, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_28.109371.1157631373", 10420067713959L, 10420067851400L, 28, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_28.120056.1157630515", 10420065815149L, 10420065815184L, 28, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_28.84825.1157631225", 10420067342632L, 10420067423592L, 28, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_29.24103.1157631173", 10420067325434L, 10420067342701L, 29, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_29.82045.1157631377", 10420067851400L, 10420067861865L, 29, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_29.87835.1157630833", 10420065815184L, 10420066542076L, 29, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_29.109113.1157631373", 10420067425882L, 10420067849955L, 29, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_30.54031.1157631225", 10420067342701L, 10420067422978L, 30, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_30.119798.1157631757", 10420067861865L, 10420070147133L, 30, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_30.106606.1157630837", 10420066542076L, 10420066542953L, 30, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_30.100166.1157631377", 10420067849955L, 10420067860620L, 30, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_31.129034.1157631373", 10420067425883L, 10420067846058L, 31, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_31.55427.1157631761", 10420070147133L, 10420070148475L, 31, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_31.44281.1157631169", 10420066542953L, 10420067324675L, 31, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_31.50230.1157631757", 10420067860620L, 10420070146856L, 31, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_32.92731.1157631375", 10420067846058L, 10420067856936L, 32, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_32.140267.1157632213", 10420070148475L, 10420074094700L, 32, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_32.30585.1157631169", 10420067324675L, 10420067324714L, 32, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_32.70775.1157631757", 10420070146856L, 10420070146866L, 32, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_33.71544.1157631757", 10420067856936L, 10420070146862L, 33, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_33.100434.1157632215", 10420074094700L, 10420074096775L, 33, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_33.22432.1157631169", 10420067324714L, 10420067325533L, 33, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_33.72768.1157631759", 10420070146866L, 10420070147905L, 33, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_34.113735.1157631759", 10420070146862L, 10420070148168L, 34, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_34.106128.1157632467", 10420074096775L, 10420075671930L, 34, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_34.38114.1157631171", 10420067325533L, 10420067325832L, 34, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_34.8103.1157631759", 10420070147905L, 10420070147916L, 34, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_35.33557.1157632213", 10420070148168L, 10420074095682L, 35, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_35.89338.1157632723", 10420075671930L, 10420076628781L, 35, 2));

        archiveLogs.add(createArchiveLog("thread_3_seq_35.59739.1157631227", 10420067325832L, 10420067424522L, 35, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_35.32891.1157632211", 10420070147916L, 10420074093989L, 35, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_36.120897.1157632465", 10420074095682L, 10420075671201L, 36, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_36.81359.1157632729", 10420076628781L, 10420076630744L, 36, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_36.41748.1157631323", 10420067425165L, 10420067430211L, 36, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_36.6997.1157632211", 10420074093989L, 10420074094538L, 36, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_37.133273.1157632469", 10420075671201L, 10420075672196L, 37, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_37.40078.1157632999", 10420076630744L, 10420077691827L, 37, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_37.69126.1157631371", 10420067430211L, 10420067844161L, 37, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_37.122124.1157632213", 10420074094538L, 10420074095336L, 37, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_38.35081.1157632723", 10420075672196L, 10420076628899L, 38, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_38.74033.1157633001", 10420077691827L, 10420077693345L, 38, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_38.64108.1157631371", 10420067844161L, 10420067844606L, 38, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_38.29494.1157632213", 10420074095336L, 10420074095462L, 38, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_39.130389.1157632727", 10420076628899L, 10420076630269L, 39, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_39.18023.1157633291", 10420077693345L, 10420078830980L, 39, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_39.112450.1157631373", 10420067844606L, 10420067852168L, 39, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_39.127429.1157632465", 10420074095462L, 10420075671144L, 39, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_40.30450.1157633001", 10420076630269L, 10420077693063L, 40, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_40.129975.1157633293", 10420078830980L, 10420078832377L, 40, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_40.127146.1157631375", 10420067852168L, 10420067853759L, 40, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_40.78124.1157632471", 10420075671144L, 10420075707251L, 40, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_41.4562.1157633003", 10420077693063L, 10420077693813L, 41, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_41.140473.1157633593", 10420078832377L, 10420079945704L, 41, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_41.130094.1157631759", 10420067853759L, 10420070147674L, 41, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_41.104351.1157632723", 10420075707251L, 10420076628892L, 41, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_42.14624.1157633289", 10420077693813L, 10420078829726L, 42, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_42.102467.1157633597", 10420079945704L, 10420079965318L, 42, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_42.48568.1157631761", 10420070147674L, 10420070148572L, 42, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_42.98502.1157632727", 10420076628892L, 10420076630222L, 42, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_43.122014.1157633291", 10420078829726L, 10420078831834L, 43, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_43.115777.1157633939", 10420079965318L, 10420081194718L, 43, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_43.121381.1157632213", 10420070148572L, 10420074094949L, 43, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_43.75656.1157632999", 10420076630222L, 10420077692782L, 43, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_44.120905.1157633593", 10420078831834L, 10420079945242L, 44, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_44.130044.1157633959", 10420081194718L, 10420081325309L, 44, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_44.24262.1157632215", 10420074094949L, 10420074097130L, 44, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_44.88613.1157633003", 10420077692782L, 10420077693742L, 44, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_45.39828.1157633595", 10420079945242L, 10420079946402L, 45, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_45.27936.1157635485", 10420084899385L, 10420086011118L, 45, 2));

        archiveLogs.add(createArchiveLog("thread_3_seq_45.102964.1157632465", 10420074097130L, 10420075670913L, 45, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_45.124098.1157633289", 10420077693742L, 10420078829579L, 45, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_46.134889.1157633905", 10420079946402L, 10420081111693L, 46, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_46.137752.1157635489", 10420086011118L, 10420086028574L, 46, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_46.114029.1157632465", 10420075670913L, 10420075670920L, 46, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_46.51944.1157633289", 10420078829579L, 10420078829656L, 46, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_47.140243.1157635487", 10420085125081L, 10420086027594L, 47, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_47.6057.1157636047", 10420086028574L, 10420087792108L, 47, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_47.124841.1157632467", 10420075670920L, 10420075671889L, 47, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_47.66253.1157633291", 10420078829656L, 10420078830848L, 47, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_48.137137.1157635489", 10420086027594L, 10420086028890L, 48, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_48.97382.1157636051", 10420087792108L, 10420087793213L, 48, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_48.18078.1157632469", 10420075671889L, 10420075672029L, 48, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_48.125320.1157633295", 10420078830848L, 10420078832806L, 48, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_49.23059.1157636047", 10420086028890L, 10420087792848L, 49, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_49.81467.1157636621", 10420087793213L, 10420089461069L, 49, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_49.141977.1157632721", 10420075672029L, 10420076628663L, 49, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_49.90810.1157633595", 10420078832806L, 10420079946327L, 49, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_50.85022.1157636051", 10420087792848L, 10420087809051L, 50, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_50.141996.1157637179", 10420089461069L, 10420091136572L, 50, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_50.38149.1157632725", 10420076628663L, 10420076628922L, 50, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_50.3369.1157633937", 10420079946327L, 10420081194004L, 50, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_51.111749.1157636619", 10420087809051L, 10420089459963L, 51, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_51.24895.1157637183", 10420091136572L, 10420091138177L, 51, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_51.123161.1157632725", 10420076628922L, 10420076629803L, 51, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_51.99472.1157633939", 10420081194004L, 10420081195494L, 51, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_52.44136.1157636621", 10420089459963L, 10420089478064L, 52, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_52.106893.1157637739", 10420091138177L, 10420092857952L, 52, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_52.93969.1157632727", 10420076629803L, 10420076630165L, 52, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_52.82230.1157634351", 10420081195494L, 10420082615011L, 52, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_53.48353.1157637177", 10420089478064L, 10420091135940L, 53, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_53.126740.1157637743", 10420092857952L, 10420092859810L, 53, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_53.119804.1157632999", 10420076630165L, 10420077691804L, 53, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_53.125337.1157634355", 10420082615011L, 10420082616544L, 53, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_54.139258.1157637181", 10420091135940L, 10420091137353L, 54, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_54.108123.1157638315", 10420092859810L, 10420094712945L, 54, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_54.38764.1157632999", 10420077691804L, 10420077691860L, 54, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_54.22206.1157634919", 10420082616544L, 10420084326020L, 54, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_55.122376.1157637741", 10420091137353L, 10420092859422L, 55, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_55.15984.1157638317", 10420094712945L, 10420094718445L, 55, 2));

        archiveLogs.add(createArchiveLog("thread_3_seq_55.99666.1157633001", 10420077691860L, 10420077693075L, 55, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_55.112387.1157634919", 10420084326020L, 10420084326059L, 55, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_56.127425.1157637745", 10420092859422L, 10420092860008L, 56, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_56.14445.1157638875", 10420094718445L, 10420096501225L, 56, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_56.37529.1157633001", 10420077693075L, 10420077693194L, 56, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_56.44015.1157634921", 10420084326059L, 10420084327448L, 56, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_57.127805.1157638315", 10420092860008L, 10420094713646L, 57, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_57.102723.1157638879", 10420096501225L, 10420096507445L, 57, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_57.36874.1157633291", 10420077693194L, 10420078830884L, 57, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_57.132878.1157634921", 10420084327448L, 10420084337112L, 57, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_58.110863.1157638877", 10420094713646L, 10420096506682L, 58, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_58.44214.1157639443", 10420096507445L, 10420098267740L, 58, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_58.89762.1157633297", 10420078830884L, 10420078833501L, 58, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_58.121953.1157635485", 10420084337112L, 10420086027306L, 58, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_59.7997.1157639445", 10420096506682L, 10420098271317L, 59, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_59.110851.1157639445", 10420098267740L, 10420098276507L, 59, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_59.26408.1157633593", 10420078833501L, 10420079945253L, 59, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_59.22154.1157635489", 10420086027306L, 10420086028699L, 59, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_60.103861.1157639447", 10420098271317L, 10420098278635L, 60, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_60.31902.1157640017", 10420098276507L, 10420100107750L, 60, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_60.138346.1157633593", 10420079945253L, 10420079945268L, 60, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_60.117681.1157636047", 10420086028699L, 10420087792353L, 60, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_61.79447.1157640015", 10420098278635L, 10420100102682L, 61, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_61.122004.1157640583", 10420100107750L, 10420101461000L, 61, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_61.88914.1157633593", 10420079945268L, 10420079945939L, 61, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_61.44134.1157636051", 10420087792353L, 10420087793313L, 61, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_62.37087.1157640017", 10420100102682L, 10420100108238L, 62, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_62.113836.1157641153", 10420101461000L, 10420102475962L, 62, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_62.118530.1157633595", 10420079945939L, 10420079946226L, 62, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_62.16260.1157636617", 10420087793313L, 10420089459527L, 62, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_63.97742.1157640581", 10420100108238L, 10420101459317L, 63, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_63.104425.1157641157", 10420102475962L, 10420102497318L, 63, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_63.41113.1157633935", 10420079946226L, 10420081193870L, 63, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_63.118937.1157636621", 10420089459527L, 10420089461185L, 63, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_64.113677.1157640585", 10420101459317L, 10420101461614L, 64, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_64.27584.1157641725", 10420102497318L, 10420103646932L, 64, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_64.130307.1157633937", 10420081193870L, 10420081193899L, 64, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_64.35292.1157637179", 10420089461185L, 10420091136632L, 64, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_65.11516.1157641155", 10420101461614L, 10420102476833L, 65, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_65.108105.1157641727", 10420103646932L, 10420103648873L, 65, 2));

        archiveLogs.add(createArchiveLog("thread_3_seq_65.129080.1157633937", 10420081193899L, 10420081194341L, 65, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_65.141640.1157637183", 10420091136632L, 10420091138266L, 65, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_66.12035.1157641725", 10420102476833L, 10420103647876L, 66, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_66.38050.1157642305", 10420103648873L, 10420104770781L, 66, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_66.2139.1157633937", 10420081194341L, 10420081194356L, 66, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_66.44568.1157637741", 10420091138266L, 10420092858137L, 66, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_67.128895.1157641729", 10420103647876L, 10420103649534L, 67, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_67.101762.1157642307", 10420104770781L, 10420104784915L, 67, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_67.129079.1157634351", 10420081194356L, 10420082614892L, 67, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_67.138035.1157637743", 10420092858137L, 10420092859900L, 67, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_68.111474.1157641731", 10420103649534L, 10420103688706L, 68, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_68.52839.1157642873", 10420104784915L, 10420105780862L, 68, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_68.52758.1157634351", 10420082614892L, 10420082614900L, 68, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_68.23909.1157638313", 10420092859900L, 10420094712992L, 68, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_69.136791.1157642305", 10420103688706L, 10420104771374L, 69, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_69.90082.1157643443", 10420105780862L, 10420106935292L, 69, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_69.92756.1157634353", 10420082614900L, 10420082615615L, 69, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_69.125507.1157638317", 10420094712992L, 10420094718451L, 69, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_70.126851.1157642873", 10420104771374L, 10420105780550L, 70, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_70.136802.1157643447", 10420106935292L, 10420106936815L, 70, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_70.112383.1157634353", 10420082615615L, 10420082615756L, 70, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_70.122141.1157638875", 10420094718451L, 10420096501109L, 70, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_71.38242.1157643443", 10420105780550L, 10420106934351L, 71, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_71.123080.1157644015", 10420106936815L, 10420108054239L, 71, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_71.125835.1157634919", 10420082615756L, 10420084326521L, 71, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_71.54371.1157638875", 10420096501109L, 10420096501125L, 71, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_72.141587.1157643445", 10420106934351L, 10420106936169L, 72, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_72.101983.1157644017", 10420108054239L, 10420108055164L, 72, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_72.38011.1157634923", 10420084326521L, 10420084337337L, 72, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_72.101993.1157638875", 10420096501125L, 10420096506348L, 72, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_73.36169.1157644017", 10420106936169L, 10420108054923L, 73, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_73.115369.1157644589", 10420108055164L, 10420109109244L, 73, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_73.54560.1157635485", 10420084337337L, 10420086011093L, 73, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_73.138263.1157638877", 10420096506348L, 10420096506595L, 73, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_74.117972.1157644589", 10420108054923L, 10420109113118L, 74, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_74.123425.1157644591", 10420109109244L, 10420109113734L, 74, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_74.109020.1157635485", 10420086011093L, 10420086011193L, 74, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_74.83271.1157639443", 10420096506595L, 10420098267554L, 74, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_75.137426.1157644593", 10420109113118L, 10420109114445L, 75, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_75.98500.1157645155", 10420109113734L, 10420110227688L, 75, 2));

        archiveLogs.add(createArchiveLog("thread_3_seq_75.10668.1157635487", 10420086011193L, 10420086028090L, 75, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_75.39786.1157639445", 10420098267554L, 10420098276212L, 75, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_76.73067.1157645157", 10420109114445L, 10420110228780L, 76, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_76.10230.1157645159", 10420110227688L, 10420110228915L, 76, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_76.26059.1157635487", 10420086028090L, 10420086028270L, 76, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_76.79359.1157640017", 10420098276212L, 10420100107710L, 76, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_77.27734.1157645737", 10420110228780L, 10420111311806L, 77, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_77.122709.1157645735", 10420110228915L, 10420111310725L, 77, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_77.134830.1157636045", 10420086028270L, 10420087705035L, 77, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_77.45801.1157640581", 10420100107710L, 10420101459104L, 77, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_78.104445.1157646319", 10420111311806L, 10420112387937L, 78, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_78.14094.1157645737", 10420111310725L, 10420111312262L, 78, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_78.127190.1157636047", 10420087705035L, 10420087791331L, 78, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_78.110733.1157640581", 10420101459104L, 10420101459169L, 78, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_79.5851.1157646321", 10420112387937L, 10420112389254L, 79, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_79.71268.1157646319", 10420111312262L, 10420112388551L, 79, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_79.93988.1157636047", 10420087791331L, 10420087792826L, 79, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_79.49211.1157640583", 10420101459169L, 10420101459877L, 79, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_80.111524.1157646889", 10420112389254L, 10420113499045L, 80, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_80.115890.1157646889", 10420112388551L, 10420113499529L, 80, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_80.113082.1157636051", 10420087792826L, 10420087793378L, 80, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_80.53039.1157640583", 10420101459877L, 10420101460583L, 80, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_81.131574.1157646893", 10420113499045L, 10420113525629L, 81, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_81.126163.1157646893", 10420113499529L, 10420113525895L, 81, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_81.54611.1157636619", 10420087793378L, 10420089459503L, 81, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_81.137511.1157641153", 10420101460583L, 10420102475246L, 81, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_82.43775.1157647475", 10420113525629L, 10420114666848L, 82, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_82.19591.1157647477", 10420113525895L, 10420114667513L, 82, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_82.143485.1157636619", 10420089459503L, 10420089459900L, 82, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_82.45175.1157641153", 10420102475246L, 10420102475315L, 82, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_83.114404.1157647477", 10420114666848L, 10420114670309L, 83, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_83.90637.1157648061", 10420114667513L, 10420115745936L, 83, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_83.123303.1157636619", 10420089459900L, 10420089460711L, 83, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_83.78441.1157641153", 10420102475315L, 10420102476039L, 83, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_84.81055.1157648059", 10420114670309L, 10420115745416L, 84, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_84.43640.1157648635", 10420115745936L, 10420116881973L, 84, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_84.127400.1157636621", 10420089460711L, 10420089460855L, 84, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_84.108657.1157641155", 10420102476039L, 10420102476322L, 84, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_85.19878.1157648637", 10420115745416L, 10420116882527L, 85, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_85.79178.1157648637", 10420116881973L, 10420116882898L, 85, 2));

        archiveLogs.add(createArchiveLog("thread_3_seq_85.102743.1157637177", 10420089460855L, 10420091135914L, 85, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_85.100544.1157641727", 10420102476322L, 10420103648723L, 85, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_86.74638.1157649213", 10420116882527L, 10420117949875L, 86, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_86.6286.1157649211", 10420116882898L, 10420117949289L, 86, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_86.143302.1157637177", 10420091135914L, 10420091135923L, 86, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_86.121601.1157642303", 10420103648723L, 10420104770724L, 86, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_87.24892.1157649789", 10420117949875L, 10420119045017L, 87, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_87.129945.1157649215", 10420117949289L, 10420117950394L, 87, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_87.2122.1157637179", 10420091135923L, 10420091136550L, 87, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_87.10413.1157642303", 10420104770724L, 10420104770760L, 87, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_88.139446.1157650361", 10420119045017L, 10420120163858L, 88, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_88.31456.1157649787", 10420117950394L, 10420119043709L, 88, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_88.137267.1157637183", 10420091136550L, 10420091138147L, 88, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_88.129513.1157642305", 10420104770760L, 10420104771343L, 88, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_89.119446.1157650363", 10420120163858L, 10420120165554L, 89, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_89.129902.1157649791", 10420119043709L, 10420119045838L, 89, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_89.127013.1157637741", 10420091138147L, 10420092857917L, 89, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_89.103562.1157642305", 10420104771343L, 10420104784730L, 89, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_90.104227.1157650939", 10420120165554L, 10420121243827L, 90, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_90.41957.1157650361", 10420119045838L, 10420120165100L, 90, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_90.130302.1157637741", 10420092857917L, 10420092858116L, 90, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_90.112710.1157642871", 10420104784730L, 10420105780069L, 90, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_91.89637.1157650941", 10420121243827L, 10420121244907L, 91, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_91.113816.1157650941", 10420120165100L, 10420121244243L, 91, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_91.34425.1157637741", 10420092858116L, 10420092859313L, 91, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_91.107452.1157642871", 10420105780069L, 10420105780082L, 91, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_92.68628.1157651525", 10420121244907L, 10420122392706L, 92, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_92.106137.1157651525", 10420121244243L, 10420122393416L, 92, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_92.121273.1157637743", 10420092859313L, 10420092859713L, 92, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_92.14944.1157642873", 10420105780082L, 10420105780503L, 92, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_93.142290.1157651527", 10420122392706L, 10420122412683L, 93, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_93.46324.1157652107", 10420122393416L, 10420123553884L, 93, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_93.108198.1157638313", 10420092859713L, 10420094712895L, 93, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_93.20140.1157642873", 10420105780503L, 10420105780698L, 93, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_94.74696.1157652107", 10420122412683L, 10420123552384L, 94, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_94.119480.1157652693", 10420123553884L, 10420124748935L, 94, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_94.7689.1157638313", 10420094712895L, 10420094712938L, 94, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_94.25883.1157643441", 10420105780698L, 10420106934041L, 94, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_95.138733.1157652109", 10420123552384L, 10420123557795L, 95, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_95.119977.1157652695", 10420124748935L, 10420124750348L, 95, 2));

        archiveLogs.add(createArchiveLog("thread_3_seq_95.134009.1157638315", 10420094712938L, 10420094713586L, 95, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_95.119836.1157643443", 10420106934041L, 10420106934075L, 95, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_96.44729.1157652693", 10420123557795L, 10420124748132L, 96, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_96.102814.1157653275", 10420124750348L, 10420126350034L, 96, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_96.123557.1157638315", 10420094713586L, 10420094713623L, 96, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_96.106100.1157643443", 10420106934075L, 10420106935245L, 96, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_97.88433.1157652695", 10420124748132L, 10420124749928L, 97, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_97.141003.1157653279", 10420126350034L, 10420126354169L, 97, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_97.90749.1157638877", 10420094713623L, 10420096506996L, 97, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_97.119399.1157643445", 10420106935245L, 10420106936336L, 97, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_98.108278.1157653275", 10420124749928L, 10420126349683L, 98, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_98.59615.1157653865", 10420126354169L, 10420127523467L, 98, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_98.137619.1157639441", 10420096506996L, 10420098266187L, 98, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_98.83517.1157644013", 10420106936336L, 10420108054108L, 98, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_99.123618.1157653277", 10420126349683L, 10420126353932L, 99, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_99.39953.1157654447", 10420127523467L, 10420128686090L, 99, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_99.115542.1157639443", 10420098266187L, 10420098266407L, 99, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_99.112849.1157644015", 10420108054108L, 10420108054147L, 99, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_100.46884.1157653863", 10420126353932L, 10420127520985L, 100, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_100.70465.1157654449", 10420128686090L, 10420128687284L, 100, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_100.41677.1157639443", 10420098266407L, 10420098271501L, 100, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_100.64268.1157644015", 10420108054147L, 10420108054702L, 100, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_101.20602.1157653865", 10420127520985L, 10420127528042L, 101, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_101.40043.1157655037", 10420128687284L, 10420129844700L, 101, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_101.138507.1157639445", 10420098271501L, 10420098272959L, 101, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_101.117745.1157644017", 10420108054702L, 10420108054995L, 101, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_102.2545.1157654445", 10420127528042L, 10420128659496L, 102, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_102.112909.1157655039", 10420129844700L, 10420129846508L, 102, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_102.90156.1157640013", 10420098272959L, 10420100102383L, 102, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_102.118961.1157644589", 10420108054995L, 10420109113202L, 102, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_103.93026.1157654449", 10420128659496L, 10420128686843L, 103, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_103.28193.1157655635", 10420129846508L, 10420131004710L, 103, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_103.71721.1157640013", 10420100102383L, 10420100102445L, 103, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_103.119155.1157645155", 10420109113202L, 10420110227518L, 103, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_104.126517.1157655039", 10420128686843L, 10420129846432L, 104, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_104.47692.1157656217", 10420131004710L, 10420132178551L, 104, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_104.90458.1157640015", 10420100102445L, 10420100103197L, 104, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_104.108176.1157645155", 10420110227518L, 10420110227531L, 104, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_105.111201.1157655041", 10420129846432L, 10420129847930L, 105, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_105.138695.1157656795", 10420132178551L, 10420133394816L, 105, 2));

        archiveLogs.add(createArchiveLog("thread_3_seq_105.119649.1157640015", 10420100103197L, 10420100103463L, 105, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_105.118749.1157645155", 10420110227531L, 10420110228751L, 105, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_106.128368.1157655635", 10420129847930L, 10420131004337L, 106, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_106.59732.1157656797", 10420133394816L, 10420133395582L, 106, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_106.117540.1157640581", 10420100103463L, 10420101459391L, 106, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_106.19091.1157645157", 10420110228751L, 10420110228771L, 106, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_107.51516.1157656217", 10420131004337L, 10420132178542L, 107, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_107.2979.1157657383", 10420133395582L, 10420134516305L, 107, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_107.116802.1157640585", 10420101459391L, 10420101461670L, 107, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_107.102361.1157645733", 10420110228771L, 10420111310571L, 107, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_108.49554.1157656793", 10420132178542L, 10420133394801L, 108, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_108.119203.1157657965", 10420134516305L, 10420135725573L, 108, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_108.137584.1157641153", 10420101461670L, 10420102475273L, 108, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_108.125096.1157645735", 10420111310571L, 10420111310599L, 108, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_109.119631.1157656797", 10420133394801L, 10420133395573L, 109, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_109.125758.1157657969", 10420135725573L, 10420135732557L, 109, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_109.139085.1157641155", 10420102475273L, 10420102476921L, 109, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_109.53528.1157645735", 10420111310599L, 10420111311720L, 109, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_110.46409.1157657383", 10420133395573L, 10420134516293L, 110, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_110.137548.1157658549", 10420135732557L, 10420136953938L, 110, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_110.63031.1157641725", 10420102476921L, 10420103646913L, 110, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_110.44795.1157645737", 10420111311720L, 10420111312026L, 110, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_111.140572.1157657967", 10420134516293L, 10420135725600L, 111, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_111.122618.1157658551", 10420136953938L, 10420136955886L, 111, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_111.118917.1157641725", 10420103646913L, 10420103646974L, 111, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_111.23772.1157646319", 10420111312026L, 10420112387531L, 111, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_112.137751.1157657969", 10420135725600L, 10420135732595L, 112, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_112.138227.1157659143", 10420136955886L, 10420138068407L, 112, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_112.86996.1157641725", 10420103646974L, 10420103647960L, 112, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_112.48136.1157646321", 10420112387531L, 10420112389165L, 112, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_113.140213.1157658547", 10420135732595L, 10420136954092L, 113, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_113.10083.1157659145", 10420138068407L, 10420138070167L, 113, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_113.96437.1157641727", 10420103647960L, 10420103648627L, 113, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_113.62336.1157646891", 10420112389165L, 10420113525561L, 113, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_114.14240.1157658551", 10420136954092L, 10420136955945L, 114, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_114.137550.1157659731", 10420138070167L, 10420139189998L, 114, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_114.119421.1157642305", 10420103648627L, 10420104771369L, 114, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_114.111902.1157647473", 10420113525561L, 10420114666634L, 114, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_115.48458.1157659143", 10420136955945L, 10420138068611L, 115, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_115.30082.1157659733", 10420139189998L, 10420139226478L, 115, 2));

        archiveLogs.add(createArchiveLog("thread_3_seq_115.55141.1157642873", 10420104771369L, 10420105780537L, 115, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_115.46567.1157647477", 10420114666634L, 10420114670030L, 115, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_116.121205.1157659145", 10420138068611L, 10420138070343L, 116, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_116.37150.1157660317", 10420139226478L, 10420140357102L, 116, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_116.39228.1157643443", 10420105780537L, 10420106934377L, 116, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_116.138992.1157648059", 10420114670030L, 10420115744752L, 116, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_117.140258.1157659731", 10420138070343L, 10420139190091L, 117, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_117.122622.1157660319", 10420140357102L, 10420140358767L, 117, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_117.31307.1157643445", 10420106934377L, 10420106936244L, 117, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_117.56387.1157648063", 10420115744752L, 10420115746293L, 117, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_118.137848.1157659733", 10420139190091L, 10420139226567L, 118, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_118.50377.1157660907", 10420140358767L, 10420141418127L, 118, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_118.34642.1157644017", 10420106936244L, 10420108054939L, 118, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_118.138586.1157648633", 10420115746293L, 10420116881661L, 118, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_119.41496.1157660317", 10420139226567L, 10420140357256L, 119, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_119.18920.1157661497", 10420141418127L, 10420142625613L, 119, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_119.139874.1157644587", 10420108054939L, 10420109109213L, 119, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_119.51489.1157648633", 10420116881661L, 10420116881701L, 119, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_120.25643.1157660319", 10420140357256L, 10420140358802L, 120, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_120.32240.1157661499", 10420142625613L, 10420142630154L, 120, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_120.108170.1157644587", 10420109109213L, 10420109109223L, 120, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_120.124493.1157648635", 10420116881701L, 10420116882288L, 120, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_121.101585.1157660907", 10420140358802L, 10420141418133L, 121, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_121.11439.1157662087", 10420142630154L, 10420143803330L, 121, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_121.1255.1157644589", 10420109109223L, 10420109113104L, 121, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_121.35135.1157648635", 10420116882288L, 10420116882363L, 121, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_122.89757.1157661497", 10420141418133L, 10420142629025L, 122, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_122.137148.1157662089", 10420143803330L, 10420143821112L, 122, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_122.122476.1157644593", 10420109113104L, 10420109114393L, 122, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_122.101609.1157649209", 10420116882363L, 10420117948418L, 122, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_123.131205.1157661499", 10420142629025L, 10420142630244L, 123, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_123.18983.1157662677", 10420143821112L, 10420144838408L, 123, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_123.133221.1157645157", 10420109114393L, 10420110228763L, 123, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_123.58076.1157649209", 10420117948418L, 10420117948485L, 123, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_124.58580.1157662085", 10420142630244L, 10420143803264L, 124, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_124.65386.1157662679", 10420144838408L, 10420144839942L, 124, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_124.74444.1157645735", 10420110228763L, 10420111311609L, 124, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_124.115481.1157649211", 10420117948485L, 10420117949382L, 124, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_125.107543.1157662089", 10420143803264L, 10420143821048L, 125, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_125.14591.1157663271", 10420144839942L, 10420145999790L, 125, 2));

        archiveLogs.add(createArchiveLog("thread_3_seq_125.88643.1157645739", 10420111311609L, 10420111312768L, 125, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_125.10547.1157649211", 10420117949382L, 10420117949610L, 125, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_126.133295.1157662677", 10420143821048L, 10420144838158L, 126, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_126.72996.1157663273", 10420145999790L, 10420146000748L, 126, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_126.84192.1157646317", 10420111312768L, 10420112386860L, 126, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_126.125119.1157649787", 10420117949610L, 10420119043402L, 126, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_127.92725.1157662679", 10420144838158L, 10420144839832L, 127, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_127.73188.1157663869", 10420146000748L, 10420147208883L, 127, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_127.127136.1157646317", 10420112386860L, 10420112386905L, 127, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_127.135898.1157649787", 10420119043402L, 10420119043425L, 127, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_128.136852.1157663271", 10420144839832L, 10420145999604L, 128, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_128.28943.1157663873", 10420147208883L, 10420147220332L, 128, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_128.134223.1157646319", 10420112386905L, 10420112387920L, 128, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_128.135025.1157649789", 10420119043425L, 10420119044528L, 128, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_129.71380.1157663273", 10420145999604L, 10420146000671L, 129, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_129.63519.1157664469", 10420147220332L, 10420148276679L, 129, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_129.120202.1157646321", 10420112387920L, 10420112389191L, 129, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_129.85610.1157649789", 10420119044528L, 10420119044988L, 129, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_130.64148.1157663871", 10420146000671L, 10420147219913L, 130, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_130.107594.1157664473", 10420148276679L, 10420148293267L, 130, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_130.106289.1157646889", 10420112389191L, 10420113498720L, 130, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_130.32726.1157650359", 10420119044988L, 10420120163632L, 130, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_131.110393.1157664471", 10420147219913L, 10420148293085L, 131, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_131.79365.1157665063", 10420148293267L, 10420149487659L, 131, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_131.111069.1157646889", 10420113498720L, 10420113498757L, 131, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_131.119850.1157650359", 10420120163632L, 10420120163677L, 131, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_132.84595.1157665065", 10420148293085L, 10420149507256L, 132, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_132.95878.1157665067", 10420149487659L, 10420149507658L, 132, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_132.105010.1157646889", 10420113498757L, 10420113499818L, 132, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_132.66935.1157650361", 10420120163677L, 10420120164805L, 132, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_133.45361.1157665661", 10420149507256L, 10420150757734L, 133, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_133.27036.1157665663", 10420149507658L, 10420150760818L, 133, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_133.31750.1157646891", 10420113499818L, 10420113525574L, 133, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_133.120254.1157650361", 10420120164805L, 10420120165262L, 133, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_134.132283.1157665663", 10420150757734L, 10420150761634L, 134, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_134.42630.1157665665", 10420150760818L, 10420150761751L, 134, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_134.142507.1157647473", 10420113525574L, 10420114666420L, 134, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_134.67652.1157650937", 10420120165262L, 10420121243135L, 134, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_135.129986.1157666257", 10420150761634L, 10420151879210L, 135, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_135.82206.1157666259", 10420150761751L, 10420151879802L, 135, 2));

        archiveLogs.add(createArchiveLog("thread_3_seq_135.68115.1157647473", 10420114666420L, 10420114666476L, 135, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_135.133141.1157650937", 10420121243135L, 10420121243168L, 135, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_136.39604.1157666261", 10420151879210L, 10420151881312L, 136, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_136.109106.1157666261", 10420151879802L, 10420151881428L, 136, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_136.80877.1157647475", 10420114666476L, 10420114667293L, 136, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_136.105736.1157650939", 10420121243168L, 10420121243781L, 136, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_137.35789.1157666849", 10420151881312L, 10420153087207L, 137, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_137.4590.1157666847", 10420151881428L, 10420153086416L, 137, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_137.26387.1157647477", 10420114667293L, 10420114670068L, 137, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_137.134559.1157650941", 10420121243781L, 10420121244453L, 137, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_138.64689.1157667447", 10420153087207L, 10420154271266L, 138, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_138.122929.1157666849", 10420153086416L, 10420153087592L, 138, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_138.14710.1157648059", 10420114670068L, 10420115744259L, 138, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_138.51604.1157651525", 10420121244453L, 10420122393524L, 138, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_139.141459.1157667449", 10420154271266L, 10420154272250L, 139, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_139.2160.1157667449", 10420153087592L, 10420154271827L, 139, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_139.106597.1157648059", 10420115744259L, 10420115744294L, 139, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_139.116460.1157652105", 10420122393524L, 10420123539571L, 139, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_140.29816.1157668041", 10420154272250L, 10420155661289L, 140, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_140.118591.1157667451", 10420154271827L, 10420154272634L, 140, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_140.56033.1157648059", 10420115744294L, 10420115745208L, 140, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_140.70148.1157652107", 10420123539571L, 10420123539761L, 140, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_141.103915.1157668045", 10420155661289L, 10420155662367L, 141, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_141.49608.1157668043", 10420154272634L, 10420155661872L, 141, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_141.56954.1157648059", 10420115745208L, 10420115745613L, 141, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_141.64780.1157652107", 10420123539761L, 10420123552795L, 141, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_142.48496.1157668881", 10420155662367L, 10420157334147L, 142, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_142.57864.1157668879", 10420155661872L, 10420157314403L, 142, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_142.51992.1157648635", 10420115745613L, 10420116882346L, 142, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_142.131213.1157652107", 10420123552795L, 10420123553995L, 142, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_143.118956.1157669221", 10420157334147L, 10420158072488L, 143, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_143.127463.1157668883", 10420157314403L, 10420157334398L, 143, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_143.127353.1157649213", 10420116882346L, 10420117949570L, 143, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_143.83892.1157652691", 10420123553995L, 10420124747781L, 143, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_144.81649.1157669223", 10420158072488L, 10420158090876L, 144, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_144.140571.1157669223", 10420157334398L, 10420158089916L, 144, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_144.32982.1157649787", 10420117949570L, 10420119043969L, 144, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_144.1731.1157652691", 10420124747781L, 10420124747922L, 144, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_145.140566.1157669573", 10420158090876L, 10420158700838L, 145, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_145.129353.1157669225", 10420158089916L, 10420158091238L, 145, 2));

        archiveLogs.add(createArchiveLog("thread_3_seq_145.126232.1157649791", 10420119043969L, 10420119045977L, 145, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_145.74484.1157652693", 10420124747922L, 10420124749013L, 145, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_146.68613.1157669575", 10420158700838L, 10420158701843L, 146, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_146.93061.1157669573", 10420158091238L, 10420158701343L, 146, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_146.119853.1157650361", 10420119045977L, 10420120165286L, 146, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_146.66056.1157652693", 10420124749013L, 10420124749274L, 146, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_147.110304.1157650941", 10420120165286L, 10420121244597L, 147, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_147.140329.1157653275", 10420124749274L, 10420126349994L, 147, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_147.49323.1157688341", 10420158701843L, 10420200646496L, 147, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_147.15387.1157688341", 10420158701343L, 10420200646831L, 147, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_148.109932.1157651523", 10420121244597L, 10420122392119L, 148, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_148.114525.1157653279", 10420126349994L, 10420126354150L, 148, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_148.141339.1157697631", 10420200646496L, 10420220312563L, 148, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_148.139234.1157697631", 10420200646831L, 10420220309051L, 148, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_149.14930.1157651523", 10420122392119L, 10420122392158L, 149, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_149.82617.1157653861", 10420126354150L, 10420127518189L, 149, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_149.140113.1157697755", 10420220312563L, 10420220578798L, 149, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_149.141378.1157697633", 10420220309051L, 10420220313554L, 149, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_150.136981.1157651525", 10420122392158L, 10420122393269L, 150, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_150.79831.1157653861", 10420127518189L, 10420127518335L, 150, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_150.125646.1157697889", 10420220578798L, 10420221121159L, 150, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_150.141377.1157697753", 10420220313554L, 10420220571272L, 150, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_151.37976.1157651525", 10420122393269L, 10420122393773L, 151, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_151.68027.1157653863", 10420127518335L, 10420127521360L, 151, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_151.55171.1157697893", 10420221121159L, 10420221155036L, 151, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_151.140109.1157697755", 10420220571272L, 10420220584758L, 151, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_152.93773.1157652107", 10420122393773L, 10420123540592L, 152, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_152.4181.1157653863", 10420127521360L, 10420127522794L, 152, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_152.59642.1157698031", 10420221155036L, 10420221432276L, 152, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_152.127889.1157697891", 10420220584758L, 10420221153903L, 152, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_153.9364.1157652109", 10420123540592L, 10420123555983L, 153, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_153.77752.1157654447", 10420127522794L, 10420128660049L, 153, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_153.126290.1157698031", 10420221432276L, 10420221434560L, 153, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_153.9819.1157697897", 10420221153903L, 10420221176601L, 153, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_154.87280.1157652691", 10420123555983L, 10420124747821L, 154, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_154.55041.1157654449", 10420128660049L, 10420128687080L, 154, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_154.20089.1157698181", 10420221434560L, 10420221735447L, 154, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_154.85899.1157698027", 10420221176601L, 10420221430134L, 154, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_155.110605.1157652695", 10420124747821L, 10420124749668L, 155, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_155.2784.1157655035", 10420128687080L, 10420129844512L, 155, 4));

        archiveLogs.add(createArchiveLog("thread_1_seq_155.107217.1157698185", 10420221735447L, 10420221738108L, 155, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_155.47208.1157698033", 10420221430134L, 10420221435326L, 155, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_156.130974.1157653273", 10420124749668L, 10420126349517L, 156, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_156.131285.1157655035", 10420129844512L, 10420129844559L, 156, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_156.112727.1157698379", 10420221738108L, 10420222119090L, 156, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_156.132968.1157698183", 10420221435326L, 10420221735968L, 156, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_157.33959.1157653275", 10420126349517L, 10420126349557L, 157, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_157.117172.1157655039", 10420129844559L, 10420129845525L, 157, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_157.8439.1157698383", 10420222119090L, 10420222121596L, 157, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_157.59511.1157698185", 10420221735968L, 10420221738791L, 157, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_158.122030.1157653275", 10420126349557L, 10420126350347L, 158, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_158.60282.1157655041", 10420129845525L, 10420129847213L, 158, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_158.6575.1157698661", 10420222121596L, 10420222697922L, 158, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_158.103682.1157698381", 10420221738791L, 10420222120407L, 158, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_159.131252.1157653277", 10420126350347L, 10420126353588L, 159, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_159.117160.1157655633", 10420129847213L, 10420130998819L, 159, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_159.96297.1157699079", 10420222697922L, 10420223679433L, 159, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_159.47032.1157698383", 10420222120407L, 10420222122177L, 159, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_160.79519.1157653863", 10420126353588L, 10420127518419L, 160, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_160.121403.1157655633", 10420130998819L, 10420130999167L, 160, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_160.139172.1157699503", 10420223679433L, 10420224518556L, 160, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_160.100283.1157698661", 10420222122177L, 10420222696916L, 160, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_161.63840.1157653865", 10420127518419L, 10420127525702L, 161, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_161.76940.1157655635", 10420130999167L, 10420131002120L, 161, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_161.139105.1157699505", 10420224518556L, 10420224520438L, 161, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_161.4129.1157698663", 10420222696916L, 10420222698186L, 161, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_162.109424.1157654445", 10420127525702L, 10420128659371L, 162, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_162.112244.1157655635", 10420131002120L, 10420131003076L, 162, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_162.26597.1157699917", 10420224520438L, 10420225317983L, 162, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_162.128965.1157699077", 10420222698186L, 10420223678349L, 162, 2));

        archiveLogs.add(createArchiveLog("thread_3_seq_163.91067.1157654445", 10420128659371L, 10420128659387L, 163, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_163.88845.1157656215", 10420131003076L, 10420132177021L, 163, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_163.140203.1157699919", 10420225317983L, 10420225319362L, 163, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_163.28836.1157699081", 10420223678349L, 10420223680167L, 163, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_164.86337.1157654447", 10420128659387L, 10420128686065L, 164, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_164.83777.1157656215", 10420132177021L, 10420132177036L, 164, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_164.68693.1157700347", 10420225319362L, 10420226144867L, 164, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_164.382.1157699503", 10420223680167L, 10420224519472L, 164, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_165.120060.1157654447", 10420128686065L, 10420128686343L, 165, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_165.128178.1157656217", 10420132177036L, 10420132178143L, 165, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_165.121184.1157700769", 10420226144867L, 10420227113967L, 165, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_165.125221.1157699919", 10420224519472L, 10420225318593L, 165, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_166.32304.1157655035", 10420128686343L, 10420129844956L, 166, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_166.78227.1157656217", 10420132178143L, 10420132178459L, 166, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_166.77850.1157700773", 10420227113967L, 10420227152766L, 166, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_166.118505.1157700347", 10420225318593L, 10420226145443L, 166, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_167.137477.1157655039", 10420129844956L, 10420129846575L, 167, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_167.104188.1157656795", 10420132178459L, 10420133395520L, 167, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_167.142082.1157701191", 10420227152766L, 10420227974843L, 167, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_167.107939.1157700769", 10420226145443L, 10420227114379L, 167, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_168.140457.1157655633", 10420129846575L, 10420130998950L, 168, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_168.123199.1157656799", 10420133395520L, 10420133396228L, 168, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_168.71851.1157701615", 10420227974843L, 10420228896557L, 168, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_168.132828.1157700773", 10420227114379L, 10420227153176L, 168, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_169.28005.1157655635", 10420130998950L, 10420131005125L, 169, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_169.47741.1157657381", 10420133396228L, 10420134515726L, 169, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_169.122995.1157702035", 10420228896557L, 10420229887127L, 169, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_169.17260.1157701191", 10420227153176L, 10420227975183L, 169, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_170.120845.1157656217", 10420131005125L, 10420132178634L, 170, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_170.115199.1157657385", 10420134515726L, 10420134516824L, 170, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_170.12108.1157702459", 10420229887127L, 10420230869885L, 170, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_170.112796.1157701617", 10420227975183L, 10420228896933L, 170, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_171.138730.1157656793", 10420132178634L, 10420133394790L, 171, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_171.18624.1157657965", 10420134516824L, 10420135725538L, 171, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_171.80782.1157702463", 10420230869885L, 10420230888441L, 171, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_171.50092.1157702033", 10420228896933L, 10420229885588L, 171, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_172.56148.1157656795", 10420133394790L, 10420133394832L, 172, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_172.138676.1157657965", 10420135725538L, 10420135725547L, 172, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_172.112433.1157702881", 10420230888441L, 10420232265982L, 172, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_172.137649.1157702037", 10420229885588L, 10420229887286L, 172, 2));

        archiveLogs.add(createArchiveLog("thread_3_seq_173.26317.1157656795", 10420133394832L, 10420133395301L, 173, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_173.138195.1157657967", 10420135725547L, 10420135732189L, 173, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_173.139304.1157703313", 10420232265982L, 10420233228609L, 173, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_173.90457.1157702459", 10420229887286L, 10420230873512L, 173, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_174.118324.1157656797", 10420133395301L, 10420133395597L, 174, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_174.58595.1157657967", 10420135732189L, 10420135732198L, 174, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_174.36500.1157703315", 10420233228609L, 10420233232516L, 174, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_174.27088.1157702877", 10420230873512L, 10420232264676L, 174, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_175.120195.1157657381", 10420133395597L, 10420134512247L, 175, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_175.60276.1157658545", 10420135732198L, 10420136953036L, 175, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_175.67493.1157703741", 10420233232516L, 10420234161781L, 175, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_175.94930.1157702881", 10420232264676L, 10420232266411L, 175, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_176.81650.1157657381", 10420134512247L, 10420134512293L, 176, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_176.27694.1157658549", 10420136953036L, 10420136955133L, 176, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_176.95905.1157703955", 10420234161781L, 10420234673454L, 176, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_176.94533.1157703313", 10420232266411L, 10420233229715L, 176, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_177.44852.1157657383", 10420134512293L, 10420134516217L, 177, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_177.80752.1157659141", 10420136955133L, 10420138068080L, 177, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_177.32018.1157704165", 10420234673454L, 10420235136606L, 177, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_177.85832.1157703741", 10420233229715L, 10420234162175L, 177, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_178.88046.1157657383", 10420134516217L, 10420134516260L, 178, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_178.45986.1157659141", 10420138068080L, 10420138068143L, 178, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_178.118644.1157704595", 10420235136606L, 10420236234283L, 178, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_178.118737.1157703955", 10420234162175L, 10420234673665L, 178, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_179.95376.1157657965", 10420134516260L, 10420135725634L, 179, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_179.115641.1157659143", 10420138068143L, 10420138069151L, 179, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_179.124036.1157704597", 10420236234283L, 10420236253866L, 179, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_179.124460.1157704161", 10420234673665L, 10420235135501L, 179, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_180.137108.1157657969", 10420135725634L, 10420135732604L, 180, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_180.112447.1157659143", 10420138069151L, 10420138069308L, 180, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_180.109869.1157705027", 10420236253866L, 10420237545203L, 180, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_180.116251.1157704165", 10420235135501L, 10420235136715L, 180, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_181.33631.1157658547", 10420135732604L, 10420136953100L, 181, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_181.141048.1157659731", 10420138069308L, 10420139225878L, 181, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_181.116803.1157705029", 10420237545203L, 10420237547157L, 181, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_181.30740.1157704595", 10420235136715L, 10420236234568L, 181, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_182.69554.1157658547", 10420136953100L, 10420136953147L, 182, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_182.117666.1157660315", 10420139225878L, 10420140344530L, 182, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_182.48757.1157705393", 10420237547157L, 10420238548557L, 182, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_182.64367.1157705027", 10420236234568L, 10420237545578L, 182, 2));

        archiveLogs.add(createArchiveLog("thread_3_seq_183.110504.1157658549", 10420136953147L, 10420136954456L, 183, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_183.141366.1157660315", 10420140344530L, 10420140344619L, 183, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_183.100245.1157705459", 10420238548557L, 10420238691883L, 183, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_183.48351.1157705029", 10420237545578L, 10420237547340L, 183, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_184.91860.1157658549", 10420136954456L, 10420136955270L, 184, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_184.120981.1157660317", 10420140344619L, 10420140357494L, 184, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_184.50209.1157705461", 10420238691883L, 10420238712372L, 184, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_184.103143.1157705393", 10420237547340L, 10420238549093L, 184, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_185.53040.1157659141", 10420136955270L, 10420138068252L, 185, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_185.77611.1157660317", 10420140357494L, 10420140357858L, 185, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_185.34167.1157705887", 10420238712372L, 10420239822044L, 185, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_185.16667.1157705459", 10420238549093L, 10420238692236L, 185, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_186.10020.1157659145", 10420138068252L, 10420138070053L, 186, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_186.38026.1157660905", 10420140357858L, 10420141415596L, 186, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_186.25540.1157706223", 10420239822044L, 10420240808551L, 186, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_186.47277.1157705885", 10420238692236L, 10420239816774L, 186, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_187.22245.1157659729", 10420138070053L, 10420139189656L, 187, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_187.22502.1157660905", 10420141415596L, 10420141415604L, 187, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_187.71211.1157706321", 10420240808551L, 10420241062885L, 187, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_187.57385.1157705889", 10420239816774L, 10420239824370L, 187, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_188.139470.1157659731", 10420139189656L, 10420139189705L, 188, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_188.3445.1157660905", 10420141415604L, 10420141416155L, 188, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_188.1611.1157706747", 10420241062885L, 10420242072636L, 188, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_188.8080.1157706223", 10420239824370L, 10420240807442L, 188, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_189.110794.1157659731", 10420139189705L, 10420139225772L, 189, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_189.4943.1157660907", 10420141416155L, 10420141416546L, 189, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_189.38202.1157706749", 10420242072636L, 10420242074046L, 189, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_189.8662.1157706317", 10420240807442L, 10420241060880L, 189, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_190.125900.1157659733", 10420139225772L, 10420139226267L, 190, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_190.131075.1157661495", 10420141416546L, 10420142625147L, 190, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_190.137050.1157707173", 10420242074046L, 10420243261615L, 190, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_190.104551.1157706321", 10420241060880L, 10420241083950L, 190, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_191.141201.1157660315", 10420139226267L, 10420140344907L, 191, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_191.114045.1157661495", 10420142625147L, 10420142625172L, 191, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_191.91327.1157707175", 10420243261615L, 10420243263535L, 191, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_191.50237.1157706747", 10420241083950L, 10420242073278L, 191, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_192.122203.1157660319", 10420140344907L, 10420140358673L, 192, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_192.25700.1157661497", 10420142625172L, 10420142629152L, 192, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_192.130375.1157707469", 10420243263535L, 10420244250095L, 192, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_192.30550.1157707173", 10420242073278L, 10420243262041L, 192, 2));

        archiveLogs.add(createArchiveLog("thread_3_seq_193.127207.1157660907", 10420140358673L, 10420141418026L, 193, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_193.54333.1157661497", 10420142629152L, 10420142629283L, 193, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_193.95890.1157707595", 10420244250095L, 10420244586054L, 193, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_193.140944.1157707471", 10420243262041L, 10420244266336L, 193, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_194.35514.1157661495", 10420141418026L, 10420142625217L, 194, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_194.129492.1157662085", 10420142629283L, 10420143803235L, 194, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_194.112997.1157707599", 10420244586054L, 10420244588321L, 194, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_194.130365.1157707597", 10420244266336L, 10420244586893L, 194, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_195.114413.1157661499", 10420142625217L, 10420142629980L, 195, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_195.115220.1157662089", 10420143803235L, 10420143821012L, 195, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_195.85976.1157708025", 10420244588321L, 10420245830557L, 195, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_195.122091.1157707599", 10420244586893L, 10420244588702L, 195, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_196.140009.1157662085", 10420142629980L, 10420143803041L, 196, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_196.131907.1157662675", 10420143821012L, 10420144837961L, 196, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_196.140257.1157708465", 10420245830557L, 10420246919935L, 196, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_196.111963.1157708023", 10420244588702L, 10420245829012L, 196, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_197.14009.1157662085", 10420143803041L, 10420143803091L, 197, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_197.29145.1157662675", 10420144837961L, 10420144838004L, 197, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_197.123899.1157708897", 10420246919935L, 10420248072784L, 197, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_197.17341.1157708027", 10420245829012L, 10420245869084L, 197, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_198.62572.1157662087", 10420143803091L, 10420143820273L, 198, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_198.14723.1157662677", 10420144838004L, 10420144838833L, 198, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_198.114153.1157708899", 10420248072784L, 10420248073977L, 198, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_198.103047.1157708465", 10420245869084L, 10420246920430L, 198, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_199.73022.1157662087", 10420143820273L, 10420143820482L, 199, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_199.133718.1157662677", 10420144838833L, 10420144838927L, 199, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_199.44708.1157709337", 10420248073977L, 10420249006089L, 199, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_199.120033.1157708895", 10420246920430L, 10420248071453L, 199, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_200.107164.1157662677", 10420143820482L, 10420144839175L, 200, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_200.109518.1157663269", 10420144838927L, 10420145999177L, 200, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_200.37132.1157709337", 10420249006089L, 10420249008008L, 200, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_200.137776.1157708897", 10420248071453L, 10420248073188L, 200, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_201.45033.1157663273", 10420144839175L, 10420146000370L, 201, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_201.38298.1157663269", 10420145999177L, 10420145999225L, 201, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_201.30398.1157709647", 10420249008008L, 10420249768659L, 201, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_201.84524.1157709335", 10420248073188L, 10420249006650L, 201, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_202.40746.1157663869", 10420146000370L, 10420147208741L, 202, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_202.39697.1157663271", 10420145999225L, 10420145999984L, 202, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_202.42457.1157709769", 10420249768659L, 10420250161245L, 202, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_202.135368.1157709339", 10420249006650L, 10420249008481L, 202, 2));

        archiveLogs.add(createArchiveLog("thread_3_seq_203.25564.1157663869", 10420147208741L, 10420147208774L, 203, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_203.20506.1157663273", 10420145999984L, 10420146000343L, 203, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_203.30479.1157710211", 10420250161245L, 10420251335167L, 203, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_203.65813.1157709645", 10420249008481L, 10420249763450L, 203, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_204.48969.1157663871", 10420147208774L, 10420147209677L, 204, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_204.115991.1157663871", 10420146000343L, 10420147209742L, 204, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_204.47748.1157710213", 10420251335167L, 10420251336476L, 204, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_204.72982.1157709767", 10420249763450L, 10420250159356L, 204, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_205.40920.1157663873", 10420147209677L, 10420147220406L, 205, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_205.50055.1157663873", 10420147209742L, 10420147220666L, 205, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_205.58226.1157710647", 10420251336476L, 10420252388916L, 205, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_205.14324.1157709771", 10420250159356L, 10420250161606L, 205, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_206.49603.1157664471", 10420147220406L, 10420148276847L, 206, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_206.123638.1157664469", 10420147220666L, 10420148276465L, 206, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_206.120588.1157710649", 10420252388916L, 10420252397156L, 206, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_206.108452.1157710213", 10420250161606L, 10420251335538L, 206, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_207.82349.1157664473", 10420148276847L, 10420148293428L, 207, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_207.112846.1157664469", 10420148276465L, 10420148276484L, 207, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_207.123961.1157711099", 10420252397156L, 10420253580790L, 207, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_207.68662.1157710649", 10420251335538L, 10420252391492L, 207, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_208.73997.1157665063", 10420148293428L, 10420149487629L, 208, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_208.50610.1157664471", 10420148276484L, 10420148277345L, 208, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_208.66162.1157711375", 10420253580790L, 10420254213387L, 208, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_208.133160.1157710651", 10420252391492L, 10420252397540L, 208, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_209.107204.1157665065", 10420149487629L, 10420149487713L, 209, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_209.40136.1157664471", 10420148277345L, 10420148277566L, 209, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_209.23624.1157711547", 10420254213387L, 10420254783194L, 209, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_209.113453.1157711101", 10420252397540L, 10420253581628L, 209, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_210.96661.1157665065", 10420149487713L, 10420149488732L, 210, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_210.123522.1157665065", 10420148277566L, 10420149488337L, 210, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_210.29242.1157711549", 10420254783194L, 10420254785489L, 210, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_210.53448.1157711377", 10420253581628L, 10420254213617L, 210, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_211.138072.1157665065", 10420149488732L, 10420149488992L, 211, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_211.49094.1157665067", 10420149488337L, 10420149508260L, 211, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_211.98925.1157711989", 10420254785489L, 10420255835278L, 211, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_211.115053.1157711547", 10420254213617L, 10420254783755L, 211, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_212.128792.1157665659", 10420149488992L, 10420150757228L, 212, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_212.5871.1157665661", 10420149508260L, 10420150757420L, 212, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_212.80152.1157711991", 10420255835278L, 10420255859848L, 212, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_212.58137.1157711551", 10420254783755L, 10420254786124L, 212, 2));

        archiveLogs.add(createArchiveLog("thread_3_seq_213.84025.1157665659", 10420150757228L, 10420150757248L, 213, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_213.123270.1157665663", 10420150757420L, 10420150761550L, 213, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_213.55175.1157712429", 10420255859848L, 10420256969427L, 213, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_213.78853.1157711989", 10420254786124L, 10420255835966L, 213, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_214.7896.1157665661", 10420150757248L, 10420150761190L, 214, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_214.137151.1157666257", 10420150761550L, 10420151879100L, 214, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_214.6151.1157712431", 10420256969427L, 10420256971332L, 214, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_214.27414.1157712427", 10420255835966L, 10420256966540L, 214, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_215.7742.1157665665", 10420150761190L, 10420150761794L, 215, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_215.85557.1157666261", 10420151879100L, 10420151880571L, 215, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_215.123280.1157712863", 10420256971332L, 10420257988234L, 215, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_215.42051.1157712431", 10420256966540L, 10420256971235L, 215, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_216.88849.1157666257", 10420150761794L, 10420151878993L, 216, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_216.140582.1157666849", 10420151880571L, 10420153087066L, 216, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_216.79898.1157712865", 10420257988234L, 10420257995196L, 216, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_216.36921.1157712863", 10420256971235L, 10420257988192L, 216, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_217.16256.1157666257", 10420151878993L, 10420151879009L, 217, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_217.35111.1157667447", 10420153087066L, 10420154271099L, 217, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_217.108570.1157713453", 10420257995196L, 10420259792960L, 217, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_217.22239.1157712865", 10420257988192L, 10420257989612L, 217, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_218.121866.1157666259", 10420151879009L, 10420151879665L, 218, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_218.141145.1157667449", 10420154271099L, 10420154272053L, 218, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_218.39359.1157713535", 10420259792960L, 10420260099512L, 218, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_218.134408.1157713453", 10420257989612L, 10420259792890L, 218, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_219.28637.1157666261", 10420151879665L, 10420151881422L, 219, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_219.124965.1157668041", 10420154272053L, 10420155655746L, 219, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_219.108904.1157714127", 10420260099512L, 10420262475649L, 219, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_219.76129.1157713535", 10420259792890L, 10420260099424L, 219, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_220.36398.1157666847", 10420151881422L, 10420153086374L, 220, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_220.51009.1157668043", 10420155655746L, 10420155662264L, 220, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_220.85528.1157714129", 10420262475649L, 10420262478717L, 220, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_220.29523.1157714127", 10420260099424L, 10420262474512L, 220, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_221.43731.1157666847", 10420153086374L, 10420153086383L, 221, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_221.127691.1157668881", 10420155662264L, 10420157333868L, 221, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_221.51710.1157714485", 10420262478717L, 10420264074144L, 221, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_221.515.1157714129", 10420262474512L, 10420262478548L, 221, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_222.119893.1157666847", 10420153086383L, 10420153086864L, 222, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_222.122881.1157669221", 10420157333868L, 10420158072184L, 222, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_222.27747.1157714487", 10420264074144L, 10420264084157L, 222, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_222.111645.1157714483", 10420262478548L, 10420264073871L, 222, 2));

        archiveLogs.add(createArchiveLog("thread_3_seq_223.46673.1157666849", 10420153086864L, 10420153087371L, 223, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_223.140570.1157669223", 10420158072184L, 10420158090564L, 223, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_223.123626.1157714817", 10420264084157L, 10420265515021L, 223, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_223.130380.1157714487", 10420264073871L, 10420264083806L, 223, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_224.142420.1157667445", 10420153087371L, 10420154267875L, 224, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_224.134293.1157669571", 10420158090564L, 10420158700220L, 224, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_224.5437.1157714847", 10420265515021L, 10420265607642L, 224, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_224.118012.1157714817", 10420264083806L, 10420265514664L, 224, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_225.95799.1157667447", 10420154267875L, 10420154267971L, 225, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_225.81653.1157669575", 10420158700220L, 10420158701712L, 225, 4));
        archiveLogs.add(createArchiveLog("thread_1_seq_225.133834.1157715439", 10420265607642L, 10420268335370L, 225, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_225.130645.1157714847", 10420265514664L, 10420265607286L, 225, 2));
        archiveLogs.add(createArchiveLog("thread_3_seq_226.52885.1157667447", 10420154267971L, 10420154271841L, 226, 3));
        archiveLogs.add(createArchiveLog("thread_1_seq_226.126885.1157715441", 10420268335370L, 10420268357980L, 226, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_226.38133.1157715441", 10420265607286L, 10420268356523L, 226, 2));
        archiveLogs.add(createArchiveLog("thread_4_seq_226.139977.1157688339", 10420158701712L, 10420200646737L, 226, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_227.143067.1157667449", 10420154271841L, 10420154271972L, 227, 3));
        archiveLogs.add(createArchiveLog("thread_1_seq_227.78425.1157715835", 10420268357980L, 10420270076605L, 227, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_227.129775.1157715835", 10420268356523L, 10420270076322L, 227, 2));
        archiveLogs.add(createArchiveLog("thread_4_seq_227.41126.1157697633", 10420200646737L, 10420220313024L, 227, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_228.90356.1157668039", 10420154271972L, 10420155655406L, 228, 3));
        archiveLogs.add(createArchiveLog("thread_1_seq_228.115611.1157715839", 10420270076605L, 10420270135278L, 228, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_228.94201.1157715837", 10420270076322L, 10420270135055L, 228, 2));
        archiveLogs.add(createArchiveLog("thread_4_seq_228.100835.1157697753", 10420220313024L, 10420220568406L, 228, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_229.129626.1157668041", 10420155655406L, 10420155655460L, 229, 3));
        archiveLogs.add(createArchiveLog("thread_1_seq_229.73971.1157716203", 10420270135278L, 10420271953224L, 229, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_229.115089.1157716205", 10420270135055L, 10420271955771L, 229, 2));
        archiveLogs.add(createArchiveLog("thread_4_seq_229.129889.1157697753", 10420220568406L, 10420220568709L, 229, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_230.31278.1157668041", 10420155655460L, 10420155661531L, 230, 3));
        archiveLogs.add(createArchiveLog("thread_1_seq_230.122514.1157716207", 10420271953224L, 10420271957759L, 230, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_230.2285.1157716577", 10420271955771L, 10420273661199L, 230, 2));
        archiveLogs.add(createArchiveLog("thread_4_seq_230.140102.1157697753", 10420220568709L, 10420220573373L, 230, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_231.22494.1157668041", 10420155661531L, 10420155661688L, 231, 3));
        archiveLogs.add(createArchiveLog("thread_1_seq_231.140083.1157716575", 10420271957759L, 10420273660823L, 231, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_231.8006.1157716579", 10420273661199L, 10420273680011L, 231, 2));
        archiveLogs.add(createArchiveLog("thread_4_seq_231.140103.1157697753", 10420220573373L, 10420220574679L, 231, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_232.138095.1157668879", 10420155661688L, 10420157314029L, 232, 3));
        archiveLogs.add(createArchiveLog("thread_1_seq_232.141028.1157716579", 10420273660823L, 10420273675951L, 232, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_232.22797.1157716957", 10420273680011L, 10420275337363L, 232, 2));
        archiveLogs.add(createArchiveLog("thread_4_seq_232.116190.1157697893", 10420220574679L, 10420221155289L, 232, 4));

        archiveLogs.add(createArchiveLog("thread_3_seq_233.133244.1157668879", 10420157314029L, 10420157314054L, 233, 3));
        archiveLogs.add(createArchiveLog("thread_1_seq_233.117555.1157716957", 10420273675951L, 10420275336197L, 233, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_233.41799.1157717387", 10420275337363L, 10420277068392L, 233, 2));
        archiveLogs.add(createArchiveLog("thread_4_seq_233.24802.1157698025", 10420221155289L, 10420221429860L, 233, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_234.61216.1157668881", 10420157314054L, 10420157315330L, 234, 3));
        archiveLogs.add(createArchiveLog("thread_1_seq_234.28716.1157716959", 10420275336197L, 10420275343441L, 234, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_234.9580.1157717389", 10420277068392L, 10420277070896L, 234, 2));
        archiveLogs.add(createArchiveLog("thread_4_seq_234.137707.1157698027", 10420221429860L, 10420221430164L, 234, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_235.126103.1157668881", 10420157315330L, 10420157334043L, 235, 3));
        archiveLogs.add(createArchiveLog("thread_1_seq_235.7442.1157717389", 10420275343441L, 10420277070672L, 235, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_235.106788.1157717647", 10420277070896L, 10420278115206L, 235, 2));
        archiveLogs.add(createArchiveLog("thread_4_seq_235.71862.1157698029", 10420221430164L, 10420221432660L, 235, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_236.31086.1157669221", 10420157334043L, 10420158072159L, 236, 3));
        archiveLogs.add(createArchiveLog("thread_1_seq_236.63412.1157717647", 10420277070672L, 10420278114802L, 236, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_236.94082.1157717887", 10420278115206L, 10420278924412L, 236, 2));
        archiveLogs.add(createArchiveLog("thread_4_seq_236.57799.1157698031", 10420221432660L, 10420221434780L, 236, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_237.28692.1157669221", 10420158072159L, 10420158072225L, 237, 3));
        archiveLogs.add(createArchiveLog("thread_1_seq_237.86047.1157717887", 10420278114802L, 10420278923763L, 237, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_237.103667.1157717891", 10420278924412L, 10420278927968L, 237, 2));
        archiveLogs.add(createArchiveLog("thread_4_seq_237.65163.1157698181", 10420221434780L, 10420221735506L, 237, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_238.140587.1157669223", 10420158072225L, 10420158090013L, 238, 3));
        archiveLogs.add(createArchiveLog("thread_1_seq_238.26843.1157717891", 10420278923763L, 10420278926760L, 238, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_238.119911.1157718497", 10420278927968L, 10420280418929L, 238, 2));
        archiveLogs.add(createArchiveLog("thread_4_seq_238.64567.1157698183", 10420221735506L, 10420221735612L, 238, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_239.121914.1157669223", 10420158090013L, 10420158090828L, 239, 3));
        archiveLogs.add(createArchiveLog("thread_1_seq_239.65549.1157718497", 10420278926760L, 10420280418533L, 239, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_239.16787.1157718819", 10420280418929L, 10420281225938L, 239, 2));
        archiveLogs.add(createArchiveLog("thread_4_seq_239.132135.1157698183", 10420221735612L, 10420221736751L, 239, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_240.72632.1157669571", 10420158090828L, 10420158674383L, 240, 3));
        archiveLogs.add(createArchiveLog("thread_1_seq_240.138889.1157718499", 10420280418533L, 10420280420308L, 240, 1));
        archiveLogs.add(createArchiveLog("thread_2_seq_240.140351.1157718833", 10420281225938L, 10420281276306L, 240, 2));
        archiveLogs.add(createArchiveLog("thread_4_seq_240.125688.1157698187", 10420221736751L, 10420221755763L, 240, 4));

        archiveLogs.add(createArchiveLog("thread_3_seq_241.110247.1157669571", 10420158674383L, 10420158674416L, 241, 3));
        archiveLogs.add(createArchiveLog("thread_1_seq_241.121103.1157718819", 10420280420308L, 10420281225783L, 241, 1));
        archiveLogs.add(createArchiveLog("thread_4_seq_241.108136.1157698379", 10420221755763L, 10420222119037L, 241, 4));

        archiveLogs.add(createArchiveLog("thread_3_seq_242.140569.1157669573", 10420158674416L, 10420158700959L, 242, 3));
        archiveLogs.add(createArchiveLog("thread_1_seq_242.96103.1157718833", 10420281225783L, 10420281275996L, 242, 1));
        archiveLogs.add(createArchiveLog("thread_4_seq_242.60059.1157698379", 10420222119037L, 10420222119113L, 242, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_243.121391.1157669573", 10420158700959L, 10420158701224L, 243, 3));

        archiveLogs.add(createArchiveLog("thread_1_seq_243.77791.1157718837", 10420281275996L, 10420281277309L, 243, 1));
        archiveLogs.add(createArchiveLog("thread_4_seq_243.53591.1157698381", 10420222119113L, 10420222121166L, 243, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_244.135936.1157688327", 10420158701224L, 10420200638863L, 244, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_244.122384.1157698383", 10420222121166L, 10420222121906L, 244, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_245.138954.1157688335", 10420200638863L, 10420200645432L, 245, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_245.107785.1157698661", 10420222121906L, 10420222697948L, 245, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_246.82345.1157688335", 10420200645432L, 10420200645450L, 246, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_246.32581.1157699077", 10420222697948L, 10420223678037L, 246, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_247.28864.1157688337", 10420200645450L, 10420200646284L, 247, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_247.120723.1157699077", 10420223678037L, 10420223678052L, 247, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_248.105338.1157688337", 10420200646284L, 10420200646695L, 248, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_248.107852.1157699077", 10420223678052L, 10420223678960L, 248, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_249.35772.1157697601", 10420200646695L, 10420220232931L, 249, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_249.85712.1157699079", 10420223678960L, 10420223679491L, 249, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_250.42641.1157697629", 10420220232931L, 10420220308993L, 250, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_250.139178.1157699503", 10420223679491L, 10420224518687L, 250, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_251.123948.1157697629", 10420220308993L, 10420220309012L, 251, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_251.140035.1157699505", 10420224518687L, 10420224520516L, 251, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_252.141340.1157697631", 10420220309012L, 10420220312314L, 252, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_252.117865.1157699915", 10420224520516L, 10420225306286L, 252, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_253.141338.1157697631", 10420220312314L, 10420220313041L, 253, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_253.32488.1157699915", 10420225306286L, 10420225306373L, 253, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_254.101661.1157697759", 10420220313041L, 10420220594510L, 254, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_254.86660.1157699917", 10420225306373L, 10420225318270L, 254, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_255.17533.1157697889", 10420220594510L, 10420221121096L, 255, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_255.115981.1157699919", 10420225318270L, 10420225319402L, 255, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_256.42923.1157697891", 10420221121096L, 10420221121212L, 256, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_256.108334.1157700345", 10420225319402L, 10420226128782L, 256, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_257.18546.1157697891", 10420221121212L, 10420221154534L, 257, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_257.49683.1157700345", 10420226128782L, 10420226128863L, 257, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_258.105577.1157697893", 10420221154534L, 10420221155527L, 258, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_258.34087.1157700345", 10420226128863L, 10420226130101L, 258, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_259.7563.1157698031", 10420221155527L, 10420221434744L, 259, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_259.113934.1157700347", 10420226130101L, 10420226144908L, 259, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_260.16101.1157698185", 10420221434744L, 10420221738352L, 260, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_260.110413.1157700769", 10420226144908L, 10420227113897L, 260, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_261.46760.1157698379", 10420221738352L, 10420222119204L, 261, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_261.64154.1157700773", 10420227113897L, 10420227152689L, 261, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_262.65617.1157698383", 10420222119204L, 10420222121698L, 262, 3));

        archiveLogs.add(createArchiveLog("thread_4_seq_262.121431.1157701189", 10420227152689L, 10420227973804L, 262, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_263.120763.1157698659", 10420222121698L, 10420222696261L, 263, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_263.103485.1157701189", 10420227973804L, 10420227973831L, 263, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_264.83015.1157698659", 10420222696261L, 10420222696281L, 264, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_264.10330.1157701189", 10420227973831L, 10420227974574L, 264, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_265.110468.1157698661", 10420222696281L, 10420222697595L, 265, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_265.82981.1157701189", 10420227974574L, 10420227974730L, 265, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_266.110115.1157698663", 10420222697595L, 10420222698085L, 266, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_266.140877.1157701617", 10420227974730L, 10420228863280L, 266, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_267.134048.1157699079", 10420222698085L, 10420223679481L, 267, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_267.109040.1157702033", 10420228863280L, 10420229885544L, 267, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_268.85576.1157699501", 10420223679481L, 10420224517928L, 268, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_268.113631.1157702033", 10420229885544L, 10420229885574L, 268, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_269.137207.1157699501", 10420224517928L, 10420224518007L, 269, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_269.51037.1157702035", 10420229885574L, 10420229886419L, 269, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_270.139654.1157699503", 10420224518007L, 10420224519114L, 270, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_270.3791.1157702035", 10420229886419L, 10420229886619L, 270, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_271.122110.1157699505", 10420224519114L, 10420224520386L, 271, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_271.114023.1157702459", 10420229886619L, 10420230867807L, 271, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_272.112760.1157699917", 10420224520386L, 10420225317888L, 272, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_272.36577.1157702461", 10420230867807L, 10420230886318L, 272, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_273.14173.1157699919", 10420225317888L, 10420225319328L, 273, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_273.19013.1157702879", 10420230886318L, 10420232265822L, 273, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_274.70125.1157700345", 10420225319328L, 10420226144683L, 274, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_274.28174.1157703311", 10420232265822L, 10420233228183L, 274, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_275.44864.1157700769", 10420226144683L, 10420227113362L, 275, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_275.90724.1157703315", 10420233228183L, 10420233231803L, 275, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_276.4069.1157700769", 10420227113362L, 10420227113446L, 276, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_276.112466.1157703739", 10420233231803L, 10420234159905L, 276, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_277.11501.1157700771", 10420227113446L, 10420227114853L, 277, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_277.117913.1157703739", 10420234159905L, 10420234159943L, 277, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_278.37103.1157700771", 10420227114853L, 10420227152176L, 278, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_278.122229.1157703741", 10420234159943L, 10420234161268L, 278, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_279.55536.1157701189", 10420227152176L, 10420227974311L, 279, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_279.124227.1157703741", 10420234161268L, 10420234161360L, 279, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_280.105413.1157701193", 10420227974311L, 10420227975828L, 280, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_280.36193.1157703953", 10420234161360L, 10420234673116L, 280, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_281.78482.1157701613", 10420227975828L, 10420228862009L, 281, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_281.102020.1157704161", 10420234673116L, 10420235135401L, 281, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_282.17703.1157701613", 10420228862009L, 10420228862113L, 282, 3));

        archiveLogs.add(createArchiveLog("thread_4_seq_282.17180.1157704161", 10420235135401L, 10420235135411L, 282, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_283.126356.1157701615", 10420228862113L, 10420228863016L, 283, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_283.4693.1157704163", 10420235135411L, 10420235136135L, 283, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_284.45631.1157701617", 10420228863016L, 10420228897093L, 284, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_284.45619.1157704163", 10420235136135L, 10420235136503L, 284, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_285.119600.1157702035", 10420228897093L, 10420229886710L, 285, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_285.110192.1157704591", 10420235136503L, 10420236232947L, 285, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_286.7053.1157702457", 10420229886710L, 10420230862979L, 286, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_286.127711.1157704593", 10420236232947L, 10420236233023L, 286, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_287.90257.1157702459", 10420230862979L, 10420230863439L, 287, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_287.23126.1157704593", 10420236233023L, 10420236234152L, 287, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_288.124141.1157702459", 10420230863439L, 10420230871133L, 288, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_288.110944.1157704595", 10420236234152L, 10420236253038L, 288, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_289.126537.1157702461", 10420230871133L, 10420230876253L, 289, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_289.133482.1157705025", 10420236253038L, 10420237525350L, 289, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_290.59440.1157702877", 10420230876253L, 10420232264568L, 290, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_290.56162.1157705025", 10420237525350L, 10420237525447L, 290, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_291.42477.1157702877", 10420232264568L, 10420232264606L, 291, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_291.43066.1157705027", 10420237525447L, 10420237545771L, 291, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_292.12978.1157702879", 10420232264606L, 10420232265229L, 292, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_292.43373.1157705029", 10420237545771L, 10420237546550L, 292, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_293.124651.1157702879", 10420232265229L, 10420232265385L, 293, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_293.107822.1157705395", 10420237546550L, 10420238550239L, 293, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_294.130359.1157703311", 10420232265385L, 10420233227060L, 294, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_294.140844.1157705457", 10420238550239L, 10420238691005L, 294, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_295.16001.1157703311", 10420233227060L, 10420233227165L, 295, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_295.3191.1157705457", 10420238691005L, 10420238691082L, 295, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_296.35770.1157703313", 10420233227165L, 10420233229222L, 296, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_296.103635.1157705459", 10420238691082L, 10420238692020L, 296, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_297.135162.1157703313", 10420233229222L, 10420233230811L, 297, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_297.140843.1157705461", 10420238692020L, 10420238693436L, 297, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_298.29204.1157703739", 10420233230811L, 10420234160411L, 298, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_298.61101.1157705885", 10420238693436L, 10420239814708L, 298, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_299.31954.1157703743", 10420234160411L, 10420234162760L, 299, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_299.54155.1157705885", 10420239814708L, 10420239814882L, 299, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_300.132192.1157703953", 10420234162760L, 10420234672496L, 300, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_300.30671.1157705887", 10420239814882L, 10420239819004L, 300, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_301.132172.1157704163", 10420234672496L, 10420235136249L, 301, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_301.95608.1157705887", 10420239819004L, 10420239820100L, 301, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_302.17443.1157704595", 10420235136249L, 10420236235160L, 302, 3));

        archiveLogs.add(createArchiveLog("thread_4_seq_302.110667.1157706223", 10420239820100L, 10420240807663L, 302, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_303.13398.1157705027", 10420236235160L, 10420237546131L, 303, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_303.122308.1157706319", 10420240807663L, 10420241061430L, 303, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_304.5248.1157705393", 10420237546131L, 10420238549222L, 304, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_304.11533.1157706321", 10420241061430L, 10420241084267L, 304, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_305.106849.1157705459", 10420238549222L, 10420238692457L, 305, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_305.99982.1157706747", 10420241084267L, 10420242073489L, 305, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_306.111973.1157705885", 10420238692457L, 10420239815980L, 306, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_306.52600.1157707175", 10420242073489L, 10420243262288L, 306, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_307.38191.1157705889", 10420239815980L, 10420239824113L, 307, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_307.15501.1157707471", 10420243262288L, 10420244250203L, 307, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_308.131874.1157706221", 10420239824113L, 10420240807407L, 308, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_308.122867.1157707595", 10420244250203L, 10420244582426L, 308, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_309.107691.1157706317", 10420240807407L, 10420241060527L, 309, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_309.23981.1157707595", 10420244582426L, 10420244585755L, 309, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_310.50901.1157706317", 10420241060527L, 10420241060551L, 310, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_310.139717.1157707597", 10420244585755L, 10420244586879L, 310, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_311.20000.1157706319", 10420241060551L, 10420241062294L, 311, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_311.76318.1157707597", 10420244586879L, 10420244587040L, 311, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_312.127488.1157706321", 10420241062294L, 10420241063031L, 312, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_312.32491.1157708023", 10420244587040L, 10420245828695L, 312, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_313.47205.1157706745", 10420241063031L, 10420242069543L, 313, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_313.113711.1157708023", 10420245828695L, 10420245828752L, 313, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_314.30182.1157706745", 10420242069543L, 10420242069566L, 314, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_314.101643.1157708025", 10420245828752L, 10420245830139L, 314, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_315.36693.1157706747", 10420242069566L, 10420242073197L, 315, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_315.138351.1157708025", 10420245830139L, 10420245830266L, 315, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_316.132329.1157706749", 10420242073197L, 10420242073634L, 316, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_316.94316.1157708461", 10420245830266L, 10420246918266L, 316, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_317.60658.1157707171", 10420242073634L, 10420243260227L, 317, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_317.6348.1157708463", 10420246918266L, 10420246918349L, 317, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_318.47619.1157707171", 10420243260227L, 10420243260316L, 318, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_318.121116.1157708463", 10420246918349L, 10420246919827L, 318, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_319.123010.1157707173", 10420243260316L, 10420243261699L, 319, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_319.117912.1157708465", 10420246919827L, 10420246920380L, 319, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_320.93060.1157707173", 10420243261699L, 10420243261893L, 320, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_320.123201.1157708895", 10420246920380L, 10420248071407L, 320, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_321.138721.1157707471", 10420243261893L, 10420244250397L, 321, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_321.129901.1157708897", 10420248071407L, 10420248073130L, 321, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_322.139413.1157707597", 10420244250397L, 10420244586425L, 322, 3));

        archiveLogs.add(createArchiveLog("thread_4_seq_322.139219.1157709335", 10420248073130L, 10420249006575L, 322, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_323.130168.1157707599", 10420244586425L, 10420244588483L, 323, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_323.89173.1157709337", 10420249006575L, 10420249008424L, 323, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_324.91636.1157708025", 10420244588483L, 10420245830729L, 324, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_324.125662.1157709645", 10420249008424L, 10420249764210L, 324, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_325.124896.1157708465", 10420245830729L, 10420246920098L, 325, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_325.126755.1157709767", 10420249764210L, 10420250159235L, 325, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_326.42810.1157708893", 10420246920098L, 10420248071247L, 326, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_326.52586.1157709767", 10420250159235L, 10420250159272L, 326, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_327.111731.1157708895", 10420248071247L, 10420248071364L, 327, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_327.111998.1157709769", 10420250159272L, 10420250160381L, 327, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_328.130084.1157708897", 10420248071364L, 10420248072875L, 328, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_328.73660.1157709769", 10420250160381L, 10420250160655L, 328, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_329.121143.1157708897", 10420248072875L, 10420248073406L, 329, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_329.63361.1157710211", 10420250160655L, 10420251335465L, 329, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_330.112024.1157709335", 10420248073406L, 10420249005989L, 330, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_330.141000.1157710647", 10420251335465L, 10420252377617L, 330, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_331.74670.1157709335", 10420249005989L, 10420249006027L, 331, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_331.141032.1157710647", 10420252377617L, 10420252388750L, 331, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_332.10562.1157709337", 10420249006027L, 10420249007395L, 332, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_332.106586.1157710649", 10420252388750L, 10420252391882L, 332, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_333.23008.1157709337", 10420249007395L, 10420249007472L, 333, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_333.81306.1157710649", 10420252391882L, 10420252397416L, 333, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_334.47175.1157709647", 10420249007472L, 10420249768155L, 334, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_334.32367.1157711097", 10420252397416L, 10420253579174L, 334, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_335.92269.1157709769", 10420249768155L, 10420250160996L, 335, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_335.106250.1157711099", 10420253579174L, 10420253579227L, 335, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_336.76898.1157710209", 10420250160996L, 10420251314301L, 336, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_336.87651.1157711099", 10420253579227L, 10420253580491L, 336, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_337.88147.1157710209", 10420251314301L, 10420251314357L, 337, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_337.76298.1157711099", 10420253580491L, 10420253580978L, 337, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_338.32555.1157710211", 10420251314357L, 10420251335284L, 338, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_338.122775.1157711375", 10420253580978L, 10420254213442L, 338, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_339.64881.1157710213", 10420251335284L, 10420251335684L, 339, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_339.107157.1157711547", 10420254213442L, 10420254782939L, 339, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_340.46069.1157710649", 10420251335684L, 10420252377705L, 340, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_340.121157.1157711547", 10420254782939L, 10420254783031L, 340, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_341.31560.1157710649", 10420252377705L, 10420252392272L, 341, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_341.5568.1157711547", 10420254783031L, 10420254784192L, 341, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_342.22861.1157710653", 10420252392272L, 10420252398479L, 342, 3));

        archiveLogs.add(createArchiveLog("thread_4_seq_342.116097.1157711549", 10420254784192L, 10420254784559L, 342, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_343.134175.1157711099", 10420252398479L, 10420253580534L, 343, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_343.3442.1157711987", 10420254784559L, 10420255832931L, 343, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_344.96146.1157711103", 10420253580534L, 10420253587927L, 344, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_344.79054.1157711987", 10420255832931L, 10420255832993L, 344, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_345.67438.1157711375", 10420253587927L, 10420254213243L, 345, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_345.41232.1157711989", 10420255832993L, 10420255835588L, 345, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_346.2086.1157711547", 10420254213243L, 10420254782957L, 346, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_346.112816.1157711989", 10420255835588L, 10420255836480L, 346, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_347.53984.1157711549", 10420254782957L, 10420254785210L, 347, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_347.17406.1157712423", 10420255836480L, 10420256966166L, 347, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_348.66156.1157711989", 10420254785210L, 10420255834456L, 348, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_348.70660.1157712425", 10420256966166L, 10420256966381L, 348, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_349.109391.1157711991", 10420255834456L, 10420255859730L, 349, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_349.90762.1157712427", 10420256966381L, 10420256969622L, 349, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_350.74069.1157712427", 10420255859730L, 10420256968239L, 350, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_350.18331.1157712429", 10420256969622L, 10420256970441L, 350, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_351.42984.1157712431", 10420256968239L, 10420256970522L, 351, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_351.120464.1157712863", 10420256970441L, 10420257987944L, 351, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_352.118515.1157712865", 10420256970522L, 10420257989031L, 352, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_352.119729.1157712863", 10420257987944L, 10420257987975L, 352, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_353.108342.1157713453", 10420257989031L, 10420259792263L, 353, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_353.121321.1157712863", 10420257987975L, 10420257988785L, 353, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_354.93699.1157713533", 10420259792263L, 10420260098481L, 354, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_354.36746.1157712863", 10420257988785L, 10420257988879L, 354, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_355.91588.1157713537", 10420260098481L, 10420260154112L, 355, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_355.10111.1157713453", 10420257988879L, 10420259792349L, 355, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_356.120784.1157714123", 10420260154112L, 10420262464907L, 356, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_356.21574.1157713533", 10420259792349L, 10420260098142L, 356, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_357.27545.1157714123", 10420262464907L, 10420262464919L, 357, 3));

        archiveLogs.add(createArchiveLog("thread_4_seq_357.118724.1157713533", 10420260098142L, 10420260098201L, 357, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_358.22375.1157714127", 10420262464919L, 10420262477681L, 358, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_358.130345.1157713535", 10420260098201L, 10420260099046L, 358, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_359.128458.1157714127", 10420262477681L, 10420262477932L, 359, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_359.67512.1157713535", 10420260099046L, 10420260099248L, 359, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_360.141539.1157714483", 10420262477932L, 10420264073921L, 360, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_360.121577.1157714125", 10420260099248L, 10420262465571L, 360, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_361.59622.1157714485", 10420264073921L, 10420264073977L, 361, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_361.74242.1157714129", 10420262465571L, 10420262478037L, 361, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_362.138738.1157714485", 10420264073977L, 10420264074793L, 362, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_362.138171.1157714485", 10420262478037L, 10420264074692L, 362, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_363.6278.1157714485", 10420264074793L, 10420264083493L, 363, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_363.62675.1157714489", 10420264074692L, 10420264084453L, 363, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_364.2299.1157714819", 10420264083493L, 10420265515261L, 364, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_364.23445.1157714817", 10420264084453L, 10420265514534L, 364, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_365.17141.1157714845", 10420265515261L, 10420265598928L, 365, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_365.10516.1157714845", 10420265514534L, 10420265598543L, 365, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_366.17972.1157714849", 10420265598928L, 10420265607962L, 366, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_366.51294.1157714845", 10420265598543L, 10420265598577L, 366, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_367.37820.1157715441", 10420265607962L, 10420268335727L, 367, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_367.25425.1157714847", 10420265598577L, 10420265607183L, 367, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_368.93101.1157715443", 10420268335727L, 10420268358400L, 368, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_368.124441.1157714847", 10420265607183L, 10420265607337L, 368, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_369.11576.1157715835", 10420268358400L, 10420270076965L, 369, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_369.26899.1157715439", 10420265607337L, 10420268335349L, 369, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_370.45680.1157715839", 10420270076965L, 10420270135762L, 370, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_370.78491.1157715439", 10420268335349L, 10420268335437L, 370, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_371.77174.1157716203", 10420270135762L, 10420271953675L, 371, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_371.28493.1157715441", 10420268335437L, 10420268356276L, 371, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_372.119330.1157716207", 10420271953675L, 10420271958116L, 372, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_372.80360.1157715443", 10420268356276L, 10420268358246L, 372, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_373.104411.1157716575", 10420271958116L, 10420273660906L, 373, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_373.39368.1157715835", 10420268358246L, 10420270076282L, 373, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_374.100337.1157716579", 10420273660906L, 10420273675999L, 374, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_374.41154.1157715835", 10420270076282L, 10420270076312L, 374, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_375.79420.1157716957", 10420273675999L, 10420275336281L, 375, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_375.129661.1157715837", 10420270076312L, 10420270134670L, 375, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_376.63935.1157716959", 10420275336281L, 10420275343676L, 376, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_376.71504.1157715837", 10420270134670L, 10420270134866L, 376, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_377.106633.1157717387", 10420275343676L, 10420277068227L, 377, 3));

        archiveLogs.add(createArchiveLog("thread_4_seq_377.3664.1157716201", 10420270134866L, 10420271952887L, 377, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_378.76045.1157717387", 10420277068227L, 10420277068271L, 378, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_378.114279.1157716201", 10420271952887L, 10420271952992L, 378, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_379.65533.1157717389", 10420277068271L, 10420277070355L, 379, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_379.128744.1157716203", 10420271952992L, 10420271955102L, 379, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_380.79873.1157717389", 10420277070355L, 10420277070468L, 380, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_380.50874.1157716207", 10420271955102L, 10420271957849L, 380, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_381.108053.1157717647", 10420277070468L, 10420278114784L, 381, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_381.141010.1157716577", 10420271957849L, 10420273660478L, 381, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_382.52375.1157717887", 10420278114784L, 10420278923648L, 382, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_382.26084.1157716575", 10420273660478L, 10420273660509L, 382, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_383.140085.1157717891", 10420278923648L, 10420278926713L, 383, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_383.51626.1157716577", 10420273660509L, 10420273674968L, 383, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_384.56048.1157718497", 10420278926713L, 10420280418163L, 384, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_384.141023.1157716577", 10420273674968L, 10420273675540L, 384, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_385.86246.1157718499", 10420280418163L, 10420280420112L, 385, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_385.30808.1157716955", 10420273675540L, 10420275313944L, 385, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_386.72104.1157718817", 10420280420112L, 10420281225515L, 386, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_386.17758.1157716955", 10420275313944L, 10420275314033L, 386, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_387.72320.1157718833", 10420281225515L, 10420281275692L, 387, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_387.135053.1157716957", 10420275314033L, 10420275335801L, 387, 4));
        archiveLogs.add(createArchiveLog("thread_3_seq_388.139428.1157718835", 10420281275692L, 10420281277224L, 388, 3));
        archiveLogs.add(createArchiveLog("thread_4_seq_388.116945.1157716959", 10420275335801L, 10420275342355L, 388, 4));

        archiveLogs.add(createArchiveLog("thread_4_seq_389.19147.1157717389", 10420275342355L, 10420277070498L, 389, 4));
        archiveLogs.add(createArchiveLog("thread_4_seq_390.68751.1157717647", 10420277070498L, 10420278114635L, 390, 4));
        archiveLogs.add(createArchiveLog("thread_4_seq_391.80366.1157717885", 10420278114635L, 10420278922540L, 391, 4));
        archiveLogs.add(createArchiveLog("thread_4_seq_392.66620.1157717887", 10420278922540L, 10420278922617L, 392, 4));
        archiveLogs.add(createArchiveLog("thread_4_seq_393.141949.1157717889", 10420278922617L, 10420278924737L, 393, 4));
        archiveLogs.add(createArchiveLog("thread_4_seq_394.120822.1157717889", 10420278924737L, 10420278925219L, 394, 4));
        archiveLogs.add(createArchiveLog("thread_4_seq_395.143822.1157718495", 10420278925219L, 10420280403567L, 395, 4));
        archiveLogs.add(createArchiveLog("thread_4_seq_396.83607.1157718495", 10420280403567L, 10420280403610L, 396, 4));
        archiveLogs.add(createArchiveLog("thread_4_seq_397.129755.1157718497", 10420280403610L, 10420280418794L, 397, 4));
        archiveLogs.add(createArchiveLog("thread_4_seq_398.119428.1157718497", 10420280418794L, 10420280419179L, 398, 4));
        archiveLogs.add(createArchiveLog("thread_4_seq_399.71016.1157718817", 10420280419179L, 10420281225033L, 399, 4));
        archiveLogs.add(createArchiveLog("thread_4_seq_400.129027.1157718831", 10420281225033L, 10420281275095L, 400, 4));
        archiveLogs.add(createArchiveLog("thread_4_seq_401.58706.1157718833", 10420281275095L, 10420281275115L, 401, 4));
        archiveLogs.add(createArchiveLog("thread_4_seq_402.100157.1157718833", 10420281275115L, 10420281276189L, 402, 4));
        archiveLogs.add(createArchiveLog("thread_4_seq_403.138149.1157718833", 10420281276189L, 10420281276471L, 403, 4));

        final List<LogFile> redoLogs = new ArrayList<>();
        redoLogs.add(createRedoLog("group_200.5681.1128514009", 10420281276306L, 241, 2));
        redoLogs.add(createRedoLog("group_304.5698.1128514231", 10420281277224L, 389, 3));
        redoLogs.add(createRedoLog("group_103.5671.1128513879", 10420281277309L, 244, 1));
        redoLogs.add(createRedoLog("group_401.5708.1128514359", 10420281276471L, 404, 4));

        final RedoThreadState state = getFourThreadOpenState(Scn.valueOf(10420281277314L), Scn.valueOf(10420281277314L));

        final LogFileCollector logFileCollector = getLogFileCollector(state);
        final List<LogFile> files = logFileCollector.deduplicateLogFiles(archiveLogs, redoLogs);
        assertThat(logFileCollector.isLogFileListConsistent(Scn.valueOf(10420281277315L), files, state)).isTrue();
    }

    @Test
    @FixFor("DBZ-2855")
    public void testLogsWithRegularScns() throws Exception {
        final List<LogFile> files = new ArrayList<>();
        files.add(createArchiveLog("logfile1", 103400, 103700, 1, 1));
        files.add(createRedoLog("logfile2", 103700, 104000, 2, 1));

        final RedoThreadState redoThreadState = getSingleThreadOpenState(Scn.valueOf(1L), Scn.valueOf(104000));

        final Configuration config = getDefaultConfig().build();
        final OracleConnection connection = getOracleConnectionMock(redoThreadState, files);
        final LogFileCollector collector = getLogFileCollector(config, connection);

        final List<LogFile> result = collector.getLogs(Scn.valueOf(10));
        assertThat(result).hasSize(2);
        assertThat(getLogFileWithName(result, "logfile1").getNextScn()).isEqualTo(Scn.valueOf(103700));
        assertThat(getLogFileWithName(result, "logfile2").getNextScn()).isEqualTo(Scn.MAX);
    }

    @Test
    @FixFor("DBZ-2855")
    public void testExcludeLogsBeforeOffsetScn() throws Exception {
        final List<LogFile> files = new ArrayList<>();
        files.add(createArchiveLog("logfile1", 103300, 103400, 1, 1));
        files.add(createArchiveLog("logfile3", 103400, 103700, 2, 1));
        files.add(createRedoLog("logfile2", 103700, 104000, 3, 1));

        final RedoThreadState redoThreadState = getSingleThreadOpenState(Scn.valueOf(1L), Scn.valueOf(104000));

        final Configuration config = getDefaultConfig().build();
        final OracleConnection connection = getOracleConnectionMock(redoThreadState, files);
        final LogFileCollector collector = getLogFileCollector(config, connection);

        final List<LogFile> result = collector.getLogs(Scn.valueOf(103401));
        assertThat(result).hasSize(2);
        assertThat(getLogFileWithName(result, "logfile1")).isNull();
    }

    @Test
    @FixFor("DBZ-2855")
    public void testNullsHandledAsMaxScn() throws Exception {
        final List<LogFile> files = new ArrayList<>();
        files.add(createArchiveLog("logfile1", 103300, 103400, 1, 1));
        files.add(createArchiveLog("logfile3", 103400, 103700, 2, 1));
        files.add(createRedoLogWithNullEndScn("logfile2", 103700, 3, 1));

        final RedoThreadState redoThreadState = getSingleThreadOpenState(Scn.valueOf(1L), Scn.valueOf(104000));

        final Configuration config = getDefaultConfig().build();
        final OracleConnection connection = getOracleConnectionMock(redoThreadState, files);
        final LogFileCollector collector = getLogFileCollector(config, connection);

        final List<LogFile> result = collector.getLogs(Scn.valueOf(600));
        assertThat(result).hasSize(3);
        assertThat(getLogFileWithName(result, "logfile2").getNextScn()).isEqualTo(Scn.MAX);
    }

    @Test
    @FixFor("DBZ-2855")
    public void testCanHandleMaxScn() throws Exception {
        final List<LogFile> files = new ArrayList<>();
        files.add(createArchiveLog("logfile1", 103300, 103400, 1, 1));
        files.add(createArchiveLog("logfile3", 103400, 103700, 2, 1));
        files.add(createRedoLog("logfile2", "103700", "18446744073709551615", 3, 1));

        final RedoThreadState redoThreadState = getSingleThreadOpenState(Scn.valueOf(1L), Scn.valueOf(104000));

        final Configuration config = getDefaultConfig().build();
        final OracleConnection connection = getOracleConnectionMock(redoThreadState, files);
        final LogFileCollector collector = getLogFileCollector(config, connection);

        final List<LogFile> result = collector.getLogs(Scn.valueOf(600));
        assertThat(result).hasSize(3);
        assertThat(getLogFileWithName(result, "logfile2").getNextScn()).isEqualTo(Scn.MAX);
    }

    @Test
    @FixFor("DBZ-2855")
    public void testCanHandleVeryLargeScnValuesInNonCurrentRedoLog() throws Exception {
        final String largeScnValue = "18446744073709551615";

        final List<LogFile> files = new ArrayList<>();
        files.add(createArchiveLog("logfile1", 103300, 103400, 1, 1));
        files.add(createArchiveLog("logfile3", 103400, 103700, 2, 1));
        files.add(createRedoLog("logfile2", "103700", largeScnValue, 3, false, 1));

        final RedoThreadState redoThreadState = getSingleThreadOpenState(Scn.valueOf(1L), Scn.valueOf(104000));

        final Configuration config = getDefaultConfig().build();
        final OracleConnection connection = getOracleConnectionMock(redoThreadState, files);
        final LogFileCollector collector = getLogFileCollector(config, connection);

        final List<LogFile> result = collector.getLogs(Scn.valueOf(600));
        assertThat(result).hasSize(3);
        assertThat(getLogFileWithName(result, "logfile2").getNextScn()).isEqualTo(Scn.valueOf(largeScnValue));
    }

    @Test
    @FixFor("DBZ-7389")
    public void testRacMultipleNodesNoThreadStateChanges() throws Exception {
        // Tests mining with 2 nodes, status/enabled state remains unchanged
        RedoThreadState redoThreadState = initialMultiNodeRedoThreadState();

        final List<LogFile> files = new ArrayList<>();
        files.add(createArchiveLog("archive.log", 1, 999, 1, 1));
        files.add(createRedoLog("redo01.log", 999, 2, 1));
        files.add(createRedoLog("redo02.log", 50, 2, 2));

        final Configuration config = getDefaultConfig().build();
        final OracleConnection connection = getOracleConnectionMock(redoThreadState);

        LogFileCollector collector = setCollectorLogFiles(getLogFileCollector(config, connection), files);

        // Since no thread state changes and mining from SCN 1, we should get the same log files
        assertThat(collector.getLogs(Scn.ONE)).isEqualTo(files);

        // Bump redo thread state
        // No redo thread state changes, no checkpoints or log switches
        redoThreadState = advanceRedoThreadState(redoThreadState, 1, 10, false);
        redoThreadState = advanceRedoThreadState(redoThreadState, 2, 10, false);
        setConnectionRedoThreadState(connection, redoThreadState);

        // Should get the same logs even after state advancement
        assertThat(collector.getLogs(Scn.ONE)).isEqualTo(files);
    }

    @Test
    @FixFor("DBZ-7389")
    public void testRacMultipleNodesOneThreadChangesToClosed() throws Exception {
        // Tests mining with 2 nodes, status of one node changes to closed
        RedoThreadState redoThreadState = initialMultiNodeRedoThreadState();

        // One archive for node 1, node 2 has no archive logs, only redo
        final List<LogFile> files = new ArrayList<>();
        files.add(createArchiveLog("archive.log", 1, 999, 1, 1));
        files.add(createRedoLog("redo01.log", 999, 2, 1));
        files.add(createRedoLog("redo02.log", 50, 2, 2));

        final Configuration config = getDefaultConfig().build();
        final OracleConnection connection = getOracleConnectionMock(redoThreadState);

        LogFileCollector collector = setCollectorLogFiles(getLogFileCollector(config, connection), files);

        // Since no thread state changes yet, we should get the same logs
        assertThat(collector.getLogs(Scn.ONE)).isEqualTo(files);

        // Transition redo thread 1 to CLOSED (offline)
        redoThreadState = transitionRedoThreadToOffline(redoThreadState, 1, 1);
        setConnectionRedoThreadState(connection, redoThreadState);

        // Since redo thread 1 transitioned from OPEN to CLOSED; this causes a checkpoint.
        // A checkpoint causes a log switch, so logs should reflect this change.
        files.clear();
        files.add(createArchiveLog("archive1.log", 1, 999, 1, 1));
        files.add(createArchiveLog("archive2.log", 999, 1102, 2, 1));
        files.add(createRedoLog("redo01.log", 1102, 3, 1));
        files.add(createRedoLog("redo02.log", 50, 2, 2));
        collector = setCollectorLogFiles(collector, files);

        assertThat(collector.getLogs(Scn.ONE)).isEqualTo(files);
    }

    @Test
    @FixFor("DBZ-7389")
    public void testRacMultipleNodesOneThreadChangesToOpen() throws Exception {
        // Tests mining with 2 nodes, status of one node changes from closed to open
        RedoThreadState redoThreadState = initialMultiNodeRedoThreadState();
        redoThreadState = transitionRedoThreadToOffline(redoThreadState, 1, 0);

        // One archive for node 1, node 2 has no archive logs, only redo
        final List<LogFile> files = new ArrayList<>();
        files.add(createArchiveLog("archive.log", 1, 999, 1, 1));
        files.add(createRedoLog("redo01.log", 999, 2, 1));
        files.add(createRedoLog("redo02.log", 50, 2, 2));

        final Configuration config = getDefaultConfig().build();
        final OracleConnection connection = getOracleConnectionMock(redoThreadState);

        LogFileCollector collector = setCollectorLogFiles(getLogFileCollector(config, connection), files);

        // Since no thread state changes yet, we should get the same logs.
        assertThat(collector.getLogs(Scn.ONE)).isEqualTo(files);

        // Transition node 1 to OPEN (online)
        redoThreadState = transitionRedoThreadToOnline(redoThreadState, 1);
        setConnectionRedoThreadState(connection, redoThreadState);

        // Since redo thread 1 transitioned from CLOSED to OPEN; this causes a checkpoint.
        // A checkpoint causes a log switch, so logs should reflect this change.
        files.clear();
        files.add(createArchiveLog("archive1.log", 1, 999, 1, 1));
        files.add(createArchiveLog("archive2.log", 999, 1102, 2, 1));
        files.add(createRedoLog("redo01.log", 1102, 3, 1));
        files.add(createRedoLog("redo02.log", 50, 2, 2));
        collector = setCollectorLogFiles(collector, files);

        assertThat(collector.getLogs(Scn.ONE)).isEqualTo(files);
    }

    @Test
    @FixFor("DBZ-7389")
    public void testRacMultipleNodesOpenedAndAddNewNodeInClosedStateEnabled() throws Exception {
        // Tests mining with 2 nodes, add a new node that is in closed state.
        RedoThreadState redoThreadState = initialMultiNodeRedoThreadState();

        // One archive for node 1, node 2 has no archive logs, only redo
        final List<LogFile> files = new ArrayList<>();
        files.add(createArchiveLog("archive.log", 1, 999, 1, 1));
        files.add(createRedoLog("redo01.log", 999, 2, 1));
        files.add(createRedoLog("redo02.log", 50, 2, 2));

        final Configuration config = getDefaultConfig().build();
        final OracleConnection connection = getOracleConnectionMock(redoThreadState);

        LogFileCollector collector = setCollectorLogFiles(getLogFileCollector(config, connection), files);

        // Since no thread state changes yet, we should get the same logs.
        assertThat(collector.getLogs(Scn.ONE)).isEqualTo(files);

        // Add new redo thread 3 in CLOSED state
        RedoThreadState.Builder builder = duplicateState(redoThreadState);
        redoThreadState = builder.thread()
                // Node 3
                .threadId(3)
                .status("CLOSED")
                .enabled("PUBLIC")
                .logGroups(2L)
                .instanceName("ORCLCDB")
                .openTime(Instant.now().minus(1, ChronoUnit.DAYS))
                .currentGroupNumber(1L)
                .currentSequenceNumber(1L)
                .checkpointScn(Scn.valueOf(1000))
                .checkpointTime(Instant.now().minus(5, ChronoUnit.MINUTES))
                .enabledScn(Scn.valueOf(1000))
                .enabledTime(Instant.now().minus(1, ChronoUnit.DAYS).plus(1, ChronoUnit.HOURS))
                .disabledScn(Scn.valueOf(0))
                .disabledTime(Instant.now())
                .lastRedoSequenceNumber(1L)
                .lastRedoBlock(1024L)
                .lastRedoScn(Scn.valueOf(1100))
                .lastRedoTime(Instant.now().minus(1, ChronoUnit.MINUTES))
                .conId(0L)
                .build()
                .build();
        setConnectionRedoThreadState(connection, redoThreadState);

        // With the addition of redo thread 3, a checkpoint will happen such that there is some logs
        // that get created for the redo thread upon start.
        files.clear();
        files.add(createArchiveLog("archive1.log", 1, 999, 1, 1));
        files.add(createArchiveLog("archive2.log", 999, 1102, 2, 1));
        files.add(createRedoLog("redo01.log", 1102, 3, 1));
        files.add(createRedoLog("redo02.log", 50, 2, 2));
        files.add(createRedoLog("redo03.log", 1000, 4, 3));
        collector = setCollectorLogFiles(collector, files);

        assertThat(collector.getLogs(Scn.ONE)).isEqualTo(files);
    }

    @Test
    @FixFor("DBZ-7389")
    public void testRacMultipleNodesOpenedAndAddNewNodeInOpened() throws Exception {
        // Tests mining with 2 nodes, add a new node that is in closed state.
        RedoThreadState redoThreadState = initialMultiNodeRedoThreadState();

        // One archive for node 1, node 2 has no archive logs, only redo
        final List<LogFile> files = new ArrayList<>();
        files.add(createArchiveLog("archive.log", 1, 999, 1, 1));
        files.add(createRedoLog("redo01.log", 999, 2, 1));
        files.add(createRedoLog("redo02.log", 50, 2, 2));

        final Configuration config = getDefaultConfig().build();
        final OracleConnection connection = getOracleConnectionMock(redoThreadState);

        LogFileCollector collector = setCollectorLogFiles(getLogFileCollector(config, connection), files);

        // Since no thread state changes yet, we should get the same logs.
        assertThat(collector.getLogs(Scn.ONE)).isEqualTo(files);

        // Add new redo thread 3 in CLOSED state
        RedoThreadState.Builder builder = duplicateState(redoThreadState);
        redoThreadState = builder.thread()
                // Node 3
                .threadId(3)
                .status("OPEN")
                .enabled("PUBLIC")
                .logGroups(2L)
                .instanceName("ORCLCDB")
                .openTime(Instant.now().minus(1, ChronoUnit.DAYS))
                .currentGroupNumber(1L)
                .currentSequenceNumber(1L)
                .checkpointScn(Scn.valueOf(1000))
                .checkpointTime(Instant.now().minus(5, ChronoUnit.MINUTES))
                .enabledScn(Scn.valueOf(500))
                .enabledTime(Instant.now().minus(1, ChronoUnit.DAYS).plus(1, ChronoUnit.HOURS))
                .disabledScn(Scn.valueOf(0))
                .disabledTime(null)
                .lastRedoSequenceNumber(1L)
                .lastRedoBlock(1024L)
                .lastRedoScn(Scn.valueOf(1100))
                .lastRedoTime(Instant.now().minus(1, ChronoUnit.MINUTES))
                .conId(0L)
                .build()
                .build();
        setConnectionRedoThreadState(connection, redoThreadState);

        // With the addition of redo thread 3, a checkpoint will happen such that there is some logs
        // that get created for the redo thread upon start.
        files.clear();
        files.add(createArchiveLog("archive1.log", 1, 999, 1, 1));
        files.add(createArchiveLog("archive2.log", 999, 1102, 2, 1));
        files.add(createRedoLog("redo01.log", 1102, 3, 1));
        files.add(createRedoLog("redo02.log", 50, 2, 2));
        files.add(createRedoLog("redo03.log", 1100, 4, 3));
        collector = setCollectorLogFiles(collector, files);

        assertThat(collector.getLogs(Scn.ONE)).isEqualTo(files);
    }

    private static LogFile createRedoLog(String name, long startScn, int sequence, int threadId) {
        return createRedoLog(name, startScn, Long.MAX_VALUE, sequence, threadId);
    }

    private static LogFile createRedoLog(String name, long startScn, long endScn, int sequence, int threadId) {
        return createRedoLog(name, startScn, endScn, sequence, threadId, true);
    }

    private static LogFile createRedoLog(String name, String startScn, String endScn, int sequence, int threadId) {
        return createRedoLog(name, startScn, endScn, sequence, true, threadId);
    }

    private static LogFile createRedoLog(String name, String startScn, String endScn, int sequence, boolean current, int threadId) {
        return new LogFile(name, Scn.valueOf(startScn), Scn.valueOf(endScn), BigInteger.valueOf(sequence), LogFile.Type.REDO, current, threadId);
    }

    private static LogFile createRedoLog(String name, long startScn, long endScn, int sequence, int threadId, boolean current) {
        return new LogFile(name, Scn.valueOf(startScn), Scn.valueOf(endScn),
                BigInteger.valueOf(sequence), LogFile.Type.REDO, current, threadId);
    }

    private static LogFile createRedoLogWithNullEndScn(String name, long startScn, int sequence, int threadId) {
        return new LogFile(name, Scn.valueOf(startScn), null, BigInteger.valueOf(sequence), LogFile.Type.REDO, true, threadId);
    }

    private static LogFile createArchiveLog(String name, long startScn, long endScn, int sequence, int threadId) {
        return new LogFile(name, Scn.valueOf(startScn), Scn.valueOf(endScn),
                BigInteger.valueOf(sequence), LogFile.Type.ARCHIVE, false, threadId);
    }

    private LogFile getLogFileWithName(List<LogFile> logs, String fileName) {
        return logs.stream().filter(log -> log.getFileName().equals(fileName)).findFirst().orElse(null);
    }

    private static Configuration.Builder getDefaultConfig() {
        return TestHelper.defaultConfig().with(OracleConnectorConfig.LOG_MINING_LOG_QUERY_MAX_RETRIES, 0);
    }

    private OracleConnection getOracleConnectionMock(RedoThreadState state) throws SQLException {
        final OracleConnection connection = Mockito.mock(OracleConnection.class);
        Mockito.when(connection.getRedoThreadState()).thenReturn(state);
        return connection;
    }

    private OracleConnection getOracleConnectionMock(RedoThreadState state, List<LogFile> logFileQueryResult) throws SQLException {
        final OracleConnection connection = getOracleConnectionMock(state);

        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Connection jdbcConnection = Mockito.mock(Connection.class);
        Mockito.when(connection.connection()).thenReturn(jdbcConnection);
        Mockito.when(connection.connection(false)).thenReturn(jdbcConnection);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        JdbcConnection.StatementFactory factory = Mockito.mock(JdbcConnection.StatementFactory.class);
        Mockito.when(factory.createStatement(jdbcConnection)).thenReturn(preparedStatement);

        Mockito.when(jdbcConnection.prepareStatement(anyString())).thenReturn(preparedStatement);

        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenAnswer(it -> ++currentQueryRow <= logFileQueryResult.size());
        Mockito.when(resultSet.getString(1)).thenAnswer(it -> logFileQueryResult.get(currentQueryRow - 1).getFileName());
        Mockito.when(resultSet.getString(2)).thenAnswer(it -> logFileQueryResult.get(currentQueryRow - 1).getFirstScn().toString());
        Mockito.when(resultSet.getString(3)).thenAnswer(it -> {
            final LogFile file = logFileQueryResult.get(currentQueryRow - 1);
            return file.getNextScn() == null ? null : file.getNextScn().toString();
        });
        Mockito.when(resultSet.getString(5)).thenAnswer(it -> {
            final LogFile file = logFileQueryResult.get(currentQueryRow - 1);
            if (LogFile.Type.ARCHIVE.equals(file.getType())) {
                return null;
            }
            return file.isCurrent() ? "CURRENT" : "ACTIVE";
        });
        Mockito.when(resultSet.getString(6)).thenAnswer(it -> {
            final LogFile file = logFileQueryResult.get(currentQueryRow - 1);
            return LogFile.Type.ARCHIVE.equals(file.getType()) ? "ARCHIVED" : "ONLINE";
        });
        Mockito.when(resultSet.getString(7)).thenAnswer(it -> logFileQueryResult.get(currentQueryRow - 1).getSequence().toString());
        Mockito.when(resultSet.getInt(10)).thenAnswer(it -> logFileQueryResult.get(currentQueryRow - 1).getThread());

        Mockito.doAnswer(a -> {
            JdbcConnection.ResultSetConsumer consumer = a.getArgument(1);
            consumer.accept(resultSet);
            return null;
        }).when(connection).query(anyString(), Mockito.any(JdbcConnection.ResultSetConsumer.class));

        return connection;
    }

    private LogFileCollector getLogFileCollector(RedoThreadState state) throws SQLException {
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(getDefaultConfig().build());
        return new LogFileCollector(connectorConfig, getOracleConnectionMock(state));
    }

    private LogFileCollector getLogFileCollector(Configuration configuration, OracleConnection connection) {
        return new LogFileCollector(new OracleConnectorConfig(configuration), connection);
    }

    private LogFileCollector setCollectorLogFiles(LogFileCollector collector, List<LogFile> logFiles) throws SQLException {
        final LogFileCollector mock = Mockito.mockingDetails(collector).isMock() ? collector : Mockito.spy(collector);
        Mockito.doReturn(logFiles).when(mock).getLogsForOffsetScn(Mockito.any(Scn.class));
        return mock;
    }

    private void setConnectionRedoThreadState(OracleConnection connection, RedoThreadState redoThreadState) throws SQLException {
        Mockito.when(connection.getRedoThreadState()).thenReturn(redoThreadState);
    }

    private RedoThreadState getSingleThreadOpenState(Scn enabledScn, Scn lastFlushedScn) {
        RedoThreadState.Builder builder = RedoThreadState.builder();
        builder = makeOpenRedoThreadState(builder.thread(), 1, enabledScn, lastFlushedScn).build();
        return builder.build();
    }

    private RedoThreadState getTwoThreadOpenState(Scn enabledScnOne, Scn lastFlushedScnOne, Scn enabledScnTwo, Scn lastFlushedScnTwo) {
        RedoThreadState.Builder builder = RedoThreadState.builder();
        builder = makeOpenRedoThreadState(builder.thread(), 1, enabledScnOne, lastFlushedScnOne).build();
        builder = makeOpenRedoThreadState(builder.thread(), 2, enabledScnTwo, lastFlushedScnTwo).build();
        return builder.build();
    }

    private RedoThreadState getFourThreadOpenState(Scn enabledScn, Scn lastFlushScn) {
        RedoThreadState.Builder builder = RedoThreadState.builder();
        builder = makeOpenRedoThreadState(builder.thread(), 1, enabledScn, lastFlushScn).build();
        builder = makeOpenRedoThreadState(builder.thread(), 2, enabledScn, lastFlushScn).build();
        builder = makeOpenRedoThreadState(builder.thread(), 3, enabledScn, lastFlushScn).build();
        builder = makeOpenRedoThreadState(builder.thread(), 4, enabledScn, lastFlushScn).build();
        return builder.build();
    }

    private RedoThread.Builder makeOpenRedoThreadState(RedoThread.Builder builder,
                                                       int threadId, Scn enabledScn, Scn lastFlushedScn) {
        return builder.threadId(threadId)
                .status("OPEN")
                .enabled("PUBLIC")
                .instanceName("ORCLCDB")
                .logGroups(2L)
                .openTime(Instant.now().minus(10, ChronoUnit.MINUTES))
                .checkpointTime(Instant.now().minus(5, ChronoUnit.MINUTES))
                .checkpointScn(enabledScn.add(Scn.ONE))
                .currentGroupNumber(1L)
                .currentSequenceNumber(1L)
                .enabledScn(enabledScn)
                .enabledTime(Instant.now().minus(10, ChronoUnit.MINUTES))
                .disabledScn(Scn.valueOf(0))
                .disabledTime(null)
                .lastRedoScn(lastFlushedScn)
                .lastRedoBlock(1234L)
                .lastRedoSequenceNumber(2L)
                .lastRedoTime(Instant.now().minus(3, ChronoUnit.SECONDS))
                .conId(0L);
    }

    // Node 1 - OPEN Sequence 1, started at SCN 500, last SCN 1100, recent checkpoint SCN 1000
    // Node 2 - OPEN Sequence 1, started at SCN 500, last SCN 1100, recent checkpoint SCN 1000
    private RedoThreadState initialMultiNodeRedoThreadState() {
        return RedoThreadState.builder()
                .thread()
                // Node 1
                .threadId(1)
                .status("OPEN")
                .enabled("PUBLIC")
                .logGroups(2L)
                .instanceName("ORCLCDB")
                .openTime(Instant.now().minus(1, ChronoUnit.DAYS))
                .currentGroupNumber(1L)
                .currentSequenceNumber(1L)
                .checkpointScn(Scn.valueOf(1000))
                .checkpointTime(Instant.now().minus(5, ChronoUnit.MINUTES))
                .enabledScn(Scn.valueOf(500))
                .enabledTime(Instant.now().minus(1, ChronoUnit.DAYS).plus(1, ChronoUnit.HOURS))
                .disabledScn(Scn.valueOf(0))
                .disabledTime(null)
                .lastRedoSequenceNumber(1L)
                .lastRedoBlock(1024L)
                .lastRedoScn(Scn.valueOf(1100))
                .lastRedoTime(Instant.now().minus(1, ChronoUnit.MINUTES))
                .conId(0L)
                .build()
                .thread()
                // Node 2
                .threadId(2)
                .status("OPEN")
                .enabled("PUBLIC")
                .logGroups(2L)
                .instanceName("ORCLCDB")
                .openTime(Instant.now().minus(1, ChronoUnit.DAYS))
                .currentGroupNumber(1L)
                .currentSequenceNumber(1L)
                .checkpointScn(Scn.valueOf(1000))
                .checkpointTime(Instant.now().minus(5, ChronoUnit.MINUTES))
                .enabledScn(Scn.valueOf(500))
                .enabledTime(Instant.now().minus(1, ChronoUnit.DAYS).plus(1, ChronoUnit.HOURS))
                .disabledScn(Scn.valueOf(0))
                .disabledTime(null)
                .lastRedoSequenceNumber(1L)
                .lastRedoBlock(1024L)
                .lastRedoScn(Scn.valueOf(1100))
                .lastRedoTime(Instant.now().minus(1, ChronoUnit.MINUTES))
                .conId(0L)
                .build()
                .build();
    }

    /**
     * Performs a checkpoint operation on a redo thread, this is when a redo log rolls to an archive.
     *
     * @param currentState the current state to be mutated, should not be {@code null}
     * @param threadId the thread id
     * @param checkpointScn the scn to store in the checkpoint
     * @return a new redo thread state with the thread modifications
     */
    private static RedoThreadState checkpointRedoThreadState(RedoThreadState currentState, int threadId, int checkpointScn) {
        RedoThreadState.Builder builder = RedoThreadState.builder();
        for (RedoThread currentThread : currentState.getThreads()) {
            final RedoThread.Builder threadBuilder = duplicateThreadState(builder.thread(), currentThread);
            if (currentThread.getThreadId() == threadId) {
                threadBuilder.checkpointTime(Instant.now())
                        .checkpointScn(Scn.valueOf(checkpointScn));
            }
            builder = threadBuilder.build();
        }
        return builder.build();
    }

    /**
     * Transitions a redo thread to online mode.
     *
     * @param currentState the current state to be mutated, should not be {@code null}
     * @param threadId the thread id
     * @return a new redo thread state with the thread modifications
     */
    private static RedoThreadState transitionRedoThreadToOnline(RedoThreadState currentState, int threadId) {
        RedoThreadState.Builder builder = RedoThreadState.builder();
        for (RedoThread currentThread : currentState.getThreads()) {
            final RedoThread.Builder threadBuilder = duplicateThreadState(builder.thread(), currentThread);
            if (currentThread.getThreadId() == threadId) {
                threadBuilder.status("OPEN")
                        .enabled("PUBLIC")
                        .disabledScn(Scn.valueOf(0))
                        .disabledTime(null);
            }
            builder = threadBuilder.build();
        }
        return builder.build();
    }

    /**
     * Transitions a redo thread to offline mode.
     *
     * @param currentState the current state to be mutated, should not be {@code null}
     * @param threadId the thread id
     * @param changes the number of changes to advance by before going offline, can be {@code 0}.
     * @return a new redo thread state with the thread modifications
     */
    private static RedoThreadState transitionRedoThreadToOffline(RedoThreadState currentState, int threadId, int changes) {
        // Advance the thread's last position details
        RedoThreadState newCurrentState = advanceRedoThreadState(currentState, threadId, changes, false);

        // Checkpoint the node
        final int checkpointScn = newCurrentState.getRedoThread(threadId).getLastRedoScn().asBigInteger().intValue();
        newCurrentState = checkpointRedoThreadState(newCurrentState, threadId, checkpointScn);

        RedoThreadState.Builder builder = RedoThreadState.builder();
        for (RedoThread currentThread : newCurrentState.getThreads()) {
            final RedoThread.Builder threadBuilder = duplicateThreadState(builder.thread(), currentThread);
            if (currentThread.getThreadId() == threadId) {
                threadBuilder.status("CLOSED")
                        .enabled("DISABLED")
                        .disabledScn(currentThread.getCheckpointScn())
                        .disabledTime(Instant.now());
            }
            builder = threadBuilder.build();
        }
        return builder.build();
    }

    /**
     * This method advances the provided redo thread state for a given thread by the specified number of changes.
     *
     * @param currentState the current state to be mutated, should not be {@code null}
     * @param threadId the thread id
     * @param changes the number of changes to advance by
     * @param increaseSequence whether to increase the sequence number
     * @return a new redo thread state with the thread modifications
     */
    private static RedoThreadState advanceRedoThreadState(RedoThreadState currentState, int threadId, int changes, boolean increaseSequence) {
        RedoThreadState.Builder builder = RedoThreadState.builder();
        for (RedoThread currentThread : currentState.getThreads()) {
            final RedoThread.Builder threadBuilder = duplicateThreadState(builder.thread(), currentThread);
            if (currentThread.getThreadId() == threadId) {
                // When advancing by changes, update the last redo attributes
                if (increaseSequence) {
                    threadBuilder.lastRedoSequenceNumber(currentThread.getLastRedoSequenceNumber() + 1)
                            .currentSequenceNumber(currentThread.getCurrentSequenceNumber() + 1);
                }
                threadBuilder.lastRedoScn(currentThread.getLastRedoScn().add(Scn.valueOf(changes)))
                        .lastRedoBlock(currentThread.getLastRedoBlock() + changes)
                        .lastRedoTime(currentThread.getLastRedoTime().plus(changes, ChronoUnit.MINUTES));
            }
            builder = threadBuilder.build();
        }
        return builder.build();
    }

    /**
     * Helper method to duplicate the existing immutable thread state for mutation operations.
     *
     * @param builder the new redo thread builder to create a thread within
     * @param thread the existing thread to be duplicated
     * @return the mutated redo thread builder
     */
    private static RedoThread.Builder duplicateThreadState(RedoThread.Builder builder, RedoThread thread) {
        return builder.threadId(thread.getThreadId())
                .status(thread.getStatus())
                .enabled(thread.getEnabled())
                .logGroups(thread.getLogGroups())
                .instanceName(thread.getInstanceName())
                .openTime(thread.getOpenTime())
                .currentGroupNumber(thread.getCurrentGroupNumber())
                .currentSequenceNumber(thread.getCurrentSequenceNumber())
                .checkpointScn(thread.getCheckpointScn())
                .checkpointTime(thread.getCheckpointTime())
                .enabledScn(thread.getEnabledScn())
                .enabledTime(thread.getEnabledTime())
                .disabledScn(thread.getDisabledScn())
                .disabledTime(thread.getDisabledTime())
                .lastRedoSequenceNumber(thread.getLastRedoSequenceNumber())
                .lastRedoBlock(thread.getLastRedoBlock())
                .lastRedoScn(thread.getLastRedoScn())
                .lastRedoTime(thread.getLastRedoTime())
                .conId(thread.getConId());
    }

    private static RedoThreadState.Builder duplicateState(RedoThreadState threadState) {
        RedoThreadState.Builder builder = RedoThreadState.builder();
        for (RedoThread redoThread : threadState.getThreads()) {
            builder = duplicateThreadState(builder.thread(), redoThread).build();
        }
        return builder;
    }
}
