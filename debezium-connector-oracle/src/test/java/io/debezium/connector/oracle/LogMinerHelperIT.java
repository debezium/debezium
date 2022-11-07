/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.LogFile;
import io.debezium.connector.oracle.logminer.LogMinerHelper;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
 * This subclasses common OracleConnectorIT for LogMiner adaptor
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER, reason = "LogMiner specific tests")
public class LogMinerHelperIT extends AbstractConnectorTest {

    @Rule
    public final TestRule skipAdapterRule = new SkipTestDependingOnAdapterNameRule();

    private static OracleConnection conn;

    @BeforeClass
    public static void beforeSuperClass() throws SQLException {
        try (OracleConnection adminConnection = TestHelper.adminConnection(true)) {
            LogMinerHelper.removeLogFilesFromMining(adminConnection);
        }

        conn = TestHelper.defaultConnection(true);
        TestHelper.forceFlushOfRedoLogsToArchiveLogs();
    }

    @AfterClass
    public static void closeConnection() throws SQLException {
        if (conn != null && conn.isConnected()) {
            conn.close();
        }
    }

    @Before
    public void before() throws SQLException {
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @Test
    @FixFor("DBZ-3256")
    public void shouldAddCorrectLogFiles() throws Exception {
        final int instances = getNumberOfInstances(conn);

        // case 1 : oldest scn = current scn
        Scn currentScn = conn.getCurrentScn();
        List<LogFile> files = LogMinerHelper.getLogFilesForOffsetScn(conn, currentScn, Duration.ofHours(0L), false, null);
        assertThat(files).hasSize(instances); // just the current redo log

        // case 2 : oldest scn = oldest in not cleared archive
        List<Scn> oneDayArchivedNextScn = getOneDayArchivedLogNextScn(conn);
        Scn oldestArchivedScn = getOldestArchivedScn(oneDayArchivedNextScn);
        files = LogMinerHelper.getLogFilesForOffsetScn(conn, oldestArchivedScn, Duration.ofHours(0L), false, null);
        assertThat(files.size()).isEqualTo(oneDayArchivedNextScn.size() + instances - 1);

        files = LogMinerHelper.getLogFilesForOffsetScn(conn, oldestArchivedScn.subtract(Scn.valueOf(1L)), Duration.ofHours(0L), false, null);
        assertThat(files.size()).isEqualTo(oneDayArchivedNextScn.size() + instances);
    }

    @Test
    @FixFor("DBZ-3256")
    public void shouldSetCorrectLogFiles() throws Exception {
        List<Scn> oneDayArchivedNextScn = getOneDayArchivedLogNextScn(conn);
        Scn oldestArchivedScn = getOldestArchivedScn(oneDayArchivedNextScn);
        LogMinerHelper.setLogFilesForMining(conn, oldestArchivedScn, Duration.ofHours(0L), false, null, 5, Duration.ofSeconds(1), Duration.ofSeconds(60));

        List<LogFile> files = LogMinerHelper.getLogFilesForOffsetScn(conn, oldestArchivedScn, Duration.ofHours(0L), false, null);
        assertThat(files.size()).isEqualTo(getNumberOfAddedLogFiles(conn));
    }

    @Test
    @FixFor("DBZ-3561")
    public void shouldOnlyReturnArchiveLogs() throws Exception {
        List<LogFile> files = LogMinerHelper.getLogFilesForOffsetScn(conn, Scn.valueOf(0), Duration.ofHours(0L), true, null);
        files.forEach(file -> assertThat(file.getType()).isEqualTo(LogFile.Type.ARCHIVE));
    }

    @Test
    @FixFor("DBZ-3661")
    public void shouldGetArchiveLogsWithDestinationSpecified() throws Exception {
        // First force all redo logs to be flushed to archives to guarantee there will be some.
        try (OracleConnection admin = TestHelper.adminConnection(true)) {
            admin.execute("ALTER SYSTEM SWITCH ALL LOGFILE");
            // Wait 5 seconds to give Oracle time to toggle the ARC process
            Thread.sleep(5000);
        }

        // Test environment always has 1 destination at LOG_ARCHIVE_DEST_1
        List<LogFile> files = LogMinerHelper.getLogFilesForOffsetScn(conn, Scn.valueOf(0), Duration.ofHours(1), true, "LOG_ARCHIVE_DEST_1");
        assertThat(files.size()).isGreaterThan(0);
        files.forEach(file -> assertThat(file.getType()).isEqualTo(LogFile.Type.ARCHIVE));
    }

    private Scn getOldestArchivedScn(List<Scn> oneDayArchivedNextScn) {
        return oneDayArchivedNextScn.stream().min(Scn::compareTo).orElse(Scn.NULL);
    }

    private static int getNumberOfAddedLogFiles(OracleConnection conn) throws SQLException {
        int counter = 0;
        try (PreparedStatement ps = conn.connection(false).prepareStatement("select * from V$LOGMNR_LOGS");
                ResultSet result = ps.executeQuery()) {
            while (result.next()) {
                counter++;
            }
        }
        return counter;
    }

    private List<Scn> getOneDayArchivedLogNextScn(OracleConnection conn) throws SQLException {
        List<Scn> allArchivedNextScn = new ArrayList<>();
        try (
                PreparedStatement st = conn.connection(false).prepareStatement("SELECT NAME AS FILE_NAME, NEXT_CHANGE# AS NEXT_CHANGE FROM V$ARCHIVED_LOG " +
                        " WHERE NAME IS NOT NULL AND FIRST_TIME >= SYSDATE - 1 AND ARCHIVED = 'YES' " +
                        " AND STATUS = 'A' ORDER BY 2");
                ResultSet rs = st.executeQuery()) {
            while (rs.next()) {
                allArchivedNextScn.add(Scn.valueOf(rs.getString(2)));
            }
        }
        return allArchivedNextScn;
    }

    private static int getNumberOfInstances(OracleConnection connection) throws SQLException {
        // In an Oracle RAC environment, there will be multiple instances connected to the database.
        // This information can be queried from GV$INSTANCE.
        // For non-RAC environments, there will always just be one record here, the standalone instance.
        return connection.queryAndMap("SELECT COUNT(1) FROM GV$INSTANCE", rs -> {
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        });
    }
}
