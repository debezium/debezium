/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
import io.debezium.connector.oracle.logminer.Scn;
import io.debezium.connector.oracle.logminer.SqlUtils;
import io.debezium.connector.oracle.util.TestHelper;
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
        try (OracleConnection adminConnection = TestHelper.adminConnection()) {
            adminConnection.resetSessionToCdb();
            LogMinerHelper.removeLogFilesFromMining(adminConnection);
        }

        conn = TestHelper.defaultConnection();
        conn.resetSessionToCdb();
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
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
    }

    @Test
    public void shouldAddRightArchivedRedoFiles() throws Exception {
        // case 1 : oldest scn = current scn
        Scn currentScn = LogMinerHelper.getCurrentScn(conn);
        Map<String, String> archivedRedoFiles = LogMinerHelper.getMap(conn, SqlUtils.archiveLogsQuery(currentScn, Duration.ofHours(0L)), "-1");
        assertThat(archivedRedoFiles.size() == 0).isTrue();

        // case 2: oldest scn = oldest in not cleared archive
        List<Scn> oneDayArchivedNextScn = getOneDayArchivedLogNextScn(conn);
        Scn oldestArchivedScn = getOldestArchivedScn(oneDayArchivedNextScn);
        List<LogFile> archivedLogsForMining = LogMinerHelper.getArchivedLogFilesForOffsetScn(conn, oldestArchivedScn, Duration.ofHours(0L));
        if (oneDayArchivedNextScn.isEmpty()) {
            assertThat(archivedLogsForMining.size()).isEqualTo(oneDayArchivedNextScn.size());
        }
        else {
            assertThat(archivedLogsForMining.size()).isEqualTo(oneDayArchivedNextScn.size() - 1);
        }

        archivedRedoFiles = LogMinerHelper.getMap(conn, SqlUtils.archiveLogsQuery(oldestArchivedScn.subtract(Scn.valueOf(1L)), Duration.ofHours(0L)), "-1");
        assertThat(archivedRedoFiles.size() == (oneDayArchivedNextScn.size())).isTrue();
    }

    @Test
    public void shouldAddRightRedoFiles() throws Exception {
        List<Scn> oneDayArchivedNextScn = getOneDayArchivedLogNextScn(conn);
        Scn oldestArchivedScn = getOldestArchivedScn(oneDayArchivedNextScn);
        LogMinerHelper.setRedoLogFilesForMining(conn, oldestArchivedScn, Duration.ofHours(0L));

        // eliminate duplications
        List<LogFile> onlineLogFilesForMining = LogMinerHelper.getOnlineLogFilesForOffsetScn(conn, oldestArchivedScn);
        List<LogFile> archivedLogFilesForMining = LogMinerHelper.getArchivedLogFilesForOffsetScn(conn, oldestArchivedScn, Duration.ofHours(0L));
        List<String> redoLogFiles = onlineLogFilesForMining.stream().filter(e -> {
            for (LogFile log : archivedLogFilesForMining) {
                if (log.isSameRange(e)) {
                    return false;
                }
            }
            return true;
        }).map(LogFile::getFileName).collect(Collectors.toList());
        int redoLogFilesCount = redoLogFiles.size();

        if (!archivedLogFilesForMining.isEmpty()) {
            assertThat(onlineLogFilesForMining.size()).isGreaterThan(redoLogFilesCount);
            assertThat(getNumberOfAddedLogFiles(conn)).isGreaterThan(redoLogFilesCount);
        }
        else {
            assertThat(onlineLogFilesForMining.size()).isEqualTo(redoLogFilesCount);
            assertThat(getNumberOfAddedLogFiles(conn)).isEqualTo(redoLogFilesCount);
        }
    }

    private Scn getOldestArchivedScn(List<Scn> oneDayArchivedNextScn) throws Exception {
        Scn oldestArchivedScn;
        Optional<Scn> archivedScn = oneDayArchivedNextScn.stream().min(Scn::compareTo);
        if (archivedScn.isPresent()) {
            oldestArchivedScn = archivedScn.get();
        }
        else {
            oldestArchivedScn = Scn.ZERO;
        }
        return oldestArchivedScn;
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

}
