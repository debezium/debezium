/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.fest.assertions.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.LogMinerHelper;
import io.debezium.connector.oracle.logminer.SqlUtils;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
 * This subclasses common OracleConnectorIT for LogMiner adaptor
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER, reason = "LogMiner specific tests")
@Ignore(value = "The super class before method fails, needs investigation - alter session insufficient privileges")
public class LogMinerHelperIT extends AbstractConnectorTest {

    @Rule
    public final TestRule skipAdapterRule = new SkipTestDependingOnAdapterNameRule();

    private static OracleConnection connection;
    private static OracleConnection lmConnection;
    private static Connection conn;

    @BeforeClass
    public static void beforeSuperClass() throws SQLException {
        connection = TestHelper.testConnection();

        lmConnection = TestHelper.testConnection();
        conn = lmConnection.connection(false);

        lmConnection.resetSessionToCdb();
        LogMinerHelper.removeLogFilesFromMining(conn);
    }

    @AfterClass
    public static void closeConnection() throws SQLException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
        if (lmConnection != null && lmConnection.isConnected()) {
            lmConnection.close();
        }
        if (connection != null && connection.isConnected()) {
            connection.close();
        }
    }

    @Before
    public void before() throws SQLException {
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
    }

    @Test
    public void shouldAddRightOnlineRedoFiles() throws Exception {
        // case 1 : oldest scn = current scn
        long currentScn = LogMinerHelper.getCurrentScn(conn);
        LogMinerHelper.setRedoLogFilesForMining(conn, currentScn);
        assertThat(getNumberOfAddedLogFiles(conn) == 1).isTrue();

        // case 2 : oldest scn = oldest in online redo
        Map<String, String> redoLogFiles = LogMinerHelper.getMap(conn, SqlUtils.allOnlineLogsQuery(), "-1");
        Long oldestScn = getOldestOnlineScn(redoLogFiles);
        LogMinerHelper.setRedoLogFilesForMining(conn, oldestScn);
        // make sure this method will not add duplications
        LogMinerHelper.setRedoLogFilesForMining(conn, oldestScn);
        assertThat(getNumberOfAddedLogFiles(conn) == redoLogFiles.size() - 1).isTrue();

        // case 3 :
        oldestScn -= 1;
        LogMinerHelper.setRedoLogFilesForMining(conn, oldestScn);
        assertThat(getNumberOfAddedLogFiles(conn) == (redoLogFiles.size())).isTrue();
    }

    @Test
    public void shouldAddRightArchivedRedoFiles() throws Exception {
        // case 1 : oldest scn = current scn
        long currentScn = LogMinerHelper.getCurrentScn(conn);
        Map<String, String> archivedRedoFiles = LogMinerHelper.getMap(conn, SqlUtils.oneDayArchivedLogsQuery(currentScn), "-1");
        assertThat(archivedRedoFiles.size() == 0).isTrue();

        // case 2: oldest scn = oldest in not cleared archive
        List<BigDecimal> oneDayArchivedNextScn = getOneDayArchivedLogNextScn(conn);
        long oldestArchivedScn = getOldestArchivedScn(oneDayArchivedNextScn);
        Map<String, Long> archivedLogsForMining = LogMinerHelper.getArchivedLogFilesForOffsetScn(conn, oldestArchivedScn);
        assertThat(archivedLogsForMining.size() == (oneDayArchivedNextScn.size() - 1)).isTrue();

        archivedRedoFiles = LogMinerHelper.getMap(conn, SqlUtils.oneDayArchivedLogsQuery(oldestArchivedScn - 1), "-1");
        assertThat(archivedRedoFiles.size() == (oneDayArchivedNextScn.size())).isTrue();
    }

    @Test
    public void shouldAddRightRedoFiles() throws Exception {
        List<BigDecimal> oneDayArchivedNextScn = getOneDayArchivedLogNextScn(conn);
        long oldestArchivedScn = getOldestArchivedScn(oneDayArchivedNextScn);
        LogMinerHelper.setRedoLogFilesForMining(conn, oldestArchivedScn);

        // eliminate duplications
        Map<String, Long> onlineLogFilesForMining = LogMinerHelper.getOnlineLogFilesForOffsetScn(conn, oldestArchivedScn);
        Map<String, Long> archivedLogFilesForMining = LogMinerHelper.getArchivedLogFilesForOffsetScn(conn, oldestArchivedScn);
        List<String> archivedLogFiles = archivedLogFilesForMining.entrySet().stream()
                .filter(e -> !onlineLogFilesForMining.values().contains(e.getValue())).map(Map.Entry::getKey).collect(Collectors.toList());
        int archivedLogFilesCount = archivedLogFiles.size();

        Map<String, String> redoLogFiles = LogMinerHelper.getMap(conn, SqlUtils.allOnlineLogsQuery(), "-1");
        assertThat(getNumberOfAddedLogFiles(conn) == (redoLogFiles.size() + archivedLogFilesCount)).isTrue();
    }

    @Test
    public void shouldCalculateAbandonTransactions() throws Exception {
        Map<String, String> redoLogFiles = LogMinerHelper.getMap(conn, SqlUtils.allOnlineLogsQuery(), "-1");
        Long oldestOnlineScn = getOldestOnlineScn(redoLogFiles);
        Optional<Long> abandonWatermark = LogMinerHelper.getLastScnToAbandon(conn, oldestOnlineScn, 1);
        assertThat(abandonWatermark.isPresent()).isTrue();

        long currentScn = LogMinerHelper.getCurrentScn(conn);
        abandonWatermark = LogMinerHelper.getLastScnToAbandon(conn, currentScn, 1);
        assertThat(abandonWatermark.isPresent()).isFalse();

        List<BigDecimal> oneDayArchivedNextScn = getOneDayArchivedLogNextScn(conn);
        long oldestArchivedScn = getOldestArchivedScn(oneDayArchivedNextScn);
        abandonWatermark = LogMinerHelper.getLastScnToAbandon(conn, oldestArchivedScn, 1);
        assertThat(abandonWatermark.isPresent()).isTrue();

        long twoHoursAgoScn;
        String scnQuery = "with minus_one as (select (systimestamp - INTERVAL '2' HOUR) as diff from dual) " +
                "select timestamp_to_scn(diff) from minus_one";
        try (PreparedStatement ps = conn.prepareStatement(scnQuery);
                ResultSet rs = ps.executeQuery()) {
            rs.next();
            twoHoursAgoScn = rs.getBigDecimal(1).longValue();
        }
        String query = SqlUtils.diffInDaysQuery(twoHoursAgoScn);
        Float diffInDays = (Float) LogMinerHelper.getSingleResult(conn, query, LogMinerHelper.DATATYPE.FLOAT);
        assertThat(Math.round(diffInDays * 24) == 2).isTrue();

        diffInDays += 0.6F; // + 4 hours
        abandonWatermark = LogMinerHelper.getLastScnToAbandon(conn, twoHoursAgoScn, Math.round(diffInDays * 24));
        assertThat(abandonWatermark.isPresent()).isFalse();

        abandonWatermark = LogMinerHelper.getLastScnToAbandon(conn, twoHoursAgoScn, 1);
        assertThat(abandonWatermark.isPresent()).isTrue();

        abandonWatermark = LogMinerHelper.getLastScnToAbandon(conn, twoHoursAgoScn, 2);
        assertThat(abandonWatermark.isPresent()).isTrue();

        abandonWatermark = LogMinerHelper.getLastScnToAbandon(conn, twoHoursAgoScn, 3);
        assertThat(abandonWatermark.isPresent()).isFalse();
    }

    private Long getOldestOnlineScn(Map<String, String> redoLogFiles) throws Exception {
        Optional<BigDecimal> scn = redoLogFiles.values().stream().map(BigDecimal::new).min(BigDecimal::compareTo);
        Long oldestScn;
        if (scn.isPresent()) {
            oldestScn = scn.get().longValue();
        }
        else {
            throw new Exception("cannot get oldest scn");
        }
        return oldestScn;
    }

    private Long getOldestArchivedScn(List<BigDecimal> oneDayArchivedNextScn) throws Exception {
        long oldestArchivedScn;
        Optional<BigDecimal> archivedScn = oneDayArchivedNextScn.stream().min(BigDecimal::compareTo);
        if (archivedScn.isPresent()) {
            oldestArchivedScn = archivedScn.get().longValue();
        }
        else {
            throw new Exception("cannot get oldest archived scn");
        }
        return oldestArchivedScn;
    }

    private static int getNumberOfAddedLogFiles(Connection conn) throws SQLException {
        int counter = 0;
        try (PreparedStatement ps = conn.prepareStatement("select * from V$LOGMNR_LOGS");
                ResultSet result = ps.executeQuery()) {
            while (result.next()) {
                counter++;
            }
        }
        return counter;
    }

    private List<BigDecimal> getOneDayArchivedLogNextScn(Connection conn) throws SQLException {
        List<BigDecimal> allArchivedNextScn = new ArrayList<>();
        try (
                PreparedStatement st = conn.prepareStatement("SELECT NAME AS FILE_NAME, NEXT_CHANGE# AS NEXT_CHANGE FROM V$ARCHIVED_LOG " +
                        " WHERE NAME IS NOT NULL AND FIRST_TIME >= SYSDATE - 1 AND ARCHIVED = 'YES' " +
                        " AND STATUS = 'A' ORDER BY 2");
                ResultSet rs = st.executeQuery()) {
            while (rs.next()) {
                allArchivedNextScn.add(rs.getBigDecimal(2));
            }
        }
        return allArchivedNextScn;
    }

}
