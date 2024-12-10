/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Strings;
import io.debezium.util.Testing;

/**
 * Integration tests for the {@link LogFileCollector} implementation.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER, reason = "LogMiner specific")
public class LogFileCollectorIT extends AbstractConnectorTest {

    @Rule
    public final TestRule skipAdapterRule = new SkipTestDependingOnAdapterNameRule();

    private static OracleConnection connection;

    @BeforeClass
    public static void beforeSuperClass() throws SQLException {
        try (OracleConnection adminConnection = TestHelper.adminConnection(true)) {
            adminConnection.removeAllLogFilesFromLogMinerSession();
        }
        connection = TestHelper.defaultConnection(true);
        TestHelper.forceFlushOfRedoLogsToArchiveLogs();
    }

    @AfterClass
    public static void closeConnection() throws SQLException {
        if (connection != null && connection.isConnected()) {
            connection.close();
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
        final int instances = getNumberOfInstances(connection);

        // case 1 : oldest scn = current scn
        Scn currentScn = connection.getCurrentScn();
        List<LogFile> redoFiles = getLogFileCollector(Duration.ofHours(0L), false, null).getLogs(currentScn);
        assertThat(redoFiles).hasSize(instances); // just the current redo log

        // case 2 : oldest scn = oldest in not cleared archive
        List<Scn> oneDayArchivedNextScn = getOneDayArchivedLogNextScn(connection);
        Scn oldestArchivedScn = getOldestArchivedScn(oneDayArchivedNextScn);
        List<LogFile> files = getLogFileCollector(Duration.ofHours(0L), false, null).getLogs(oldestArchivedScn);
        assertLogFilesHaveNoGaps(instances, files, oneDayArchivedNextScn);

        files = getLogFileCollector(Duration.ofHours(0L), false, null).getLogs(oldestArchivedScn.subtract(Scn.ONE));
        assertLogFilesHaveNoGaps(instances, files, oneDayArchivedNextScn);
    }

    @Test
    @FixFor("DBZ-3561")
    public void shouldOnlyReturnArchiveLogs() throws Exception {
        List<LogFile> files = getLogFileCollector(Duration.ofHours(0L), true, null).getLogs(Scn.valueOf(0));
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
        List<LogFile> files = getLogFileCollector(Duration.ofHours(1L), true, "LOG_ARCHIVE_DEST_1").getLogs(Scn.valueOf(0));
        assertThat(files.isEmpty()).isFalse();
        files.forEach(file -> assertThat(file.getType()).isEqualTo(LogFile.Type.ARCHIVE));
    }

    private LogFileCollector getLogFileCollector(Duration logRetention, boolean archiveLogsOnly, String destinationName) {
        Configuration.Builder builder = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.ARCHIVE_LOG_HOURS, logRetention.toHours())
                .with(OracleConnectorConfig.LOG_MINING_ARCHIVE_LOG_ONLY_MODE, Boolean.toString(archiveLogsOnly));

        if (!Strings.isNullOrBlank(destinationName)) {
            builder = builder.with(OracleConnectorConfig.ARCHIVE_DESTINATION_NAME, destinationName);
        }

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(builder.build());
        return new LogFileCollector(connectorConfig, connection);
    }

    private void assertLogFilesHaveNoGaps(int instances, List<LogFile> logFiles, List<Scn> scnList) {
        final Set<Integer> threads = logFiles.stream().map(LogFile::getThread).collect(Collectors.toSet());
        assertThat(threads).hasSize(instances);

        int totalThreadLogs = 0;
        for (Integer thread : threads) {
            List<LogFile> threadLogs = logFiles.stream().filter(l -> l.getThread() == thread).collect(Collectors.toList());
            BigInteger min = threadLogs.stream().map(LogFile::getSequence).min(BigInteger::compareTo).orElse(BigInteger.ZERO);
            BigInteger max = threadLogs.stream().map(LogFile::getSequence).max(BigInteger::compareTo).orElse(BigInteger.ZERO);
            assertThat(threadLogs).hasSize(max.subtract(min).intValue() + 1);
            totalThreadLogs += threadLogs.size();
            for (int i = min.intValue(); i <= max.intValue(); i++) {
                final int sequence = i;
                long hits = threadLogs.stream().filter(l -> l.getSequence().intValue() == sequence).count();
                assertThat(hits).isEqualTo(1);
            }
        }
        assertThat(totalThreadLogs).isEqualTo(logFiles.size());

        for (Scn scn : scnList) {
            assertThat(logFiles.stream().anyMatch(l -> l.isScnInLogFileRange(scn))).isTrue();
        }
    }

    private Scn getOldestArchivedScn(List<Scn> oneDayArchivedNextScn) {
        return oneDayArchivedNextScn.stream().min(Scn::compareTo).orElse(Scn.NULL);
    }

    private List<Scn> getOneDayArchivedLogNextScn(OracleConnection conn) throws SQLException {
        List<Scn> allArchivedNextScn = new ArrayList<>();
        try (
                PreparedStatement st = conn.connection(false).prepareStatement(
                        "SELECT NAME AS FILE_NAME, NEXT_CHANGE# AS NEXT_CHANGE FROM V$ARCHIVED_LOG " +
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
