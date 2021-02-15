/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.Map;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import io.debezium.connector.oracle.logminer.LogMinerHelper;
import io.debezium.doc.FixFor;

public class LogMinerHelperTest {

    private OracleConnection connection = Mockito.mock(OracleConnection.class);
    private int current;
    private String[][] mockRows;
    private String maxScnStr;
    private BigInteger maxScn;

    @Before
    public void beforeEach() throws Exception {

        current = 0;
        mockRows = new String[][]{};

        ResultSet rs = Mockito.mock(ResultSet.class);
        Connection conn = Mockito.mock(Connection.class);
        Mockito.when(connection.connection()).thenReturn(conn);
        Mockito.when(connection.connection(false)).thenReturn(conn);
        Mockito.when(connection.getOracleVersion())
                .thenReturn(OracleDatabaseVersion.parse("Oracle Database 12c Enterprise Edition Release 12.2.0.1.0 - 64bit Production"));
        maxScnStr = LogMinerHelper.getDatabaseMaxScnValue(connection);
        maxScn = new BigInteger(maxScnStr);

        PreparedStatement pstmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(conn.prepareStatement(anyString())).thenReturn(pstmt);
        Mockito.when(pstmt.executeQuery()).thenReturn(rs);
        Mockito.when(rs.next()).thenAnswer(it -> ++current > mockRows.length ? false : true);
        Mockito.when(rs.getString(anyInt())).thenAnswer(it -> {
            return mockRows[current - 1][(Integer) it.getArguments()[0] - 1];
        });
    }

    @Test
    public void logsWithRegularScns() throws Exception {

        mockRows = new String[][]{
                new String[]{ "logfile1", "103400", "11", "103700" },
                new String[]{ "logfile2", "103700", "12", "104000" }
        };

        Map<String, BigInteger> onlineLogs = LogMinerHelper.getOnlineLogFilesForOffsetScn(connection, 10L);
        assertEquals(onlineLogs.size(), 2);
        assertEquals(onlineLogs.get("logfile1"), BigInteger.valueOf(103400L));
        assertEquals(onlineLogs.get("logfile2"), BigInteger.valueOf(103700L));
    }

    @Test
    public void excludeLogsBeforeOffsetScn() throws Exception {

        mockRows = new String[][]{
                new String[]{ "logfile1", "103400", "11", "103700" },
                new String[]{ "logfile2", "103700", "12", "104000" },
                new String[]{ "logfile3", "500", "13", "103100" },
        };

        Map<String, BigInteger> onlineLogs = LogMinerHelper.getOnlineLogFilesForOffsetScn(connection, 600L);
        assertEquals(onlineLogs.size(), 2);
        assertNull(onlineLogs.get("logfile3"));
    }

    @Test
    public void nullsHandledAsMaxScn() throws Exception {

        mockRows = new String[][]{
                new String[]{ "logfile1", "103400", "11", "103700" },
                new String[]{ "logfile2", "103700", "12", "104000" },
                new String[]{ "logfile3", null, "13", "103100" },
        };

        Map<String, BigInteger> onlineLogs = LogMinerHelper.getOnlineLogFilesForOffsetScn(connection, 600L);
        assertEquals(onlineLogs.size(), 3);
        assertEquals(onlineLogs.get("logfile3"), maxScn);
    }

    @Test
    public void canHandleMaxScn() throws Exception {

        mockRows = new String[][]{
                new String[]{ "logfile1", "103400", "11", "103700" },
                new String[]{ "logfile2", "103700", "12", "104000" },
                new String[]{ "logfile3", maxScnStr, "13", "104300" },
        };

        Map<String, BigInteger> onlineLogs = LogMinerHelper.getOnlineLogFilesForOffsetScn(connection, 600L);
        assertEquals(onlineLogs.size(), 3);
        assertEquals(onlineLogs.get("logfile3"), maxScn);
    }

    @Test
    public void logsWithLongerScnAreSupported() throws Exception {

        // Proves that a SCN larger than what long data type supports, is still handled appropriately
        String scnLonger = "9295429630892703743";

        mockRows = new String[][]{
                new String[]{ "logfile1", "103400", "11", "103700" },
                new String[]{ "logfile2", "103700", "12", "104000" },
                new String[]{ "logfile3", scnLonger, "13", "104300" },
        };

        Map<String, BigInteger> onlineLogs = LogMinerHelper.getOnlineLogFilesForOffsetScn(connection, 600L);
        assertEquals(onlineLogs.size(), 3);
        assertEquals(onlineLogs.get("logfile3"), new BigInteger(scnLonger));
    }

    @Test
    public void archiveLogsWithRegularScns() throws Exception {

        mockRows = new String[][]{
                new String[]{ "logfile1", "103400", "11" },
                new String[]{ "logfile2", "103700", "12" }
        };

        Map<String, BigInteger> onlineLogs = LogMinerHelper.getArchivedLogFilesForOffsetScn(connection, 500L, Duration.ofDays(60));
        assertEquals(onlineLogs.size(), 2);
        assertEquals(onlineLogs.get("logfile1"), BigInteger.valueOf(103400L));
        assertEquals(onlineLogs.get("logfile2"), BigInteger.valueOf(103700L));
    }

    // Following are the same set of tests used for online logs but on archived logs
    @Test
    @Ignore // TODO: Is this test not passing a bug?
    public void archiveExcludeLogsBeforeOffsetScn() throws Exception {

        mockRows = new String[][]{
                new String[]{ "logfile1", "103400", "11" },
                new String[]{ "logfile2", "103700", "12" },
                new String[]{ "logfile3", "500", "13" },
        };

        Map<String, BigInteger> onlineLogs = LogMinerHelper.getArchivedLogFilesForOffsetScn(connection, 600L, Duration.ofDays(60));
        assertEquals(onlineLogs.size(), 2);
        assertNull(onlineLogs.get("logfile3"));
    }

    @Test
    public void archiveNullsHandledAsMaxScn() throws Exception {

        mockRows = new String[][]{
                new String[]{ "logfile1", "103400", "11", "103700" },
                new String[]{ "logfile2", "103700", "12", "104000" },
                new String[]{ "logfile3", null, "13", "104300" },
        };

        Map<String, BigInteger> onlineLogs = LogMinerHelper.getArchivedLogFilesForOffsetScn(connection, 500L, Duration.ofDays(60));
        assertEquals(onlineLogs.size(), 3);
        assertEquals(onlineLogs.get("logfile3"), maxScn);
    }

    @Test
    public void archiveCanHandleMaxScn() throws Exception {

        mockRows = new String[][]{
                new String[]{ "logfile1", "103400", "11" },
                new String[]{ "logfile2", "103700", "12" },
                new String[]{ "logfile3", maxScnStr, "13" },
        };

        Map<String, BigInteger> onlineLogs = LogMinerHelper.getArchivedLogFilesForOffsetScn(connection, 500L, Duration.ofDays(60));
        assertEquals(onlineLogs.size(), 3);
        assertEquals(onlineLogs.get("logfile3"), maxScn);
    }

    @Test
    public void archiveLogsWithLongerScnAreSupported() throws Exception {

        // Proves that a SCN larger than what long data type supports, is still handled appropriately
        String scnLonger = "9295429630892703743";

        mockRows = new String[][]{
                new String[]{ "logfile1", "103400", "11" },
                new String[]{ "logfile2", "103700", "12" },
                new String[]{ "logfile3", scnLonger, "13" },
        };

        Map<String, BigInteger> onlineLogs = LogMinerHelper.getArchivedLogFilesForOffsetScn(connection, 500L, Duration.ofDays(60));
        assertEquals(onlineLogs.size(), 3);
        assertEquals(onlineLogs.get("logfile3"), new BigInteger(scnLonger));
    }

    @Test
    @FixFor("DBZ-3001")
    public void testOracleMaxScn() throws Exception {
        final OracleConnection connection = Mockito.mock(OracleConnection.class);
        final ResultSet rs = Mockito.mock(ResultSet.class);
        final Connection conn = Mockito.mock(Connection.class);
        Mockito.when(connection.connection()).thenReturn(conn);
        Mockito.when(connection.connection(false)).thenReturn(conn);

        // Test Oracle 11
        String banner = "Oracle Database 11g Enterprise Edition Release 11.2.0.4.0 - 64bit Production";
        Mockito.when(connection.getOracleVersion()).thenReturn(OracleDatabaseVersion.parse(banner));
        assertThat(LogMinerHelper.getDatabaseMaxScnValue(connection)).isEqualTo("281474976710655");

        // Test Oracle 12.1
        banner = "Oracle Database 12c Enterprise Edition Release 12.1.0.0.0 - 64bit Production";
        Mockito.when(connection.getOracleVersion()).thenReturn(OracleDatabaseVersion.parse(banner));
        assertThat(LogMinerHelper.getDatabaseMaxScnValue(connection)).isEqualTo("281474976710655");

        // Test Oracle 12.2
        banner = "Oracle Database 12c Enterprise Edition Release 12.2.0.4.0 - 64bit Production";
        Mockito.when(connection.getOracleVersion()).thenReturn(OracleDatabaseVersion.parse(banner));
        assertThat(LogMinerHelper.getDatabaseMaxScnValue(connection)).isEqualTo("18446744073709551615");

        // Test Oracle 18.0
        banner = "Oracle Database 18c Enterprise Edition Release 18.0.0.0.0 - 64bit Production";
        Mockito.when(connection.getOracleVersion()).thenReturn(OracleDatabaseVersion.parse(banner));
        assertThat(LogMinerHelper.getDatabaseMaxScnValue(connection)).isEqualTo("18446744073709551615");

        // Test Oracle 18.1
        banner = "Oracle Database 18c Enterprise Edition Release 18.1.0.0.0 - 64bit Production";
        Mockito.when(connection.getOracleVersion()).thenReturn(OracleDatabaseVersion.parse(banner));
        assertThat(LogMinerHelper.getDatabaseMaxScnValue(connection)).isEqualTo("18446744073709551615");

        // Test Oracle 18.2
        banner = "Oracle Database 18c Enterprise Edition Release 18.0.0.0.0 - Production" + System.lineSeparator() + "Version 18.2.0.0.0";
        Mockito.when(connection.getOracleVersion()).thenReturn(OracleDatabaseVersion.parse(banner));
        assertThat(LogMinerHelper.getDatabaseMaxScnValue(connection)).isEqualTo("18446744073709551615");

        // Test Oracle 19.0
        banner = "Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production" + System.lineSeparator() + "Version 19.0.0.0.0";
        Mockito.when(connection.getOracleVersion()).thenReturn(OracleDatabaseVersion.parse(banner));
        assertThat(LogMinerHelper.getDatabaseMaxScnValue(connection)).isEqualTo("18446744073709551615");

        // Test Oracle 19.1
        banner = "Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production" + System.lineSeparator() + "Version 19.1.0.0.0";
        Mockito.when(connection.getOracleVersion()).thenReturn(OracleDatabaseVersion.parse(banner));
        assertThat(LogMinerHelper.getDatabaseMaxScnValue(connection)).isEqualTo("18446744073709551615");

        // Test Oracle 19.2
        banner = "Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production" + System.lineSeparator() + "Version 19.2.0.0.0";
        Mockito.when(connection.getOracleVersion()).thenReturn(OracleDatabaseVersion.parse(banner));
        assertThat(LogMinerHelper.getDatabaseMaxScnValue(connection)).isEqualTo("18446744073709551615");

        // Test Oracle 19.3
        banner = "Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production" + System.lineSeparator() + "Version 19.3.0.0.0";
        Mockito.when(connection.getOracleVersion()).thenReturn(OracleDatabaseVersion.parse(banner));
        assertThat(LogMinerHelper.getDatabaseMaxScnValue(connection)).isEqualTo("18446744073709551615");

        // Test Oracle 19.4
        banner = "Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production" + System.lineSeparator() + "Version 19.4.0.0.0";
        Mockito.when(connection.getOracleVersion()).thenReturn(OracleDatabaseVersion.parse(banner));
        assertThat(LogMinerHelper.getDatabaseMaxScnValue(connection)).isEqualTo("18446744073709551615");

        // Test Oracle 19.5
        banner = "Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production" + System.lineSeparator() + "Version 19.5.0.0.0";
        Mockito.when(connection.getOracleVersion()).thenReturn(OracleDatabaseVersion.parse(banner));
        assertThat(LogMinerHelper.getDatabaseMaxScnValue(connection)).isEqualTo("18446744073709551615");

        // Test Oracle 19.6
        banner = "Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production" + System.lineSeparator() + "Version 19.6.0.0.0";
        Mockito.when(connection.getOracleVersion()).thenReturn(OracleDatabaseVersion.parse(banner));
        assertThat(LogMinerHelper.getDatabaseMaxScnValue(connection)).isEqualTo("9295429630892703743");

        // Test Oracle 19.7
        banner = "Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production" + System.lineSeparator() + "Version 19.7.0.0.0";
        Mockito.when(connection.getOracleVersion()).thenReturn(OracleDatabaseVersion.parse(banner));
        assertThat(LogMinerHelper.getDatabaseMaxScnValue(connection)).isEqualTo("9295429630892703743");

        // Test Oracle 21
        banner = "Oracle Database 21c Enterprise Edition Release 21.0.0.0.0 - Production" + System.lineSeparator() + "Version 21.0.0.0.0";
        Mockito.when(connection.getOracleVersion()).thenReturn(OracleDatabaseVersion.parse(banner));
        assertThat(LogMinerHelper.getDatabaseMaxScnValue(connection)).isEqualTo("9295429630892703743");
    }
}
