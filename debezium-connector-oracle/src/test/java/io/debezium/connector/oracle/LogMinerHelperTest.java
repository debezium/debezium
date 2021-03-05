/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import io.debezium.connector.oracle.logminer.LogMinerHelper;
import io.debezium.connector.oracle.logminer.Scn;

public class LogMinerHelperTest {

    private OracleConnection connection = Mockito.mock(OracleConnection.class);
    private int current;
    private String[][] mockRows;

    @Before
    public void beforeEach() throws Exception {

        current = 0;
        mockRows = new String[][]{};

        ResultSet rs = Mockito.mock(ResultSet.class);
        Connection conn = Mockito.mock(Connection.class);
        Mockito.when(connection.connection()).thenReturn(conn);
        Mockito.when(connection.connection(false)).thenReturn(conn);

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
                new String[]{ "logfile1", "103400", "11", "103700", "ACTIVE" },
                new String[]{ "logfile2", "103700", "12", "104000", "ACTIVE" }
        };

        Map<String, Scn> onlineLogs = LogMinerHelper.getOnlineLogFilesForOffsetScn(connection, 10L);
        assertEquals(onlineLogs.size(), 2);
        assertEquals(onlineLogs.get("logfile1"), Scn.valueOf(103400L));
        assertEquals(onlineLogs.get("logfile2"), Scn.valueOf(103700L));
    }

    @Test
    public void excludeLogsBeforeOffsetScn() throws Exception {

        mockRows = new String[][]{
                new String[]{ "logfile1", "103400", "11", "103700", "ACTIVE" },
                new String[]{ "logfile2", "103700", "12", "104000", "ACTIVE" },
                new String[]{ "logfile3", "500", "13", "103100", "ACTIVE" },
        };

        Map<String, Scn> onlineLogs = LogMinerHelper.getOnlineLogFilesForOffsetScn(connection, 600L);
        assertEquals(onlineLogs.size(), 2);
        assertNull(onlineLogs.get("logfile3"));
    }

    @Test
    public void nullsHandledAsMaxScn() throws Exception {

        mockRows = new String[][]{
                new String[]{ "logfile1", "103400", "11", "103700", "ACTIVE" },
                new String[]{ "logfile2", "103700", "12", "104000", "ACTIVE" },
                new String[]{ "logfile3", null, "13", "103100", "CURRENT" },
        };

        Map<String, Scn> onlineLogs = LogMinerHelper.getOnlineLogFilesForOffsetScn(connection, 600L);
        assertEquals(onlineLogs.size(), 3);
        assertEquals(onlineLogs.get("logfile3"), Scn.MAX);
    }

    @Test
    public void canHandleMaxScn() throws Exception {

        mockRows = new String[][]{
                new String[]{ "logfile1", "103400", "11", "103700", "ACTIVE" },
                new String[]{ "logfile2", "103700", "12", "104000", "ACTIVE" },
                new String[]{ "logfile3", "18446744073709551615", "13", "104300", "CURRENT" },
        };

        Map<String, Scn> onlineLogs = LogMinerHelper.getOnlineLogFilesForOffsetScn(connection, 600L);
        assertEquals(onlineLogs.size(), 3);
        assertEquals(onlineLogs.get("logfile3"), Scn.MAX);
    }

    @Test
    public void logsWithVeryLargeScnAreSupported() throws Exception {
        // Proves that a SCN larger than what long data type supports, is still handled appropriately
        String scnLonger = "18446744073709551615";

        mockRows = new String[][]{
                new String[]{ "logfile1", "103400", "11", "103700", "ACTIVE" },
                new String[]{ "logfile2", "103700", "12", "104000", "ACTIVE" },
                new String[]{ "logfile3", scnLonger, "13", "104300", "ACTIVE" },
        };

        Map<String, Scn> onlineLogs = LogMinerHelper.getOnlineLogFilesForOffsetScn(connection, 600L);
        assertEquals(onlineLogs.size(), 3);
        assertEquals(onlineLogs.get("logfile3"), Scn.valueof(scnLonger));
    }

    @Test
    public void archiveLogsWithRegularScns() throws Exception {

        mockRows = new String[][]{
                new String[]{ "logfile1", "103400", "11", "ACTIVE" },
                new String[]{ "logfile2", "103700", "12", "ACTIVE" }
        };

        Map<String, Scn> onlineLogs = LogMinerHelper.getArchivedLogFilesForOffsetScn(connection, 500L, Duration.ofDays(60));
        assertEquals(onlineLogs.size(), 2);
        assertEquals(onlineLogs.get("logfile1"), Scn.valueOf(103400L));
        assertEquals(onlineLogs.get("logfile2"), Scn.valueOf(103700L));
    }

    // Following are the same set of tests used for online logs but on archived logs
    @Test
    public void archiveExcludeLogsBeforeOffsetScn() throws Exception {

        mockRows = new String[][]{
                new String[]{ "logfile1", "103400", "11", },
                new String[]{ "logfile2", "103700", "12" },
                // the following would be omitted due to being older than 60 days
                // new String[]{ "logfile3", "500", "13" },
        };

        Map<String, Scn> onlineLogs = LogMinerHelper.getArchivedLogFilesForOffsetScn(connection, 600L, Duration.ofDays(60));
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

        Map<String, Scn> onlineLogs = LogMinerHelper.getArchivedLogFilesForOffsetScn(connection, 500L, Duration.ofDays(60));
        assertEquals(onlineLogs.size(), 3);
        assertEquals(onlineLogs.get("logfile3"), Scn.MAX);
    }

    @Test
    public void archiveLogsWithVeryLargeScnAreSupported() throws Exception {
        // Proves that a SCN larger than what long data type supports, is still handled appropriately
        String scnLonger = "18446744073709551615";

        mockRows = new String[][]{
                new String[]{ "logfile1", "103400", "11" },
                new String[]{ "logfile2", "103700", "12" },
                new String[]{ "logfile3", scnLonger, "13" },
        };

        Map<String, Scn> onlineLogs = LogMinerHelper.getArchivedLogFilesForOffsetScn(connection, 500L, Duration.ofDays(60));
        assertEquals(onlineLogs.size(), 3);
        assertEquals(onlineLogs.get("logfile3"), Scn.valueof(scnLonger));
    }
}
