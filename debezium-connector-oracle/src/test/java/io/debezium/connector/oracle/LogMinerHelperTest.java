/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import io.debezium.connector.oracle.logminer.LogFile;
import io.debezium.connector.oracle.logminer.LogMinerHelper;
import io.debezium.jdbc.JdbcConnection;

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
        JdbcConnection.StatementFactory factory = Mockito.mock(JdbcConnection.StatementFactory.class);
        Mockito.when(factory.createStatement(conn)).thenReturn(pstmt);

        Mockito.when(conn.prepareStatement(anyString())).thenReturn(pstmt);
        Mockito.when(pstmt.executeQuery()).thenReturn(rs);
        Mockito.when(rs.next()).thenAnswer(it -> ++current > mockRows.length ? false : true);
        Mockito.when(rs.getString(anyInt())).thenAnswer(it -> {
            return mockRows[current - 1][(Integer) it.getArguments()[0] - 1];
        });
        Mockito.when(rs.getLong(anyInt())).thenAnswer(it -> {
            return Long.valueOf(mockRows[current - 1][(Integer) it.getArguments()[0] - 1]);
        });

        Mockito.doAnswer(a -> {
            JdbcConnection.ResultSetConsumer consumer = a.getArgument(1);
            consumer.accept(rs);
            return null;
        }).when(connection).query(anyString(), any(JdbcConnection.ResultSetConsumer.class));
    }

    @Test
    public void logsWithRegularScns() throws Exception {

        mockRows = new String[][]{
                new String[]{ "logfile1", "103400", "103700", "YES", null, "ARCHIVED", "1", "YES", "YES" },
                new String[]{ "logfile2", "103700", "104000", "NO", "ACTIVE", "ONLINE", "2", "NO", "NO" }
        };

        List<LogFile> logs = LogMinerHelper.getLogFilesForOffsetScn(connection, Scn.valueOf(10L), Duration.ofDays(60), false, null);
        assertThat(logs).hasSize(2);
        assertThat(getLogFileNextScnByName(logs, "logfile1")).isEqualTo(Scn.valueOf(103700L));
        assertThat(getLogFileNextScnByName(logs, "logfile2")).isEqualTo(Scn.valueOf(104000L));
    }

    @Test
    public void excludeLogsBeforeOffsetScn() throws Exception {

        mockRows = new String[][]{
                new String[]{ "logfile3", "103300", "103400", "YES", null, "ARCHIVED", "1", "YES", "YES" },
                new String[]{ "logfile1", "103400", "103700", "YES", null, "ARCHIVED", "2", "YES", "YES" },
                new String[]{ "logfile2", "103700", "104000", "NO", "ACTIVE", "ONLINE", "3", "NO", "NO" }
        };

        List<LogFile> logs = LogMinerHelper.getLogFilesForOffsetScn(connection, Scn.valueOf(103401), Duration.ofDays(60), false, null);
        assertThat(logs).hasSize(2);
        assertThat(getLogFileNextScnByName(logs, "logfile3")).isNull();
    }

    @Test
    public void nullsHandledAsMaxScn() throws Exception {

        mockRows = new String[][]{
                new String[]{ "logfile1", "103400", "103700", "YES", null, "ARCHIVED", "1", "YES", "YES" },
                new String[]{ "logfile2", "103700", "104000", "YES", null, "ARCHIVED", "2", "YES", "YES" },
                new String[]{ "logfile3", "104000", null, "NO", "CURRENT", "ONLINE", "3", "NO", "NO" }
        };

        List<LogFile> onlineLogs = LogMinerHelper.getLogFilesForOffsetScn(connection, Scn.valueOf(600L), Duration.ofDays(60), false, null);
        assertEquals(onlineLogs.size(), 3);
        assertEquals(getLogFileNextScnByName(onlineLogs, "logfile3"), Scn.MAX);
    }

    @Test
    public void canHandleMaxScn() throws Exception {

        mockRows = new String[][]{
                new String[]{ "logfile1", "103400", "103700", "YES", null, "ARCHIVED", "1", "YES", "YES" },
                new String[]{ "logfile2", "103700", "104000", "YES", null, "ARCHIVED", "2", "YES", "YES" },
                new String[]{ "logfile3", "104000", "18446744073709551615", "NO", "CURRENT", "ONLINE", "3", "NO", "NO" }
        };

        List<LogFile> onlineLogs = LogMinerHelper.getLogFilesForOffsetScn(connection, Scn.valueOf(600L), Duration.ofDays(60), false, null);
        assertEquals(onlineLogs.size(), 3);
        assertEquals(getLogFileNextScnByName(onlineLogs, "logfile3"), Scn.MAX);
    }

    @Test
    public void logsWithVeryLargeScnAreSupported() throws Exception {
        // Proves that a SCN larger than what long data type supports, is still handled appropriately
        String scnLonger = "18446744073709551615";

        mockRows = new String[][]{
                new String[]{ "logfile1", "103400", "103700", "YES", null, "ARCHIVED", "1", "YES", "YES" },
                new String[]{ "logfile2", "103700", "104000", "YES", null, "ARCHIVED", "2", "YES", "YES" },
                new String[]{ "logfile3", "104000", "18446744073709551615", "NO", "ACTIVE", "ONLINE", "3", "NO", "NO" }
        };

        List<LogFile> onlineLogs = LogMinerHelper.getLogFilesForOffsetScn(connection, Scn.valueOf(600L), Duration.ofDays(60), false, null);
        assertEquals(onlineLogs.size(), 3);
        assertEquals(getLogFileNextScnByName(onlineLogs, "logfile3"), Scn.valueOf(scnLonger));
    }

    private static Scn getLogFileNextScnByName(List<LogFile> logs, String name) {
        Optional<LogFile> file = logs.stream().filter(l -> l.getFileName().equals(name)).findFirst();
        if (file.isPresent()) {
            return file.get().getNextScn();
        }
        return null;
    }
}
