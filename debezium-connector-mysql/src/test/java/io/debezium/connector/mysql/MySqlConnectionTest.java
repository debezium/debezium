/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.jdbc.BinlogFieldReader;
import io.debezium.connector.mysql.gtid.MySqlGtidSet;
import io.debezium.connector.mysql.gtid.MySqlGtidSetFactory;
import io.debezium.connector.mysql.jdbc.MySqlConnection;
import io.debezium.connector.mysql.jdbc.MySqlConnectionConfiguration;
import io.debezium.jdbc.JdbcConfiguration;

/**
 * Unit test for MySqlConnection network error detection and binlog position validation.
 *
 * @author Arya Dharmadhikari
 */

public class MySqlConnectionTest {

    private static final String BINLOG_FILE = "mysql-bin-changelog.210661";
    private static final String STORED_GTID = "52dfc80f-df0d-3b9b-905b-4eb20dad19a7:1-2191466007";
    private static final String SERVER_GTID = "52dfc80f-df0d-3b9b-905b-4eb20dad19a7:2234217821-2468638141";

    @Test
    public void shouldIdentifyConnectionErrorSQLStates() {
        // Specific network-related connection failures
        assertThat(MySqlConnection.isNetworkError(new SQLException("Cannot connect", "08001"))).isTrue();
        assertThat(MySqlConnection.isNetworkError(new SQLException("Connection does not exist", "08003"))).isTrue();
        assertThat(MySqlConnection.isNetworkError(new SQLException("Server rejected connection", "08004"))).isTrue();
        assertThat(MySqlConnection.isNetworkError(new SQLException("Connection failure", "08006"))).isTrue();
        assertThat(MySqlConnection.isNetworkError(new SQLException("Communication link failure", "08S01"))).isTrue();
    }

    @Test
    public void shouldNotIdentifyNonConnectionErrors() {
        assertThat(MySqlConnection.isNetworkError(new SQLException("Syntax error", "42000"))).isFalse();
        assertThat(MySqlConnection.isNetworkError(new SQLException("Unknown command", "42S02"))).isFalse();
        assertThat(MySqlConnection.isNetworkError(new SQLException("Access denied", "28000"))).isFalse();
        assertThat(MySqlConnection.isNetworkError(new SQLException("Duplicate key", "23000"))).isFalse();
    }

    @Test
    public void shouldHandleNullSQLStateGracefully() {
        assertThat(MySqlConnection.isNetworkError(new SQLException("Error with no SQLState", ""))).isFalse();
    }

    @Test
    public void shouldHandleEmptySQLStateGracefully() {
        assertThat(MySqlConnection.isNetworkError(new SQLException("Error with empty SQLState", ""))).isFalse();
    }

    @Test
    public void shouldCloseJdbcConnectionWhenConstructionFails() throws SQLException {
        final MySqlConnectionConfiguration connectionConfig = mock(MySqlConnectionConfiguration.class);
        final Connection jdbcConnection = mock(Connection.class);
        final Statement statement = mock(Statement.class);
        final ResultSet resultSet = mock(ResultSet.class);
        final SQLException permissionError = new SQLException("Access denied", "42000");

        when(connectionConfig.config()).thenReturn(JdbcConfiguration.empty());
        when(connectionConfig.factory()).thenReturn(config -> jdbcConnection);
        when(jdbcConnection.createStatement()).thenReturn(statement);
        when(jdbcConnection.isClosed()).thenReturn(false);
        when(statement.getConnection()).thenReturn(jdbcConnection);
        when(statement.executeQuery(MySqlConnection.BINARY_LOG_STATUS_STATEMENT)).thenThrow(permissionError);
        when(statement.executeQuery("SELECT VERSION()")).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getString(1)).thenReturn("8.4.0");

        assertThatThrownBy(() -> new MySqlConnection(connectionConfig, mock(BinlogFieldReader.class)))
                .isInstanceOf(DebeziumException.class)
                .hasCause(permissionError);

        verify(jdbcConnection).close();
    }

    private MySqlConnection connectionWithRealPositionCheck() {
        final MySqlConnection connection = mock(MySqlConnection.class);
        doCallRealMethod().when(connection).isBinlogPositionAvailable(any(), any(), any());
        return connection;
    }

    @Test
    public void shouldValidateBinlogFileWhenGtidModeIsNotEnabled() {
        final MySqlConnection connection = connectionWithRealPositionCheck();
        final BinlogConnectorConfig config = mock(BinlogConnectorConfig.class);

        when(connection.isGtidModeEnabled()).thenReturn(false);
        when(connection.knownGtidSet()).thenReturn(new MySqlGtidSet(SERVER_GTID));
        when(connection.availableBinlogFiles()).thenReturn(List.of(BINLOG_FILE));

        assertThat(connection.isBinlogPositionAvailable(config, STORED_GTID, BINLOG_FILE)).isTrue();
        verify(connection, never()).purgedGtidSet();
    }

    @Test
    public void shouldRejectPositionWhenGtidModeIsNotEnabledAndBinlogFileIsPurged() {
        final MySqlConnection connection = connectionWithRealPositionCheck();
        final BinlogConnectorConfig config = mock(BinlogConnectorConfig.class);

        when(connection.isGtidModeEnabled()).thenReturn(false);
        when(connection.knownGtidSet()).thenReturn(new MySqlGtidSet(SERVER_GTID));
        when(connection.availableBinlogFiles()).thenReturn(List.of("mysql-bin-changelog.210999"));

        assertThat(connection.isBinlogPositionAvailable(config, STORED_GTID, BINLOG_FILE)).isFalse();
    }

    @Test
    public void shouldRejectPositionWhenOffsetHasGtidsButServerHasNone() {
        final MySqlConnection connection = connectionWithRealPositionCheck();
        final BinlogConnectorConfig config = mock(BinlogConnectorConfig.class);

        when(connection.isGtidModeEnabled()).thenReturn(false);
        when(connection.knownGtidSet()).thenReturn(new MySqlGtidSet(""));

        assertThat(connection.isBinlogPositionAvailable(config, STORED_GTID, BINLOG_FILE)).isFalse();
        verify(connection, never()).availableBinlogFiles();
    }

    @Test
    public void shouldRejectGtidSetTheServerDoesNotRetainWhenGtidModeIsEnabled() {
        final MySqlConnection connection = connectionWithRealPositionCheck();
        final BinlogConnectorConfig config = mock(BinlogConnectorConfig.class);

        when(config.getGtidSetFactory()).thenReturn(new MySqlGtidSetFactory());
        when(config.getGtidSourceFilter()).thenReturn(uuid -> true);
        when(connection.isGtidModeEnabled()).thenReturn(true);
        when(connection.knownGtidSet()).thenReturn(new MySqlGtidSet(SERVER_GTID));

        assertThat(connection.isBinlogPositionAvailable(config, STORED_GTID, BINLOG_FILE)).isFalse();
    }

    @Test
    public void shouldRejectStoredGtidSetWhoseRemainingRangeWasPurged() {
        final MySqlConnection connection = connectionWithRealPositionCheck();
        final BinlogConnectorConfig config = mock(BinlogConnectorConfig.class);
        final String uuid = "52dfc80f-df0d-3b9b-905b-4eb20dad19a7";

        when(config.getGtidSetFactory()).thenReturn(new MySqlGtidSetFactory());
        when(config.getGtidSourceFilter()).thenReturn(id -> true);
        when(connection.isGtidModeEnabled()).thenReturn(true);
        when(connection.knownGtidSet()).thenReturn(new MySqlGtidSet(uuid + ":1-200"));
        when(connection.purgedGtidSet()).thenReturn(new MySqlGtidSet(uuid + ":1-150"));
        // GTID_SUBTRACT runs on the server; the two answers are what it returns for the sets above
        when(connection.subtractGtidSet(any(), any()))
                .thenReturn(new MySqlGtidSet(uuid + ":101-200"), new MySqlGtidSet(uuid + ":151-200"));

        assertThat(connection.isBinlogPositionAvailable(config, uuid + ":1-100", BINLOG_FILE)).isFalse();
    }

    @Test
    public void shouldAcceptStoredGtidSetWhenNothingStillNeededWasPurged() {
        final MySqlConnection connection = connectionWithRealPositionCheck();
        final BinlogConnectorConfig config = mock(BinlogConnectorConfig.class);
        final String uuid = "52dfc80f-df0d-3b9b-905b-4eb20dad19a7";

        when(config.getGtidSetFactory()).thenReturn(new MySqlGtidSetFactory());
        when(config.getGtidSourceFilter()).thenReturn(id -> true);
        when(connection.isGtidModeEnabled()).thenReturn(true);
        when(connection.knownGtidSet()).thenReturn(new MySqlGtidSet(uuid + ":1-200"));
        when(connection.purgedGtidSet()).thenReturn(new MySqlGtidSet(uuid + ":1-50"));
        when(connection.subtractGtidSet(any(), any()))
                .thenReturn(new MySqlGtidSet(uuid + ":101-200"), new MySqlGtidSet(uuid + ":101-200"));

        assertThat(connection.isBinlogPositionAvailable(config, uuid + ":1-100", BINLOG_FILE)).isTrue();
    }
}
