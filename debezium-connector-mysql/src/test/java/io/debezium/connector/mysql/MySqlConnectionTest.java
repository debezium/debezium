/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.connector.binlog.jdbc.BinlogFieldReader;
import io.debezium.connector.mysql.jdbc.MySqlConnection;
import io.debezium.connector.mysql.jdbc.MySqlConnectionConfiguration;
import io.debezium.jdbc.JdbcConfiguration;

/**
 * Unit test for MySqlConnection network error detection.
 *
 * @author Arya Dharmadhikari
 */

public class MySqlConnectionTest {

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
}
