/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;

import org.junit.jupiter.api.Test;

import io.debezium.connector.mysql.jdbc.MySqlConnection;

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
}