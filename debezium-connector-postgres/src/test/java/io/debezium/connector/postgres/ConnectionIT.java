/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgres;

import java.sql.SQLException;

import org.junit.Test;

public class ConnectionIT {

    @Test
    public void shouldConnectToDefaulDatabase() throws SQLException {
        try (PostgresConnection conn = PostgresConnection.forTestDatabase("postgres");) {
            conn.connect();
        }
    }

    @Test
    public void shouldConnectToEmptyDatabase() throws SQLException {
        try (PostgresConnection conn = PostgresConnection.forTestDatabase("emptydb");) {
            conn.connect();
        }
    }
}
