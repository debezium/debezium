/*
 * Copyright 2015 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ingest.postgresql;

import java.sql.SQLException;

import org.junit.Test;

import io.debezium.ingest.postgres.PostgresConnection;
import io.debezium.jdbc.TestDatabase;

public class ConnectionIT {
    
    @Test
    public void shouldConnectToDefaulDatabase() throws SQLException {
        try (PostgresConnection conn = new PostgresConnection( TestDatabase.testConfig("postgres") );) {
            conn.connect();
        }
    }
    
    @Test
    public void shouldConnectToEmptyDatabase() throws SQLException {
        try (PostgresConnection conn = new PostgresConnection( TestDatabase.testConfig("emptydb") );) {
            conn.connect();
        }
    }
}
