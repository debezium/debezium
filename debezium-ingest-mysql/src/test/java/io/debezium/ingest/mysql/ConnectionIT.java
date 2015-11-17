/*
 * Copyright 2015 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ingest.mysql;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Test;

import io.debezium.injest.jdbc.util.TestDatabase;

public class ConnectionIT {
    
    @Test
    public void shouldConnectToDefaulDatabase() throws SQLException {
        try (Connection conn = TestDatabase.connect("jdbc:mysql://${hostname}:${port}/mysql");) {
        }
    }
    
    @Test
    public void shouldConnectToEmptyDatabase() throws SQLException {
        try (Connection conn = TestDatabase.connect("jdbc:mysql://${hostname}:${port}/emptydb");) {
        }
    }
}
