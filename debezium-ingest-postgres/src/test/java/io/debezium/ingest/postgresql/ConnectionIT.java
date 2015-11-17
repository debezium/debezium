/*
 * Copyright 2015 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ingest.postgresql;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Test;

import io.debezium.injest.jdbc.util.TestDatabase;

public class ConnectionIT {
    
    @Test
    public void shouldConnectToDefaultDatabase() throws SQLException {
        try (Connection conn = TestDatabase.connect("jdbc:postgresql://${hostname}:${port}/postgres");) {
        }
    }
    
    @Test
    public void shouldConnectToEmptyDatabase() throws SQLException {
        try (Connection conn = TestDatabase.connect("jdbc:postgresql://${hostname}:${port}/emptydb");) {
        }
    }
}
