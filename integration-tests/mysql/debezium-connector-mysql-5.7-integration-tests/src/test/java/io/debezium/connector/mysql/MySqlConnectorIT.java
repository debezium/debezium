/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import org.junit.Test;

/**
 * Run the {@link MySqlConnector} in various configurations and against different MySQL server instances
 * and verify the output is as expected.
 * 
 * @author Randall Hauch
 */
public class MySqlConnectorIT extends AbstractMySqlConnectorOutputTest {

    /**
     * Perform a snapshot of the entire MySQL server (without GTIDs) and ignore built-in tables.
     * This checks the approximately 160 change events against expected values.
     */
    @Test
    public void shouldCaptureChangesFromMySqlUsingSnapshot() {
        runConnector(usingSpec("mysql", "src/test/expected/snapshot/no-filter").withVariables(this::readSystemVariables));
    }

    /**
     * Perform a snapshot of the some of the MySQL server (without GTIDs) and ignore built-in tables.
     */
    @Test
    public void shouldCaptureChangesFromMySqlUsingSnapshotIncludingSpecificDatabases() {
        runConnector(usingSpec("mysql-with-dbs", "src/test/expected/snapshot/filter-db").withVariables(this::readSystemVariables));
    }

    /**
     * Perform a snapshot of the some of the MySQL server (without GTIDs) and ignore built-in tables.
     */
    @Test
    public void shouldCaptureChangesFromMySqlUsingSnapshotIncludingSpecificTables() {
        runConnector(usingSpec("mysql-with-tables", "src/test/expected/snapshot/filter-table").withVariables(this::readSystemVariables));
    }

    /**
     * Read binlog of the entire MySQL server (without GTIDs) and ignore built-in tables.
     */
    @Test
    public void shouldCaptureChangesFromMySqlUsingNoSnapshot() {
        runConnector(usingSpec("mysql-nosnap", "src/test/expected/no-snapshot/no-filter").withVariables(this::readSystemVariables));
    }

    /**
     * Read binlog of some of the MySQL server (without GTIDs) and ignore built-in tables.
     */
    @Test
    public void shouldCaptureChangesFromMySqlUsingNoSnapshotIncludingSpecificDatabases() {
        runConnector(usingSpec("mysql-nosnap-with-dbs",
                               "src/test/expected/no-snapshot/filter-db").withVariables(this::readSystemVariables));
    }

    /**
     * Read binlog of some of the MySQL server (without GTIDs) and ignore built-in tables.
     */
    @Test
    public void shouldCaptureChangesFromMySqlUsingNoSnapshotIncludingSpecificTables() {
        runConnector(usingSpec("mysql-nosnap-with-tables",
                               "src/test/expected/no-snapshot/filter-table").withVariables(this::readSystemVariables));
    }
}
