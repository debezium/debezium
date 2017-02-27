/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.util.Testing;

/**
 * Run the {@link MySqlConnector} in various configurations and against different MySQL server instances
 * and verify the output is as expected.
 * 
 * @author Randall Hauch
 */
public class MySqlConnectorReplicaIT extends AbstractMySqlConnectorOutputTest {

    @BeforeClass
    public static void beforeAll() throws InterruptedException {
        Testing.Print.enable();
        // We need to wait for the replica to catch up to the master
        Configuration system = Configuration.fromSystemProperties("");
        Configuration master = Configuration.create()
                                            .with("database.hostname", system.getString("database.hostname", "localhost"))
                                            .with("database.port", system.getInteger("database.port", 3306))
                                            .with("database.user", system.getString("database.user", "mysqluser"))
                                            .with("database.password", system.getString("database.password", "mysqlpw"))
                                            .build();
        Configuration replica = Configuration.create()
                                             .with("database.hostname", system.getString("database.hostname", "localhost"))
                                             .with("database.port", system.getInteger("database.replica.port", 4306))
                                             .with("database.user", system.getString("database.replica.user", "mysqlreplica"))
                                             .with("database.password", system.getString("database.replica.password", "mysqlpw"))
                                             .build();
        waitForGtidSetsToMatch(master, replica, 10, TimeUnit.SECONDS);
    }

    /**
     * Perform a snapshot of the entire MySQL server (with GTIDs) and ignore built-in tables.
     * This checks the approximately 160 change events against expected values.
     */
    @Test
    public void shouldCaptureChangesFromMySqlWithGtidsUsingSnapshot() {
        runConnector(usingSpec("mysql-replica-gtids",
                               "src/test/expected/replica/snapshot/no-filter").withVariables(this::readSystemVariables));
    }

    /**
     * Perform a snapshot of the some of the MySQL server (with GTIDs) and ignore built-in tables.
     */
    @Test
    public void shouldCaptureChangesFromMySqlWithGtidsUsingSnapshotIncludingSpecificDatabases() {
        runConnector(usingSpec("mysql-replica-gtids-with-dbs",
                               "src/test/expected/replica/snapshot/filter-db").withVariables(this::readSystemVariables));
    }

    /**
     * Perform a snapshot of the some of the MySQL server (with GTIDs) and ignore built-in tables.
     */
    @Test
    public void shouldCaptureChangesFromMySqlWithGtidsUsingSnapshotIncludingSpecificTables() {
        runConnector(usingSpec("mysql-replica-gtids-with-tables",
                               "src/test/expected/replica/snapshot/filter-table").withVariables(this::readSystemVariables));
    }

    /**
     * Read binlog of the entire MySQL server (with GTIDs) and ignore built-in tables.
     */
    @Test
    public void shouldCaptureChangesFromMySqlWithGtidsUsingNoSnapshot() {
        runConnector(usingSpec("mysql-replica-gtids-nosnap",
                               "src/test/expected/replica/no-snapshot/no-filter").withVariables(this::readSystemVariables));
    }

    /**
     * Read binlog of some of the MySQL server (with GTIDs) and ignore built-in tables.
     */
    @Test
    public void shouldCaptureChangesFromMySqlWithGtidsUsingNoSnapshotIncludingSpecificDatabases() {
        runConnector(usingSpec("mysql-replica-gtids-nosnap-with-dbs",
                               "src/test/expected/replica/no-snapshot/filter-db").withVariables(this::readSystemVariables));
    }

    /**
     * Read binlog of some of the MySQL server (with GTIDs) and ignore built-in tables.
     */
    @Test
    public void shouldCaptureChangesFromMySqlWithGtidsUsingNoSnapshotIncludingSpecificTables() {
        runConnector(usingSpec("mysql-replica-gtids-nosnap-with-tables",
                               "src/test/expected/replica/no-snapshot/filter-table").withVariables(this::readSystemVariables));
    }
}
