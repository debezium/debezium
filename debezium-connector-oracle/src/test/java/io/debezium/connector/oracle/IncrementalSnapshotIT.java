/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipTestRule;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotTest;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.util.Testing;

/**
 * Incremental Snapshots tests for the Oracle connector.
 *
 * @author Chris Cranford
 */
public class IncrementalSnapshotIT extends AbstractIncrementalSnapshotTest<OracleConnector> {

    private OracleConnection connection;

    @Rule
    public SkipTestRule skipRule = new SkipTestRule();

    @Before
    public void before() throws Exception {
        connection = TestHelper.testConnection();

        TestHelper.dropTable(connection, "a");
        connection.execute("CREATE TABLE a (pk numeric(9,0) primary key, aa numeric(9,0))");

        // todo: creates signal table in the PDB, do we want it to be in the CDB?
        TestHelper.dropTable(connection, "debezium_signal");
        connection.execute("CREATE TABLE debezium_signal (id varchar2(64), type varchar2(32), data varchar2(2048))");
        connection.execute("GRANT INSERT on debezium_signal to " + TestHelper.getConnectorUserName());
        TestHelper.streamTable(connection, "debezium_signal");

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
    }

    @After
    public void after() throws Exception {
        stopConnector();
        if (connection != null) {
            TestHelper.dropTable(connection, "a");
            TestHelper.dropTable(connection, "debezium_signal");
            connection.close();
        }
    }

    @Override
    protected void waitForConnectorToStart() {
        super.waitForConnectorToStart();
        try {
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void populateTable() throws SQLException {
        super.populateTable();
        TestHelper.streamTable(connection, "a");
    }

    @Override
    protected Class<OracleConnector> connectorClass() {
        return OracleConnector.class;
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return connection;
    }

    @Override
    protected String topicName() {
        return "server1.DEBEZIUM.A";
    }

    @Override
    protected String tableName() {
        return "DEBEZIUM.A";
    }

    @Override
    protected String tableDataCollectionId() {
        return "ORCLPDB1.DEBEZIUM.A";
    }

    @Override
    protected String signalTableName() {
        return "DEBEZIUM.DEBEZIUM_SIGNAL";
    }

    @Override
    protected Configuration.Builder config() {
        return TestHelper.defaultConfig()
                .with(OracleConnectorConfig.SNAPSHOT_MODE, OracleConnectorConfig.SnapshotMode.SCHEMA_ONLY)
                .with(OracleConnectorConfig.SIGNAL_DATA_COLLECTION, "ORCLPDB1.DEBEZIUM.DEBEZIUM_SIGNAL")
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.A,DEBEZIUM\\.DEBEZIUM_SIGNAL")
                .with(DatabaseHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true);
    }

    @Override
    protected String valueFieldName() {
        return "AA";
    }

    @Override
    protected String pkFieldName() {
        return "PK";
    }

    @Override
    protected String alterTableAddColumnStatement(String tableName) {
        return "ALTER TABLE " + tableName + " ADD col3 INTEGER DEFAULT 0";
    }
}
