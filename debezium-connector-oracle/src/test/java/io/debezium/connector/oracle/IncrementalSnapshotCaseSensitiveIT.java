/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.VerifyRecord;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipTestRule;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotTest;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.util.Testing;

/**
 * Incremental Snapshots tests for the Oracle connector.
 *
 * @author Chris Cranford
 */
public class IncrementalSnapshotCaseSensitiveIT extends AbstractIncrementalSnapshotTest<OracleConnector> {

    private OracleConnection connection;

    @Rule
    public SkipTestRule skipRule = new SkipTestRule();

    @Before
    public void before() throws Exception {
        connection = TestHelper.testConnection();

        TestHelper.dropTable(connection, "a");
        TestHelper.dropTable(connection, "b");
        connection.execute("CREATE TABLE a (\"Pk\" numeric(9,0) primary key, aa numeric(9,0))");
        connection.execute("CREATE TABLE b (\"Pk\" numeric(9,0) primary key, aa numeric(9,0))");

        // todo: creates signal table in the PDB, do we want it to be in the CDB?
        TestHelper.dropTable(connection, "debezium_signal");
        connection.execute("CREATE TABLE debezium_signal (id varchar2(64), type varchar2(32), data varchar2(2048))");
        connection.execute("GRANT INSERT on debezium_signal to " + TestHelper.getConnectorUserName());
        TestHelper.streamTable(connection, "debezium_signal");

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws Exception {
        stopConnector();
        if (connection != null) {
            TestHelper.dropTable(connection, "a");
            TestHelper.dropTable(connection, "b");
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
    protected void populateTables() throws SQLException {
        super.populateTables();
        TestHelper.streamTable(connection, "a");
        TestHelper.streamTable(connection, "b");
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
    protected List<String> topicNames() {
        return List.of("server1.DEBEZIUM.A", "server1.DEBEZIUM.B");
    }

    @Override
    protected String tableName() {
        return "DEBEZIUM.A";
    }

    @Override
    protected List<String> tableNames() {
        return List.of("DEBEZIUM.A", "DEBEZIUM.B");
    }

    @Override
    protected String tableDataCollectionId() {
        return TestHelper.getDatabaseName() + ".DEBEZIUM.A";
    }

    @Override
    protected List<String> tableDataCollectionIds() {
        return List.of(TestHelper.getDatabaseName() + ".DEBEZIUM.A", TestHelper.getDatabaseName() + ".DEBEZIUM.B");
    }

    @Override
    protected String signalTableName() {
        return "DEBEZIUM.DEBEZIUM_SIGNAL";
    }

    @Override
    protected Configuration.Builder config() {
        return TestHelper.defaultConfig()
                .with(OracleConnectorConfig.SNAPSHOT_MODE, OracleConnectorConfig.SnapshotMode.SCHEMA_ONLY)
                .with(OracleConnectorConfig.SIGNAL_DATA_COLLECTION, TestHelper.getDatabaseName() + ".DEBEZIUM.DEBEZIUM_SIGNAL")
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.A,DEBEZIUM\\.B")
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true);
    }

    @Override
    protected Configuration.Builder mutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl) {
        final String tableIncludeList;
        if (signalTableOnly) {
            tableIncludeList = "DEBEZIUM\\.B";
        }
        else {
            tableIncludeList = "DEBEZIUM\\.A,DEBEZIUM\\.B";
        }
        return TestHelper.defaultConfig()
                .with(OracleConnectorConfig.SNAPSHOT_MODE, OracleConnectorConfig.SnapshotMode.INITIAL)
                .with(OracleConnectorConfig.SIGNAL_DATA_COLLECTION, TestHelper.getDatabaseName() + ".DEBEZIUM.DEBEZIUM_SIGNAL")
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, tableIncludeList)
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, storeOnlyCapturedDdl);
    }

    @Override
    protected String valueFieldName() {
        return "AA";
    }

    @Override
    protected String pkFieldName() {
        return "Pk";
    }

    @Override
    protected String getSignalTypeFieldName() {
        return "TYPE";
    }

    @Override
    protected String alterTableAddColumnStatement(String tableName) {
        return "ALTER TABLE " + tableName + " ADD col3 INTEGER DEFAULT 0";
    }

    @Override
    protected int defaultIncrementalSnapshotChunkSize() {
        return 250;
    }

    @Test
    public void snapshotPreceededBySchemaChange() throws Exception {
        // TODO: remove once https://github.com/Apicurio/apicurio-registry/issues/2980 is fixed
        if (VerifyRecord.isApucurioAvailable()) {
            skipAvroValidation();
        }
        super.snapshotPreceededBySchemaChange();
    }
}
