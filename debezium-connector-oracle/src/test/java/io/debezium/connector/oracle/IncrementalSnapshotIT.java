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
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.history.SchemaHistory;
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

        dropTables();
        createTables();

        // todo: creates signal table in the PDB, do we want it to be in the CDB?
        TestHelper.dropTable(connection, "debezium_signal");
        connection.execute("CREATE TABLE debezium_signal (id varchar2(64), type varchar2(32), data varchar2(2048))");
        connection.execute("GRANT INSERT on debezium_signal to " + TestHelper.getConnectorUserName());
        connection.execute("GRANT DELETE on debezium_signal to " + TestHelper.getConnectorUserName());
        TestHelper.streamTable(connection, "debezium_signal");

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws Exception {
        stopConnector();
        if (connection != null) {
            dropTables();
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
        // TestHelper.streamTable(connection, "a");
    }

    @Override
    protected void populateTables() throws SQLException {
        super.populateTables();
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
    protected String noPKTopicName() {
        return "server1.DEBEZIUM.A42";
    }

    @Override
    protected String noPKTableName() {
        return "DEBEZIUM.A42";
    }

    @Override
    protected String returnedIdentifierName(String queriedID) {
        return queriedID.toUpperCase();
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
    protected String noPKTableDataCollectionId() {
        return TestHelper.getDatabaseName() + ".DEBEZIUM.A42";
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
                .with(OracleConnectorConfig.SNAPSHOT_MODE, OracleConnectorConfig.SnapshotMode.NO_DATA)
                .with(OracleConnectorConfig.SIGNAL_DATA_COLLECTION, TestHelper.getDatabaseName() + ".DEBEZIUM.DEBEZIUM_SIGNAL")
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.A,DEBEZIUM\\.B,DEBEZIUM\\.A42")
                .with(RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS, "DEBEZIUM.A42:pk1,pk2,pk3,pk4")
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
                .with(RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS, "DEBEZIUM.A42:pk1,pk2,pk3,pk4")
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, storeOnlyCapturedDdl);
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

    @Override
    protected String connector() {
        return "oracle";
    }

    @Override
    protected String server() {
        return TestHelper.SERVER_NAME;
    }

    @Test
    public void snapshotPreceededBySchemaChange() throws Exception {
        // TODO: remove once we upgrade Apicurio version (DBZ-7357)
        if (VerifyRecord.isApucurioAvailable()) {
            skipAvroValidation();
        }
        super.snapshotPreceededBySchemaChange();
    }

    private void createTables() throws Exception {
        connection.execute("CREATE TABLE a (pk numeric(9,0) primary key, aa numeric(9,0))");
        connection.execute("CREATE TABLE b (pk numeric(9,0) primary key, aa numeric(9,0))");
        connection.execute("CREATE TABLE a42 (pk1 numeric(9,0), pk2 numeric(9,0), pk3 numeric(9,0), pk4 numeric(9,0), aa numeric(9,0))");
        TestHelper.streamTable(connection, "a");
        TestHelper.streamTable(connection, "b");
        TestHelper.streamTable(connection, "a42");
    }

    private void dropTables() throws Exception {
        TestHelper.dropTable(connection, "a");
        TestHelper.dropTable(connection, "b");
        TestHelper.dropTable(connection, "a42");
    }
}
