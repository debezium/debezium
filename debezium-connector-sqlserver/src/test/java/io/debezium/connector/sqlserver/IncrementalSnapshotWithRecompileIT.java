/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import io.debezium.config.Configuration.Builder;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.ConditionalFail;
import io.debezium.junit.Flaky;
import io.debezium.junit.SkipTestRule;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotTest;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.util.Testing;

public class IncrementalSnapshotWithRecompileIT extends AbstractIncrementalSnapshotTest<SqlServerConnector> {

    private SqlServerConnection connection;

    @Rule
    public SkipTestRule skipRule = new SkipTestRule();
    @Rule
    public ConditionalFail conditionalFail = new ConditionalFail();

    @Before
    public void before() throws SQLException {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnectionWithOptionRecompile();
        connection.execute(
                "CREATE TABLE a (pk int primary key, aa int)",
                "CREATE TABLE b (pk int primary key, aa int)",
                "CREATE TABLE a42 (pk1 integer, pk2 integer, pk3 integer, pk4 integer, aa integer);",
                "CREATE TABLE debezium_signal (id varchar(64), type varchar(32), data varchar(2048))");
        TestHelper.enableTableCdc(connection, "debezium_signal");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    protected void populateTable() throws SQLException {
        super.populateTable();
        TestHelper.enableTableCdc(connection, "a");
    }

    @Override
    protected void populateTables() throws SQLException {
        super.populateTables();
        TestHelper.enableTableCdc(connection, "a");
        TestHelper.enableTableCdc(connection, "b");
    }

    @Override
    protected Class<SqlServerConnector> connectorClass() {
        return SqlServerConnector.class;
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return connection;
    }

    @Override
    protected String topicName() {
        return "server1.testDB1.dbo.a";
    }

    @Override
    public List<String> topicNames() {
        return List.of("server1.testDB1.dbo.a", "server1.testDB1.dbo.b");
    }

    @Override
    protected String tableName() {
        return "testDB1.dbo.a";
    }

    @Override
    protected String noPKTopicName() {
        return "server1.testDB1.dbo.a42";
    }

    @Override
    protected String noPKTableName() {
        return "testDB1.dbo.a42";
    }

    @Override
    protected List<String> tableNames() {
        return List.of("testDB1.dbo.a", "testDB1.dbo.b");
    }

    @Override
    protected String signalTableName() {
        return "dbo.debezium_signal";
    }

    @Override
    protected Builder config() {
        return TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(SqlServerConnectorConfig.SIGNAL_DATA_COLLECTION, "testDB1.dbo.debezium_signal")
                .with(SqlServerConnectorConfig.INCREMENTAL_SNAPSHOT_OPTION_RECOMPILE, true)
                .with(RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS, "dbo.a42:pk1,pk2,pk3,pk4");
    }

    @Override
    protected Builder mutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl) {
        final String tableIncludeList;
        if (signalTableOnly) {
            tableIncludeList = "dbo.b";
        }
        else {
            tableIncludeList = "dbo.a,dbo.b";
        }
        return TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.SIGNAL_DATA_COLLECTION, "testDB1.dbo.debezium_signal")
                .with(SqlServerConnectorConfig.INCREMENTAL_SNAPSHOT_OPTION_RECOMPILE, true)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, tableIncludeList)
                .with(RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS, "dbo.a42:pk1,pk2,pk3,pk4")
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, storeOnlyCapturedDdl);
    }

    @Override
    protected String connector() {
        return "sql_server";
    }

    @Override
    protected String server() {
        return TestHelper.TEST_SERVER_NAME;
    }

    @Override
    protected String task() {
        return "0";
    }

    @Override
    protected String database() {
        return TestHelper.TEST_DATABASE_1;
    }

    @Override
    protected void waitForCdcTransactionPropagation(int expectedTransactions) throws Exception {
        TestHelper.waitForCdcTransactionPropagation(connection, TestHelper.TEST_DATABASE_1, expectedTransactions);
    }

    @Override
    protected int defaultIncrementalSnapshotChunkSize() {
        return 250;
    }

    @Override
    @Flaky("DBZ-5393")
    public void stopCurrentIncrementalSnapshotWithAllCollectionsAndTakeNewNewIncrementalSnapshotAfterRestart() throws Exception {
        super.stopCurrentIncrementalSnapshotWithAllCollectionsAndTakeNewNewIncrementalSnapshotAfterRestart();
    }
}
