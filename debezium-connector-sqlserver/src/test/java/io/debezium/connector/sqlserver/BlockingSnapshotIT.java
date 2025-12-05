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

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.AbstractBlockingSnapshotTest;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.util.Testing;

public class BlockingSnapshotIT extends AbstractBlockingSnapshotTest {

    private static final int POLLING_INTERVAL = 1;

    private SqlServerConnection connection;

    @Before
    public void before() throws SQLException {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        connection.execute(
                "CREATE TABLE a (pk int primary key, aa int)",
                "CREATE TABLE b (pk int primary key, aa int)",
                "CREATE TABLE debezium_signal (id varchar(64), type varchar(32), data varchar(2048))");
        TestHelper.enableTableCdc(connection, "debezium_signal");
        TestHelper.adjustCdcPollingInterval(connection, POLLING_INTERVAL);

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
    protected List<String> topicNames() {
        return List.of("server1.testDB1.dbo.a", "server1.testDB1.dbo.b");
    }

    @Override
    protected String tableName() {
        return "testDB1.dbo.a";
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
    protected String escapedTableDataCollectionId() {
        return "\\\"testDB1\\\".\\\"dbo\\\".\\\"a\\\"";
    }

    @Override
    protected Configuration.Builder config() {
        return TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SqlServerConnectorConfig.SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.SIGNAL_DATA_COLLECTION, "testDB1.dbo.debezium_signal");
    }

    @Override
    protected Configuration.Builder mutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl) {

        return TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SqlServerConnectorConfig.SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.SIGNAL_DATA_COLLECTION, "testDB1.dbo.debezium_signal")
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE_TABLES, tableName())
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, storeOnlyCapturedDdl);
    }

    @Override
    protected void waitForCdcTransactionPropagation(int expectedTransactions) throws Exception {
        TestHelper.waitForCdcTransactionPropagation(connection, TestHelper.TEST_DATABASE_1, expectedTransactions);
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
    protected int insertMaxSleep() {
        return 100;
    }
}
