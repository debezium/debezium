/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import io.debezium.config.Configuration.Builder;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipTestRule;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotTest;
import io.debezium.util.Testing;

public class IncrementalSnapshotWithRecompileIT extends AbstractIncrementalSnapshotTest<SqlServerConnector> {

    private SqlServerConnection connection;

    @Rule
    public SkipTestRule skipRule = new SkipTestRule();

    @Before
    public void before() throws SQLException {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnectionWithOptionRecompile();
        connection.execute(
                "CREATE TABLE a (pk int primary key, aa int)",
                "CREATE TABLE debezium_signal (id varchar(64), type varchar(32), data varchar(2048))");
        TestHelper.enableTableCdc(connection, "debezium_signal");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
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
        return "server1.dbo.a";
    }

    @Override
    protected String tableName() {
        return "testDB.dbo.a";
    }

    @Override
    protected String signalTableName() {
        return "dbo.debezium_signal";
    }

    @Override
    protected Builder config() {
        return TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(SqlServerConnectorConfig.SIGNAL_DATA_COLLECTION, "testDB.dbo.debezium_signal")
                .with(SqlServerConnectorConfig.INCREMENTAL_SNAPSHOT_OPTION_RECOMPILE, true);
    }
}
