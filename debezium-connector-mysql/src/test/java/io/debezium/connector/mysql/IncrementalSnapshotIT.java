/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql;

import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotMode;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotTest;
import io.debezium.util.Testing;

public class IncrementalSnapshotIT extends AbstractIncrementalSnapshotTest<MySqlConnector> {

    private static final String SERVER_NAME = "is_test";
    private final UniqueDatabase DATABASE = new UniqueDatabase(SERVER_NAME, "incremental_snapshot_test").withDbHistoryPath(DB_HISTORY_PATH);

    @Before
    public void before() throws SQLException {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(DB_HISTORY_PATH);
    }

    @After
    public void after() {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    protected Configuration.Builder config() {
        return DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.USER, "mysqluser")
                .with(MySqlConnectorConfig.PASSWORD, "mysqlpw")
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY.getValue())
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(MySqlConnectorConfig.SIGNAL_DATA_COLLECTION, DATABASE.qualifiedTableName("debezium_signal"))
                .with(MySqlConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 10)
                .with(MySqlConnector.IMPLEMENTATION_PROP, "new");
    }

    @Override
    protected Class<MySqlConnector> connectorClass() {
        return MySqlConnector.class;
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());
    }

    @Override
    protected String topicName() {
        return DATABASE.topicForTable("a");
    }

    @Override
    protected String tableName() {
        return DATABASE.qualifiedTableName("a");
    }

    @Override
    protected String signalTableName() {
        return DATABASE.qualifiedTableName("debezium_signal");
    }
}
