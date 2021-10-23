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
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotWithSchemaChangesSupportTest;
import io.debezium.relational.TableId;
import io.debezium.util.Testing;

public class IncrementalSnapshotIT extends AbstractIncrementalSnapshotWithSchemaChangesSupportTest<MySqlConnector> {

    protected static final String SERVER_NAME = "is_test";
    protected final UniqueDatabase DATABASE = new UniqueDatabase(SERVER_NAME, "incremental_snapshot-test").withDbHistoryPath(DB_HISTORY_PATH);

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
                .with(MySqlConnectorConfig.INCREMENTAL_SNAPSHOT_ALLOW_SCHEMA_CHANGES, true)
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
        return TableId.parse(DATABASE.qualifiedTableName("a")).toQuotedString('`');
    }

    @Override
    protected String signalTableName() {
        return TableId.parse(DATABASE.qualifiedTableName("debezium_signal")).toQuotedString('`');
    }

    @Override
    protected String tableName(String table) {
        return TableId.parse(DATABASE.qualifiedTableName(table)).toQuotedString('`');
    }

    @Override
    protected String alterColumnStatement(String table, String column, String type) {
        return String.format("ALTER TABLE %s MODIFY COLUMN %s %s", table, column, type);
    }

    @Override
    protected String alterColumnSetNotNullStatement(String table, String column, String type) {
        return String.format("ALTER TABLE %s MODIFY COLUMN %s %s NOT NULL", table, column, type);
    }

    @Override
    protected String alterColumnDropNotNullStatement(String table, String column, String type) {
        return String.format("ALTER TABLE %s MODIFY COLUMN %s %s NULL", table, column, type);
    }

    @Override
    protected String alterColumnSetDefaultStatement(String table, String column, String type, String defaultValue) {
        return String.format("ALTER TABLE %s MODIFY COLUMN %s %s DEFAULT %s", table, column, type, defaultValue);
    }

    @Override
    protected String alterColumnDropDefaultStatement(String table, String column, String type) {
        return String.format("ALTER TABLE %s MODIFY COLUMN %s %s", table, column, type);
    }

    @Override
    protected void executeRenameTable(JdbcConnection connection, String newTable) throws SQLException {
        connection.setAutoCommit(false);
        String query = String.format("RENAME TABLE %s to %s, %s to %s", tableName(), "old_table", newTable, tableName());
        logger.info(query);
        connection.executeWithoutCommitting(query);
        connection.commit();
    }

    @Override
    protected String createTableStatement(String newTable, String copyTable) {
        return String.format("CREATE TABLE %s LIKE %s", newTable, copyTable);
    }
}
