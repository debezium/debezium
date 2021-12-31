/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.sql.SQLException;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
                .with(MySqlConnectorConfig.INCLUDE_SQL_QUERY, true)
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

    @Test
    public void updates() throws Exception {
        // Testing.Print.enable();

        populateTable();
        startConnector();

        sendAdHocSnapshotSignal();

        final int batchSize = 10;
        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(false);
            connection.execute("SET binlog_rows_query_log_events=ON");
            for (int i = 0; i < ROW_COUNT; i++) {
                connection.executeWithoutCommitting(
                        String.format("UPDATE %s SET aa = aa + 2000 WHERE pk > %s AND pk <= %s", tableName(),
                                i * batchSize, (i + 1) * batchSize));
                connection.commit();
            }
        }

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, SourceRecord> dbChanges = consumeRecordsMixedWithIncrementalSnapshot(expectedRecordCount,
                x -> ((Struct) x.getValue().value()).getStruct("after").getInt32(valueFieldName()) >= 2000, null);
        for (int i = 0; i < expectedRecordCount; i++) {
            SourceRecord record = dbChanges.get(i + 1);
            final int value = ((Struct) record.value()).getStruct("after").getInt32(valueFieldName());
            assertEquals(i + 2000, value);
            Object query = ((Struct) record.value()).getStruct("source").get("query");
            String snapshot = ((Struct) record.value()).getStruct("source").get("snapshot").toString();
            if (snapshot.equals("false")) {
                assertNotNull(query);
            }
            else {
                assertNull(query);
                assertEquals("incremental", snapshot);
            }
        }
    }
}
