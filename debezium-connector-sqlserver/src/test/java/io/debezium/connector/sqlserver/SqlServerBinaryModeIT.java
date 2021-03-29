/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

public class SqlServerBinaryModeIT extends AbstractConnectorTest {

    private SqlServerConnection connection;

    @Before
    public void before() throws SQLException {
        TestHelper.createMultipleTestDatabases();
        connection = TestHelper.testConnection();
        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            connection.execute(
                    "CREATE TABLE binary_mode_test (id INT IDENTITY (1, 1) PRIMARY KEY, binary_col BINARY(3) NOT NULL, varbinary_col VARBINARY(3) NOT NULL)",
                    "INSERT INTO binary_mode_test (binary_col, varbinary_col) VALUES (0x010203, 0x010203)");
            TestHelper.enableTableCdc(connection, databaseName, "binary_mode_test");
        });

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
    }

    @After
    public void after() throws SQLException {
        stopConnector();

        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void shouldReceiveRawBinary() throws InterruptedException {
        runTest(BinaryHandlingMode.BYTES, ByteBuffer.wrap(new byte[]{ 1, 2, 3 }));
    }

    @Test
    public void shouldReceiveHexBinary() throws InterruptedException {
        runTest(BinaryHandlingMode.HEX, "010203");
    }

    @Test
    public void shouldReceiveBase64Binary() throws InterruptedException {
        runTest(BinaryHandlingMode.BASE64, "AQID");
    }

    private void runTest(BinaryHandlingMode mode, Object expectedValue) throws InterruptedException {
        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SqlServerConnectorConfig.SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo\\.binary_mode_test")
                .with(SqlServerConnectorConfig.BINARY_HANDLING_MODE, mode)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        SourceRecords records = consumeRecordsByTopic(TestHelper.TEST_DATABASES.size());
        TestHelper.forEachDatabase(databaseName -> {
            Struct data = getModified(records, databaseName);
            assertEquals(expectedValue, data.get("binary_col"));
            assertEquals(expectedValue, data.get("varbinary_col"));
        });
    }

    private Struct getModified(SourceRecords records, String databaseName) {
        final List<SourceRecord> results = records.recordsForTopic(TestHelper.topicName(databaseName, "binary_mode_test"));
        Assertions.assertThat(results).hasSize(1);

        return (Struct) ((Struct) results.get(0).value()).get("after");
    }
}
