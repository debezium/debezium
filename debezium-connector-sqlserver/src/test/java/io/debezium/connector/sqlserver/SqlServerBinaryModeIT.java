/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
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
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();

        connection.execute(
                "CREATE TABLE binary_mode_test (id INT IDENTITY (1, 1) PRIMARY KEY, binary_col BINARY(3) NOT NULL, varbinary_col VARBINARY(3) NOT NULL)",
                "INSERT INTO binary_mode_test (binary_col, varbinary_col) VALUES (0x010203, 0x010203)");
        TestHelper.enableTableCdc(connection, "binary_mode_test");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
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
        Struct data = consume(BinaryHandlingMode.BYTES);

        ByteBuffer expectedValue = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        assertEquals(expectedValue, data.get("binary_col"));
        assertEquals(expectedValue, data.get("varbinary_col"));
    }

    @Test
    public void shouldReceiveHexBinary() throws InterruptedException {
        Struct data = consume(BinaryHandlingMode.HEX);

        String expectedValue = "010203";
        assertEquals(expectedValue, data.get("binary_col"));
        assertEquals(expectedValue, data.get("varbinary_col"));
    }

    @Test
    public void shouldReceiveBase64Binary() throws InterruptedException {
        Struct data = consume(BinaryHandlingMode.BASE64);

        String expectedValue = "AQID";
        assertEquals(expectedValue, data.get("binary_col"));
        assertEquals(expectedValue, data.get("varbinary_col"));
    }

    @Test
    public void shouldReceiveBase64UrlSafeBinary() throws InterruptedException {
        Struct data = consume(BinaryHandlingMode.BASE64_URL_SAFE);

        String expectedValue = "AQID";
        assertEquals(expectedValue, data.get("binary_col"));
        assertEquals(expectedValue, data.get("varbinary_col"));
    }

    private Struct consume(BinaryHandlingMode binaryMode) throws InterruptedException {
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SqlServerConnectorConfig.SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo\\.binary_mode_test")
                .with(SqlServerConnectorConfig.BINARY_HANDLING_MODE, binaryMode)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        SourceRecords records = consumeRecordsByTopic(1);
        final List<SourceRecord> results = records.recordsForTopic("server1.testDB1.dbo.binary_mode_test");
        assertThat(results).hasSize(1);

        return (Struct) ((Struct) results.get(0).value()).get("after");
    }
}
