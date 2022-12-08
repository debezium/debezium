/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
 * @author Chris Cranford
 */
public class OracleBinaryModeIT extends AbstractConnectorTest {

    private OracleConnection connection;

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();

        TestHelper.dropTable(connection, "binary_mode_test");
        connection.execute("CREATE TABLE binary_mode_test (id numeric(9,0), blob_col blob not null, primary key(id))");
        connection.execute("INSERT INTO binary_mode_test (id, blob_col) values (1, HEXTORAW('010203'))");
        TestHelper.streamTable(connection, "binary_mode_test");

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws SQLException {
        stopConnector();
        if (connection != null && connection.isConnected()) {
            TestHelper.dropTable(connection, "binary_mode_test");
            connection.close();
        }
    }

    @Test
    public void shouldReceiveRawBinary() throws InterruptedException {
        Struct data = consume(BinaryHandlingMode.BYTES);

        ByteBuffer expectedValue = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        assertEquals(expectedValue, data.get("BLOB_COL"));
    }

    @Test
    public void shouldReceiveHexBinary() throws InterruptedException {
        Struct data = consume(BinaryHandlingMode.HEX);

        String expectedValue = "010203";
        assertEquals(expectedValue, data.get("BLOB_COL"));
    }

    @Test
    public void shouldReceiveBase64Binary() throws InterruptedException {
        Struct data = consume(BinaryHandlingMode.BASE64);

        String expectedValue = "AQID";
        assertEquals(expectedValue, data.get("BLOB_COL"));
    }

    @Test
    public void shouldReceiveBase64UrlSafeBinary() throws InterruptedException {
        Struct data = consume(BinaryHandlingMode.BASE64_URL_SAFE);

        String expectedValue = "AQID";
        assertEquals(expectedValue, data.get("BLOB_COL"));
    }

    private Struct consume(BinaryHandlingMode binaryMode) throws InterruptedException {
        final Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.SNAPSHOT_MODE, OracleConnectorConfig.SnapshotMode.INITIAL)
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.BINARY_MODE_TEST")
                .with(OracleConnectorConfig.BINARY_HANDLING_MODE, binaryMode)
                .with(OracleConnectorConfig.LOB_ENABLED, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        SourceRecords records = consumeRecordsByTopic(1);
        final List<SourceRecord> results = records.recordsForTopic("server1.DEBEZIUM.BINARY_MODE_TEST");
        assertThat(results).hasSize(1);

        return (Struct) ((Struct) results.get(0).value()).get("after");
    }
}
