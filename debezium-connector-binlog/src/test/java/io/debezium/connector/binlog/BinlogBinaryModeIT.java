/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig.SnapshotMode;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;

/**
 * @author Robert B. Hanviriyapunt
 */
public abstract class BinlogBinaryModeIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-binary-mode.txt")
            .toAbsolutePath();
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("binarymodeit", "binary_mode_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private Configuration config;

    @BeforeEach
    void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
    }

    @AfterEach
    void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("DBZ-1814")
    public void shouldReceiveRawBinaryStreaming() throws InterruptedException, SQLException {
        consume(false, BinaryHandlingMode.BYTES, 5, ByteBuffer.wrap(new byte[]{ 1, 2, 3 }));
    }

    @Test
    @FixFor("DBZ-8076")
    public void shouldReceiveRawBinarySnapshot() throws InterruptedException, SQLException {
        // SET CHARSET, DROP TABLE, DROP DATABASE, CREATE DATABASE, USE DATABASE
        consume(true, BinaryHandlingMode.BYTES, 5, ByteBuffer.wrap(new byte[]{ 1, 2, 3 }));
    }

    @Test
    @FixFor("DBZ-1814")
    public void shouldReceiveHexBinaryStreaming() throws InterruptedException, SQLException {
        consume(false, BinaryHandlingMode.HEX, 5, "010203");
    }

    @Test
    @FixFor("DBZ-8076")
    public void shouldReceiveHexBinarySnapshot() throws InterruptedException, SQLException {
        // SET CHARSET, DROP TABLE, DROP DATABASE, CREATE DATABASE, USE DATABASE
        consume(true, BinaryHandlingMode.HEX, 5, "010203");
    }

    @Test
    @FixFor("DBZ-1814")
    public void shouldReceiveBase64BinaryStream() throws InterruptedException, SQLException {
        consume(false, BinaryHandlingMode.BASE64, 5, "AQID");
    }

    @Test
    @FixFor("DBZ-8076")
    public void shouldReceiveBase64BinarySnapshot() throws InterruptedException, SQLException {
        consume(true, BinaryHandlingMode.BASE64, 5, "AQID");
    }

    @Test
    @FixFor("DBZ-5544")
    public void shouldReceiveBase64UrlSafeBinaryStream() throws InterruptedException, SQLException {
        consume(false, BinaryHandlingMode.BASE64_URL_SAFE, 5, "AQID");
    }

    @Test
    @FixFor("DBZ-8076")
    public void shouldReceiveBase64UrlSafeBinarySnapshot() throws InterruptedException, SQLException {
        consume(true, BinaryHandlingMode.BASE64_URL_SAFE, 5, "AQID");
    }

    private void consume(boolean snapshot, BinaryHandlingMode binaryHandlingMode, int metadataEventCount, Object expectedValue)
            throws InterruptedException, SQLException {
        // Use the DB configuration to define the connector's configuration ...
        if (snapshot) {
            config = DATABASE.defaultConfig()
                    .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                    .with(BinlogConnectorConfig.BINARY_HANDLING_MODE, binaryHandlingMode)
                    .build();

            insertRow();
            start(getConnectorClass(), config);

        }
        else {
            config = DATABASE.defaultConfig()
                    .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                    .with(BinlogConnectorConfig.BINARY_HANDLING_MODE, binaryHandlingMode)
                    .build();

            start(getConnectorClass(), config);
            waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), getStreamingNamespace());
            insertRow();
        }

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        SourceRecords sourceRecords = consumeRecordsByTopic(2 + metadataEventCount);
        stopConnector();
        assertThat(sourceRecords).isNotNull();

        List<SourceRecord> topicSourceRecords = sourceRecords.recordsForTopic(DATABASE.topicForTable("dbz_1814_binary_mode_test"));
        assertThat(topicSourceRecords).hasSize(1);

        SourceRecord topicSourceRecord = topicSourceRecords.get(0);
        Struct kafkaDataStructure = (Struct) ((Struct) topicSourceRecord.value()).get("after");
        assertEquals(expectedValue, kafkaDataStructure.get("blob_col"));
        assertEquals(expectedValue, kafkaDataStructure.get("tinyblob_col"));
        assertEquals(expectedValue, kafkaDataStructure.get("mediumblob_col"));
        assertEquals(expectedValue, kafkaDataStructure.get("longblob_col"));
        assertEquals(expectedValue, kafkaDataStructure.get("binary_col"));
        assertEquals(expectedValue, kafkaDataStructure.get("varbinary_col"));

        // Check that all records are valid, can be serialized and deserialized ...
        sourceRecords.forEach(this::validate);
    }

    private void insertRow() throws SQLException {
        try (BinlogTestConnection connection = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            connection.execute("INSERT INTO dbz_1814_binary_mode_test (\n" +
                    "    id,\n" +
                    "    blob_col,\n" +
                    "    tinyblob_col,\n" +
                    "    mediumblob_col,\n" +
                    "    longblob_col,\n" +
                    "    binary_col,\n" +
                    "    varbinary_col )\n" +
                    "VALUES (\n" +
                    "    default,\n" +
                    "    X'010203',\n" +
                    "    X'010203',\n" +
                    "    X'010203',\n" +
                    "    X'010203',\n" +
                    "    X'010203',\n" +
                    "    X'010203' );");
        }
    }
}
