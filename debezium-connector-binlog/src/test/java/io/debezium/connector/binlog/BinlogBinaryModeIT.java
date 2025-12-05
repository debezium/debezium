/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig.SnapshotMode;
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

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("DBZ-1814")
    public void shouldReceiveRawBinaryStreaming() throws InterruptedException {
        consume(SnapshotMode.NEVER, BinaryHandlingMode.BYTES, 1, ByteBuffer.wrap(new byte[]{ 1, 2, 3 }));
    }

    @Test
    @FixFor("DBZ-8076")
    public void shouldReceiveRawBinarySnapshot() throws InterruptedException {
        // SET CHARSET, DROP TABLE, DROP DATABASE, CREATE DATABASE, USE DATABASE
        consume(SnapshotMode.INITIAL, BinaryHandlingMode.BYTES, 5, ByteBuffer.wrap(new byte[]{ 1, 2, 3 }));
    }

    @Test
    @FixFor("DBZ-1814")
    public void shouldReceiveHexBinaryStreaming() throws InterruptedException {
        consume(SnapshotMode.NEVER, BinaryHandlingMode.HEX, 1, "010203");
    }

    @Test
    @FixFor("DBZ-8076")
    public void shouldReceiveHexBinarySnapshot() throws InterruptedException {
        // SET CHARSET, DROP TABLE, DROP DATABASE, CREATE DATABASE, USE DATABASE
        consume(SnapshotMode.INITIAL, BinaryHandlingMode.HEX, 5, "010203");
    }

    @Test
    @FixFor("DBZ-1814")
    public void shouldReceiveBase64BinaryStream() throws InterruptedException {
        consume(SnapshotMode.NEVER, BinaryHandlingMode.BASE64, 1, "AQID");
    }

    @Test
    @FixFor("DBZ-8076")
    public void shouldReceiveBase64BinarySnapshot() throws InterruptedException {
        consume(SnapshotMode.INITIAL, BinaryHandlingMode.BASE64, 5, "AQID");
    }

    @Test
    @FixFor("DBZ-5544")
    public void shouldReceiveBase64UrlSafeBinaryStream() throws InterruptedException {
        consume(SnapshotMode.NEVER, BinaryHandlingMode.BASE64_URL_SAFE, 1, "AQID");
    }

    @Test
    @FixFor("DBZ-8076")
    public void shouldReceiveBase64UrlSafeBinarySnapshot() throws InterruptedException {
        consume(SnapshotMode.INITIAL, BinaryHandlingMode.BASE64_URL_SAFE, 5, "AQID");
    }

    private void consume(SnapshotMode snapshotMode, BinaryHandlingMode binaryHandlingMode, int metadataEventCount, Object expectedValue) throws InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, snapshotMode)
                .with(BinlogConnectorConfig.BINARY_HANDLING_MODE, binaryHandlingMode)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        int createTableCount = 1;
        int insertCount = 1;
        SourceRecords sourceRecords = consumeRecordsByTopic(metadataEventCount + createTableCount + insertCount);
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
}
