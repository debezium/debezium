/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

/**
 * @author Robert B. Hanviriyapunt
 */
public class MySqlBinaryModeIT extends AbstractAsyncEngineConnectorTest {

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-binary-mode.txt")
            .toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("binarymodeit", "binary_mode_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("DBZ-1814")
    public void shouldReceiveRawBinary() throws SQLException, InterruptedException {

        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NEVER)
                .with(MySqlConnectorConfig.BINARY_HANDLING_MODE, BinaryHandlingMode.BYTES)
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        int createDatabaseCount = 1;
        int createTableCount = 1;
        int insertCount = 1;
        SourceRecords sourceRecords = consumeRecordsByTopic(createDatabaseCount + createTableCount + insertCount);
        stopConnector();
        assertThat(sourceRecords).isNotNull();

        List<SourceRecord> topicSourceRecords = sourceRecords.recordsForTopic(DATABASE.topicForTable("dbz_1814_binary_mode_test"));
        assertThat(topicSourceRecords).hasSize(1);

        SourceRecord topicSourceRecord = topicSourceRecords.get(0);
        Struct kafkaDataStructure = (Struct) ((Struct) topicSourceRecord.value()).get("after");
        ByteBuffer expectedValue = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        assertEquals(expectedValue, kafkaDataStructure.get("blob_col"));
        assertEquals(expectedValue, kafkaDataStructure.get("tinyblob_col"));
        assertEquals(expectedValue, kafkaDataStructure.get("mediumblob_col"));
        assertEquals(expectedValue, kafkaDataStructure.get("longblob_col"));
        assertEquals(expectedValue, kafkaDataStructure.get("binary_col"));
        assertEquals(expectedValue, kafkaDataStructure.get("varbinary_col"));

        // Check that all records are valid, can be serialized and deserialized ...
        sourceRecords.forEach(this::validate);
    }

    @Test
    @FixFor("DBZ-1814")
    public void shouldReceiveHexBinary() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NEVER)
                .with(MySqlConnectorConfig.BINARY_HANDLING_MODE, BinaryHandlingMode.HEX)
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        int createDatabaseCount = 1;
        int createTableCount = 1;
        int insertCount = 1;
        SourceRecords sourceRecords = consumeRecordsByTopic(createDatabaseCount + createTableCount + insertCount);
        stopConnector();
        assertThat(sourceRecords).isNotNull();

        List<SourceRecord> topicSourceRecords = sourceRecords.recordsForTopic(DATABASE.topicForTable("dbz_1814_binary_mode_test"));
        assertThat(topicSourceRecords).hasSize(1);

        SourceRecord topicSourceRecord = topicSourceRecords.get(0);
        Struct kafkaDataStructure = (Struct) ((Struct) topicSourceRecord.value()).get("after");
        String expectedValue = "010203";
        assertEquals(expectedValue, kafkaDataStructure.get("blob_col"));
        assertEquals(expectedValue, kafkaDataStructure.get("tinyblob_col"));
        assertEquals(expectedValue, kafkaDataStructure.get("mediumblob_col"));
        assertEquals(expectedValue, kafkaDataStructure.get("longblob_col"));
        assertEquals(expectedValue, kafkaDataStructure.get("binary_col"));
        assertEquals(expectedValue, kafkaDataStructure.get("varbinary_col"));

        // Check that all records are valid, can be serialized and deserialized ...
        sourceRecords.forEach(this::validate);
    }

    @Test
    @FixFor("DBZ-1814")
    public void shouldReceiveBase64Binary() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NEVER)
                .with(MySqlConnectorConfig.BINARY_HANDLING_MODE, BinaryHandlingMode.BASE64)
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        int createDatabaseCount = 1;
        int createTableCount = 1;
        int insertCount = 1;
        SourceRecords sourceRecords = consumeRecordsByTopic(createDatabaseCount + createTableCount + insertCount);
        stopConnector();
        assertThat(sourceRecords).isNotNull();

        List<SourceRecord> topicSourceRecords = sourceRecords.recordsForTopic(DATABASE.topicForTable("dbz_1814_binary_mode_test"));
        assertThat(topicSourceRecords).hasSize(1);

        SourceRecord topicSourceRecord = topicSourceRecords.get(0);
        Struct kafkaDataStructure = (Struct) ((Struct) topicSourceRecord.value()).get("after");
        String expectedValue = "AQID";
        assertEquals(expectedValue, kafkaDataStructure.get("blob_col"));
        assertEquals(expectedValue, kafkaDataStructure.get("tinyblob_col"));
        assertEquals(expectedValue, kafkaDataStructure.get("mediumblob_col"));
        assertEquals(expectedValue, kafkaDataStructure.get("longblob_col"));
        assertEquals(expectedValue, kafkaDataStructure.get("binary_col"));
        assertEquals(expectedValue, kafkaDataStructure.get("varbinary_col"));

        // Check that all records are valid, can be serialized and deserialized ...
        sourceRecords.forEach(this::validate);
    }

    @Test
    @FixFor("DBZ-5544")
    public void shouldReceiveBase64UrlSafeBinary() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NEVER)
                .with(MySqlConnectorConfig.BINARY_HANDLING_MODE, BinaryHandlingMode.BASE64_URL_SAFE)
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        int createDatabaseCount = 1;
        int createTableCount = 1;
        int insertCount = 1;
        SourceRecords sourceRecords = consumeRecordsByTopic(createDatabaseCount + createTableCount + insertCount);
        stopConnector();
        assertThat(sourceRecords).isNotNull();

        List<SourceRecord> topicSourceRecords = sourceRecords.recordsForTopic(DATABASE.topicForTable("dbz_1814_binary_mode_test"));
        assertThat(topicSourceRecords).hasSize(1);

        SourceRecord topicSourceRecord = topicSourceRecords.get(0);
        Struct kafkaDataStructure = (Struct) ((Struct) topicSourceRecord.value()).get("after");
        String expectedValue = "AQID";
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
