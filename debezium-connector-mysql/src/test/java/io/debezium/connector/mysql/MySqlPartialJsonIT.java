/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.AbstractBinlogConnectorIT;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.processors.reselect.ReselectColumnsPostProcessor;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Testing;

/**
 * Verifies that updates written with {@code binlog_row_value_options=PARTIAL_JSON} are captured.
 */
public class MySqlPartialJsonIT extends AbstractBinlogConnectorIT<MySqlConnector> implements MySqlCommon {

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-partial-json.txt")
            .toAbsolutePath();
    private static final String PLAIN_TABLE = "partial_json_plain";
    private static final String JSON_TABLE = "partial_json_doc";
    private static final String PARTIAL_JSON = "SET SESSION binlog_row_value_options = 'PARTIAL_JSON'";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private UniqueDatabase database;

    @BeforeEach
    void beforeEach() {
        stopConnector();
        Testing.Files.delete(OFFSET_STORE_PATH);
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
        database = TestHelper.getUniqueDatabase("partial_json_server", "empty")
                .withDbHistoryPath(SCHEMA_HISTORY_PATH);
        database.create();
        executeStatements(database.getDatabaseName(),
                "CREATE TABLE " + PLAIN_TABLE + " (id INT PRIMARY KEY, name VARCHAR(20))",
                "CREATE TABLE " + JSON_TABLE + " (id INT PRIMARY KEY, v INT, j JSON)",
                "INSERT INTO " + PLAIN_TABLE + " VALUES (1, 'a')",
                "INSERT INTO " + JSON_TABLE + " VALUES (1, 10, '{\"a\": 1, \"b\": 2}')");
        initializeConnectorTestFramework();
    }

    @AfterEach
    void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(OFFSET_STORE_PATH);
            Testing.Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("debezium/dbz#2958")
    void shouldCaptureUpdateOfTableWithoutJsonColumns() throws Exception {
        start(getConnectorClass(), connectorConfig().build());
        waitForStreamingRunning(getConnectorName(), database.getServerName());

        executeStatements(database.getDatabaseName(), PARTIAL_JSON, "UPDATE " + PLAIN_TABLE + " SET name = 'b' WHERE id = 1");

        assertThat(consumeUpdate(PLAIN_TABLE).getString("name")).isEqualTo("b");
    }

    @Test
    @FixFor("debezium/dbz#2958")
    void shouldTakeUnmodifiedJsonColumnFromBeforeImage() throws Exception {
        start(getConnectorClass(), connectorConfig().build());
        waitForStreamingRunning(getConnectorName(), database.getServerName());

        executeStatements(database.getDatabaseName(), PARTIAL_JSON, "UPDATE " + JSON_TABLE + " SET v = v + 1 WHERE id = 1");

        final Struct after = consumeUpdate(JSON_TABLE);
        assertThat(after.getInt32("v")).isEqualTo(11);
        assertJson(after.getString("j"), "{\"a\": 1, \"b\": 2}");
    }

    @Test
    @FixFor("debezium/dbz#2958")
    void shouldCaptureJsonColumnReplacedWithNewDocument() throws Exception {
        start(getConnectorClass(), connectorConfig().build());
        waitForStreamingRunning(getConnectorName(), database.getServerName());

        executeStatements(database.getDatabaseName(), PARTIAL_JSON, "UPDATE " + JSON_TABLE + " SET j = '{\"x\": 1}' WHERE id = 1");

        assertJson(consumeUpdate(JSON_TABLE).getString("j"), "{\"x\": 1}");
    }

    @Test
    @FixFor("debezium/dbz#2958")
    void shouldEmitUnavailableValuePlaceholderForJsonDiff() throws Exception {
        start(getConnectorClass(), connectorConfig().build());
        waitForStreamingRunning(getConnectorName(), database.getServerName());

        executeStatements(database.getDatabaseName(), PARTIAL_JSON, "UPDATE " + JSON_TABLE + " SET v = v + 1, j = JSON_SET(j, '$.b', 7) WHERE id = 1");

        final Struct after = consumeUpdate(JSON_TABLE);
        assertThat(after.getInt32("v")).isEqualTo(11);
        assertThat(after.getString("j")).isEqualTo(RelationalDatabaseConnectorConfig.DEFAULT_UNAVAILABLE_VALUE_PLACEHOLDER);
    }

    @Test
    @FixFor("debezium/dbz#2958")
    void shouldReselectJsonColumnWithJsonDiff() throws Exception {
        start(getConnectorClass(), connectorConfig()
                .with(BinlogConnectorConfig.CUSTOM_POST_PROCESSORS, "reselector")
                .with("post.processors.reselector.type", ReselectColumnsPostProcessor.class.getName())
                .build());
        waitForStreamingRunning(getConnectorName(), database.getServerName());

        executeStatements(database.getDatabaseName(), PARTIAL_JSON, "UPDATE " + JSON_TABLE + " SET j = JSON_SET(j, '$.b', 7) WHERE id = 1");

        assertJson(consumeUpdate(JSON_TABLE).getString("j"), "{\"a\": 1, \"b\": 7}");
    }

    private Struct consumeUpdate(String table) throws InterruptedException {
        final List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic(database.topicForTable(table));
        assertThat(records).hasSize(1);
        final Struct value = (Struct) records.get(0).value();
        assertThat(value.getString(Envelope.FieldName.OPERATION)).isEqualTo(Envelope.Operation.UPDATE.code());
        return value.getStruct(Envelope.FieldName.AFTER);
    }

    private static void assertJson(String actual, String expected) throws Exception {
        final JsonNode actualNode = MAPPER.readTree(actual);
        assertThat(actualNode).isEqualTo(MAPPER.readTree(expected));
    }

    private Configuration.Builder connectorConfig() {
        return database.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false);
    }
}
