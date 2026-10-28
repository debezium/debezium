/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.CommonConnectorConfig.EventProcessingFailureHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.AbstractBinlogConnectorIT;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.util.Testing;

/**
 * Verifies that updates written with {@code binlog_row_value_options=PARTIAL_JSON} aren't dropped silently.
 */
public class MySqlPartialJsonIT extends AbstractBinlogConnectorIT<MySqlConnector> implements MySqlCommon {

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-partial-json.txt")
            .toAbsolutePath();
    private static final String CAPTURED_TABLE = "partial_json_captured";
    private static final String OTHER_TABLE = "partial_json_other";
    private static final String PARTIAL_JSON = "SET SESSION binlog_row_value_options = 'PARTIAL_JSON'";

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
                "CREATE TABLE " + CAPTURED_TABLE + " (id INT PRIMARY KEY, name VARCHAR(20))",
                "CREATE TABLE " + OTHER_TABLE + " (id INT PRIMARY KEY, name VARCHAR(20))",
                "INSERT INTO " + CAPTURED_TABLE + " VALUES (1, 'a')",
                "INSERT INTO " + OTHER_TABLE + " VALUES (1, 'a')");
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
    void shouldFailAtStartupWhenGlobalValueIsPartialJson() {
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        executeStatements(database.getDatabaseName(), "SET GLOBAL binlog_row_value_options = 'PARTIAL_JSON'");
        try {
            start(getConnectorClass(), connectorConfig(EventProcessingFailureHandlingMode.FAIL), (success, message, error) -> failure.set(error));
            Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> !isEngineRunning.get());
        }
        finally {
            executeStatements(database.getDatabaseName(), "SET GLOBAL binlog_row_value_options = ''");
        }
        assertThat(failure.get()).hasStackTraceContaining("configured to use binlog_row_value_options=PARTIAL_JSON");
    }

    @Test
    @FixFor("debezium/dbz#2958")
    void shouldFailOnPartialUpdateOfCapturedTable() throws Exception {
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        start(getConnectorClass(), connectorConfig(EventProcessingFailureHandlingMode.FAIL), (success, message, error) -> failure.set(error));
        waitForStreamingRunning(getConnectorName(), database.getServerName());

        executeStatements(database.getDatabaseName(), PARTIAL_JSON, "UPDATE " + CAPTURED_TABLE + " SET name = 'b' WHERE id = 1");

        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> !isEngineRunning.get());
        assertThat(failure.get()).hasStackTraceContaining("Encountered update event for table " + database.qualifiedTableName(CAPTURED_TABLE));
    }

    @Test
    @FixFor("debezium/dbz#2958")
    void shouldSkipPartialUpdateOfTableThatIsNotCaptured() throws Exception {
        start(getConnectorClass(), connectorConfig(EventProcessingFailureHandlingMode.FAIL));
        waitForStreamingRunning(getConnectorName(), database.getServerName());

        executeStatements(database.getDatabaseName(), PARTIAL_JSON, "UPDATE " + OTHER_TABLE + " SET name = 'b' WHERE id = 1");
        executeStatements(database.getDatabaseName(), "UPDATE " + CAPTURED_TABLE + " SET name = 'b' WHERE id = 1");

        assertUpdate("b");
        assertConnectorIsRunning();
    }

    @Test
    @FixFor("debezium/dbz#2958")
    void shouldSkipPartialUpdateOfCapturedTableInWarnMode() throws Exception {
        start(getConnectorClass(), connectorConfig(EventProcessingFailureHandlingMode.WARN));
        waitForStreamingRunning(getConnectorName(), database.getServerName());

        executeStatements(database.getDatabaseName(), PARTIAL_JSON, "UPDATE " + CAPTURED_TABLE + " SET name = 'b' WHERE id = 1");
        executeStatements(database.getDatabaseName(), "UPDATE " + CAPTURED_TABLE + " SET name = 'c' WHERE id = 1");

        // The partial update is skipped, so the next update is the first event
        assertUpdate("c");
        assertConnectorIsRunning();
    }

    private void assertUpdate(String expectedName) throws InterruptedException {
        final List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic(database.topicForTable(CAPTURED_TABLE));
        assertThat(records).hasSize(1);
        final Struct value = (Struct) records.get(0).value();
        assertThat(value.getString(Envelope.FieldName.OPERATION)).isEqualTo(Envelope.Operation.UPDATE.code());
        assertThat(value.getStruct(Envelope.FieldName.AFTER).getString("name")).isEqualTo(expectedName);
    }

    private Configuration connectorConfig(EventProcessingFailureHandlingMode mode) {
        return database.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, database.qualifiedTableName(CAPTURED_TABLE))
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(CommonConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE, mode)
                .build();
    }
}
