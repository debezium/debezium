/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.source.SourceConnector;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.util.Testing;

/**
 * Verifies that the connector fails with an explanation when a client writes row images that omit columns, by setting
 * {@code binlog_row_image} for its session, while the database server uses {@code FULL}.
 */
public abstract class BinlogSessionRowImageIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-session-row-image.txt")
            .toAbsolutePath();
    private static final String TABLE = "session_row_image";

    private UniqueDatabase database;

    @BeforeEach
    void beforeEach() {
        stopConnector();
        Testing.Files.delete(OFFSET_STORE_PATH);
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
        database = TestHelper.getUniqueDatabase("session_row_image_server", "empty")
                .withDbHistoryPath(SCHEMA_HISTORY_PATH);
        database.create();
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

    @ParameterizedTest(name = "{0} session, {1}")
    @CsvSource(delimiter = '|', value = {
            "MINIMAL | UPDATE | UPDATE before",
            "NOBLOB  | UPDATE | UPDATE before",
            "MINIMAL | DELETE | DELETE" })
    @FixFor("debezium/dbz#962")
    void shouldFailOnRowImageThatOmitsColumnsOfSession(String sessionRowImage, String operation, String image) throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor(BinlogStreamingChangeEventSource.class);
        executeStatements(database.getDatabaseName(),
                "CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, a INT, b VARCHAR(10), c TEXT)",
                "INSERT INTO " + TABLE + " VALUES (1, 1, 'x', 'y')");
        start(getConnectorClass(), database.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, database.qualifiedTableName(TABLE))
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .build());
        assertThat(consumeRecordsByTopic(1).recordsForTopic(database.topicForTable(TABLE))).hasSize(1);
        waitForStreamingRunning(getConnectorName(), database.getServerName());

        // The server uses FULL, but a client changes the row image of its own session
        executeStatements(database.getDatabaseName(),
                "SET SESSION binlog_row_image='" + sessionRowImage + "'",
                "UPDATE".equals(operation) ? "UPDATE " + TABLE + " SET a = 2 WHERE id = 1" : "DELETE FROM " + TABLE + " WHERE id = 1");

        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> !isEngineRunning.get());
        assertThat(logInterceptor.containsMessage("Incomplete row image (" + image + ") for table " + database.qualifiedTableName(TABLE))).isTrue();
        assertThat(logInterceptor.containsMessage("A client may have set binlog_row_image=MINIMAL or NOBLOB for its session")).isTrue();
        assertThat(logInterceptor.containsMessage("restart connector with schema recovery mode")).isFalse();
    }
}
