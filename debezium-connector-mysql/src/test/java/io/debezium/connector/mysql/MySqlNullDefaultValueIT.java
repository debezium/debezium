/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.connector.binlog.AbstractBinlogConnectorIT;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;
import io.debezium.util.Testing;

public class MySqlNullDefaultValueIT extends AbstractBinlogConnectorIT<MySqlConnector> implements MySqlCommon {

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-null-defaults.txt").toAbsolutePath();
    private static final String TABLE = "null_defaults";
    private static final List<String> COLUMNS = List.of("text_value", "binary_value", "enum_value", "set_value", "boolean_value",
            "bit_value", "number_value", "date_value");

    private UniqueDatabase database;

    @BeforeEach
    void beforeEach() {
        stopConnector();
        Testing.Files.delete(OFFSET_STORE_PATH);
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
        database = TestHelper.getUniqueDatabase("null_defaults_server", "null_defaults").withDbHistoryPath(SCHEMA_HISTORY_PATH);
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
            executeStatements("mysql", "DROP DATABASE IF EXISTS " + database.getDatabaseName());
        }
    }

    @ParameterizedTest
    @EnumSource(BinaryHandlingMode.class)
    @FixFor("debezium/dbz#2755")
    void shouldPreserveNullDefaultsAfterStreamingDdlAndRestart(BinaryHandlingMode binaryHandlingMode) throws Exception {
        executeStatements(database.getDatabaseName(), """
                CREATE TABLE null_defaults (
                    id INT PRIMARY KEY,
                    text_value VARCHAR(16) NULL DEFAULT 'old',
                    binary_value VARBINARY(16) NULL DEFAULT 'old',
                    enum_value ENUM('old','null') NULL DEFAULT 'old',
                    set_value SET('old','null') NULL DEFAULT 'old',
                    boolean_value BOOLEAN NULL DEFAULT TRUE,
                    bit_value BIT(1) NULL DEFAULT b'1',
                    number_value INT NULL DEFAULT 1,
                    date_value DATE NULL DEFAULT '2026-01-01',
                    quoted_value VARCHAR(16) NULL DEFAULT 'old'
                )
                """, "INSERT INTO " + TABLE + " (id) VALUES (1)");
        final var config = database.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, database.qualifiedTableName(TABLE))
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(MySqlConnectorConfig.BINARY_HANDLING_MODE, binaryHandlingMode)
                .build();
        start(MySqlConnector.class, config);

        final var snapshot = consumeAfter(1, "r");
        for (String column : COLUMNS) {
            assertThat(snapshot.schema().field(column).schema().defaultValue()).as(column).isNotNull();
        }
        waitForStreamingRunning(getConnectorName(), database.getServerName());
        executeStatements(database.getDatabaseName(), "INSERT INTO " + TABLE + " (id) VALUES (2)");
        consumeAfter(2, "c");

        final var alterations = COLUMNS.stream().map(column -> "ALTER COLUMN " + column + " SET DEFAULT NULL")
                .collect(Collectors.joining(", "));
        final var nulls = COLUMNS.stream().map(column -> "NULL").collect(Collectors.joining(", "));
        executeStatements(database.getDatabaseName(),
                "ALTER TABLE " + TABLE + " " + alterations + ", ALTER COLUMN quoted_value SET DEFAULT 'null'",
                "INSERT INTO " + TABLE + " (id) VALUES (3)",
                "INSERT INTO " + TABLE + " (id, " + String.join(", ", COLUMNS) + ") VALUES (4, " + nulls + ")");
        assertNullDefaults(3);
        assertNullDefaults(4);

        stopConnector();
        executeStatements(database.getDatabaseName(), "INSERT INTO " + TABLE + " (id) VALUES (5)");
        start(MySqlConnector.class, config);
        assertNullDefaults(5);
    }

    private Struct consumeAfter(int id, String operation) throws InterruptedException {
        final var records = consumeRecordsByTopic(1).recordsForTopic(database.topicForTable(TABLE));
        assertThat(records).hasSize(1);
        final var envelope = (Struct) records.get(0).value();
        assertThat(envelope.getString("op")).isEqualTo(operation);
        final var after = envelope.getStruct("after");
        assertThat(after.getInt32("id")).isEqualTo(id);
        return after;
    }

    private void assertNullDefaults(int id) throws InterruptedException, SQLException {
        final var after = consumeAfter(id, "c");
        for (String column : COLUMNS) {
            assertThat(after.schema().field(column).schema().defaultValue()).as("Schema default: %s", column).isNull();
            assertThat(after.getWithoutDefault(column)).as("Raw value: %s", column).isNull();
            assertThat(after.get(column)).as("Value with default: %s", column).isNull();
        }
        assertThat(after.schema().field("quoted_value").schema().defaultValue()).isEqualTo("null");
        assertThat(after.getWithoutDefault("quoted_value")).isEqualTo("null");

        try (var connection = getTestDatabaseConnection(database.getDatabaseName())) {
            connection.query("SELECT * FROM " + TABLE + " WHERE id = " + id, result -> {
                assertThat(result.next()).isTrue();
                for (String column : COLUMNS) {
                    assertThat(result.getObject(column)).as("MySQL value: %s", column).isNull();
                }
                assertThat(result.getString("quoted_value")).isEqualTo("null");
                assertThat(result.next()).isFalse();
            });
        }
    }
}
