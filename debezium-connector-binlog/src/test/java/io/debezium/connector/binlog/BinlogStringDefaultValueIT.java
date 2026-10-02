/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;
import io.debezium.util.Testing;

public abstract class BinlogStringDefaultValueIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-string-defaults.txt").toAbsolutePath();
    private static final String TABLE = "string_defaults";

    private UniqueDatabase database;

    @BeforeEach
    void beforeEach() {
        stopConnector();
        Testing.Files.delete(OFFSET_STORE_PATH);
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
        database = TestHelper.getUniqueDatabase("string_defaults_server", "string_defaults").withDbHistoryPath(SCHEMA_HISTORY_PATH);
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
    @MethodSource("stringDefaults")
    @FixFor("debezium/dbz#2764")
    void shouldPreserveStringDefaults(String literal, String expected, String alteration) throws Exception {
        final var snapshotDefault = alteration.isEmpty() ? literal : "'old'";
        executeStatements(database.getDatabaseName(),
                "CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, v VARCHAR(64) NOT NULL DEFAULT " + snapshotDefault + ")",
                "INSERT INTO " + TABLE + " (id) VALUES (1)");
        final var config = database.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, database.qualifiedTableName(TABLE))
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .build();
        start(getConnectorClass(), config);

        assertDefault(1, "r", alteration.isEmpty() ? expected : "old");
        waitForStreamingRunning(getConnectorName(), database.getServerName());
        if (!alteration.isEmpty()) {
            executeStatements(database.getDatabaseName(), "ALTER TABLE " + TABLE + " " + alteration.formatted(literal));
        }
        executeStatements(database.getDatabaseName(), "INSERT INTO " + TABLE + " (id) VALUES (2)");
        assertDefault(2, "c", expected);

        stopConnector();
        executeStatements(database.getDatabaseName(), "INSERT INTO " + TABLE + " (id) VALUES (3)");
        start(getConnectorClass(), config);
        assertDefault(3, "c", expected);
    }

    private static Stream<Arguments> stringDefaults() {
        return Stream.of(
                Arguments.of("'a''b'", "a'b"),
                Arguments.of("'a\\nb'", "a\nb"),
                Arguments.of("N'abc'", "abc"),
                Arguments.of("'a' 'b'", "ab"),
                Arguments.of("'a\\\\nb'", "a\\nb"),
                Arguments.of("\"a\"\"b\"", "a\"b"))
                .flatMap(arguments -> Stream.of("", "MODIFY COLUMN v VARCHAR(64) NOT NULL DEFAULT %s", "ALTER COLUMN v SET DEFAULT %s")
                        .map(alteration -> Arguments.of(arguments.get()[0], arguments.get()[1], alteration)));
    }

    private void assertDefault(int id, String operation, String expected) throws InterruptedException, SQLException {
        try (var connection = getTestDatabaseConnection(database.getDatabaseName())) {
            connection.query("SELECT v FROM " + TABLE + " WHERE id = " + id, result -> {
                assertThat(result.next()).isTrue();
                assertThat(result.getString("v")).as("Database value").isEqualTo(expected);
                assertThat(result.next()).isFalse();
            });
        }
        final var records = consumeRecordsByTopic(1).recordsForTopic(database.topicForTable(TABLE));
        assertThat(records).hasSize(1);
        final var envelope = (Struct) records.get(0).value();
        assertThat(envelope.getString("op")).isEqualTo(operation);
        final var after = envelope.getStruct("after");
        assertThat(after.getInt32("id")).isEqualTo(id);
        assertThat(after.getWithoutDefault("v")).as("Raw value").isEqualTo(expected);
        assertThat(after.schema().field("v").schema().defaultValue()).as("Schema default").isEqualTo(expected);
    }
}
