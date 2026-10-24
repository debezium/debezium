/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;
import io.debezium.util.Testing;

public abstract class BinlogEnumSetDefaultValueIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-enum-set-defaults.txt")
            .toAbsolutePath();
    private static final String TABLE = "enum_set_defaults";
    private static final List<DefaultColumn> COLUMNS = List.of(
            new DefaultColumn("SET('a','b','c')", "'b,a'", "'c'"),
            new DefaultColumn("SET('a','b','c')", "'c,a,b'", "'c'"),
            new DefaultColumn("SET('a','b','c')", "'B,a'", "'c'"),
            new DefaultColumn("SET('a','b','c')", "'b,a,b'", "'c'"),
            new DefaultColumn("SET('a','b','c')", "''", "'c'"),
            new DefaultColumn("SET('A','b')", "'a'", "'b'"),
            new DefaultColumn("ENUM('a','b','c')", "'B'", "'c'"),
            new DefaultColumn("SET('a','A') COLLATE utf8mb4_bin", "'A'", "'a'"));

    private UniqueDatabase database;

    @BeforeEach
    void beforeEach() {
        stopConnector();
        Testing.Files.delete(OFFSET_STORE_PATH);
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
        database = TestHelper.getUniqueDatabase("enum_set_defaults_server", "empty")
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

    @ParameterizedTest
    @ValueSource(strings = { "create-table", "add-column", "modify-column", "alter-default" })
    @FixFor("debezium/dbz#2768")
    void shouldMatchStoredValuesOfEnumAndSetDefaults(String action) throws Exception {
        executeStatements(database.getDatabaseName(), createTable(), "INSERT INTO " + TABLE + " (id) VALUES (1)");
        start(getConnectorClass(), database.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, database.qualifiedTableName(TABLE))
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .build());

        final var assertions = new SoftAssertions();
        assertDefaultsMatchValues(assertions, consumeRecord(1), "snapshot");
        waitForStreamingRunning(getConnectorName(), database.getServerName());

        executeStatements(database.getDatabaseName(), streamingDdl(action).toArray(String[]::new));
        executeStatements(database.getDatabaseName(), "INSERT INTO " + TABLE + " (id) VALUES (2)");
        assertDefaultsMatchValues(assertions, consumeRecord(2), action);
        assertions.assertAll();
    }

    private SourceRecord consumeRecord(int id) throws InterruptedException {
        final List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic(database.topicForTable(TABLE));
        assertThat(records).hasSize(1);
        assertThat(((Struct) records.get(0).value()).getStruct("after").getInt32("id")).isEqualTo(id);
        return records.get(0);
    }

    private void assertDefaultsMatchValues(SoftAssertions assertions, SourceRecord record, String description) {
        final Struct after = ((Struct) record.value()).getStruct("after");
        for (int index = 0; index < COLUMNS.size(); index++) {
            final String column = columnName(index);
            // The row was inserted without the column, so the raw value is the default stored by the database
            final Object storedValue = after.getWithoutDefault(column);
            assertions.assertThat(after.schema().field(column).schema().defaultValue())
                    .as("Schema default of %s, %s", COLUMNS.get(index), description)
                    .isEqualTo(expectedSchemaDefault(record, storedValue));
        }
    }

    private List<String> streamingDdl(String action) {
        if ("create-table".equals(action)) {
            return List.of("DROP TABLE " + TABLE, createTable());
        }
        final List<String> statements = new ArrayList<>();
        for (int index = 0; index < COLUMNS.size(); index++) {
            final String column = columnName(index);
            final DefaultColumn defaultColumn = COLUMNS.get(index);
            switch (action) {
                case "add-column" -> {
                    statements.add("ALTER TABLE " + TABLE + " DROP COLUMN " + column);
                    statements.add("ALTER TABLE " + TABLE + " ADD COLUMN " + column + " " + defaultColumn);
                }
                case "modify-column" -> {
                    // Start from another default, so that a default that is not converted cannot keep a matching default
                    statements.add("ALTER TABLE " + TABLE + " ALTER COLUMN " + column + " SET DEFAULT " + defaultColumn.otherLiteral());
                    statements.add("ALTER TABLE " + TABLE + " MODIFY COLUMN " + column + " " + defaultColumn);
                }
                case "alter-default" -> {
                    statements.add("ALTER TABLE " + TABLE + " ALTER COLUMN " + column + " SET DEFAULT " + defaultColumn.otherLiteral());
                    statements.add("ALTER TABLE " + TABLE + " ALTER COLUMN " + column + " SET DEFAULT " + defaultColumn.literal());
                }
                default -> throw new IllegalArgumentException("Unexpected DDL action: " + action);
            }
        }
        return statements;
    }

    private static String createTable() {
        return "CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, "
                + IntStream.range(0, COLUMNS.size()).mapToObj(index -> columnName(index) + " " + COLUMNS.get(index))
                        .collect(Collectors.joining(", "))
                + ")";
    }

    private static String columnName(int index) {
        return "c" + index;
    }

    private record DefaultColumn(String type, String literal, String otherLiteral) {
        @Override
        public String toString() {
            return type + " NOT NULL DEFAULT " + literal;
        }
    }
}
