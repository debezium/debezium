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
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;
import io.debezium.util.Testing;

/**
 * Verifies that the schema defaults of hexadecimal and bit-value literals match the values stored by the database,
 * both in a snapshot, which reads the defaults from SHOW CREATE TABLE, and after streamed DDL, which contains the literals.
 */
public abstract class BinlogBinaryLiteralDefaultValueIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-binary-literal-defaults.txt")
            .toAbsolutePath();
    private static final String TABLE = "binary_literal_defaults";
    private static final List<DefaultColumn> COLUMNS = List.of(
            new DefaultColumn("INT", "0x10", "0"),
            new DefaultColumn("INT", "X'31'", "0"),
            new DefaultColumn("INT", "b'10'", "0"),
            new DefaultColumn("INT", "0b10", "0"),
            new DefaultColumn("INT", "_latin1 0x31", "0"),
            new DefaultColumn("BIGINT UNSIGNED", "0xFFFFFFFFFFFFFFFF", "0"),
            new DefaultColumn("DECIMAL(10,2)", "0x10", "0"),
            new DefaultColumn("DOUBLE", "X'312E35'", "0"),
            new DefaultColumn("VARCHAR(32)", "0x4142", "'z'"),
            new DefaultColumn("VARCHAR(32)", "b'01000001'", "'z'"),
            new DefaultColumn("VARCHAR(32)", "0xE4BDA0", "'z'"),
            new DefaultColumn("VARCHAR(32) CHARACTER SET latin1", "0xE9", "'z'"),
            new DefaultColumn("CHAR(4)", "X'41'", "'z'"),
            new DefaultColumn("ENUM('a','b')", "0x62", "'a'"),
            new DefaultColumn("DATE", "0x323032342D30312D3032", "'2000-01-01'"),
            new DefaultColumn("BINARY(2)", "0x4142", "'zz'"),
            new DefaultColumn("VARBINARY(16)", "0x4142", "'z'"));

    private UniqueDatabase database;

    @BeforeEach
    void beforeEach() {
        stopConnector();
        Testing.Files.delete(OFFSET_STORE_PATH);
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
        database = TestHelper.getUniqueDatabase("binary_literal_defaults_server", "binary_literal_defaults")
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

    static Stream<Arguments> actionsAndCharacterSets() {
        // A column without a character set interprets the literal in the character set of its table, which can differ
        // from the one of the database
        return Stream.of("create-table", "add-column", "modify-column", "alter-default").flatMap(action -> Stream.of(
                Arguments.of(action, "utf8mb4", null),
                Arguments.of(action, "utf8mb4", "latin1"),
                Arguments.of(action, "latin1", "utf8mb4")));
    }

    @ParameterizedTest(name = "{0}, {1} database, {2} table")
    @MethodSource("actionsAndCharacterSets")
    @FixFor("debezium/dbz#2765")
    void shouldMatchStoredValuesOfHexadecimalAndBitValueDefaults(String action, String databaseCharset, String tableCharset) throws Exception {
        executeStatements(database.getDatabaseName(),
                "ALTER DATABASE " + database.getDatabaseName() + " CHARACTER SET " + databaseCharset,
                createTable(tableCharset),
                "INSERT INTO " + TABLE + " (id) VALUES (1)");
        start(getConnectorClass(), database.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, database.qualifiedTableName(TABLE))
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .build());

        final var assertions = new SoftAssertions();
        assertDefaultsMatchValues(assertions, consumeRecord(1), "snapshot");
        waitForStreamingRunning(getConnectorName(), database.getServerName());

        executeStatements(database.getDatabaseName(), streamingDdl(action, tableCharset).toArray(String[]::new));
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
            assertions.assertThat(storedValue).as("Stored value of %s, %s", COLUMNS.get(index), description).isNotNull();
            assertions.assertThat(after.schema().field(column).schema().defaultValue())
                    .as("Schema default of %s, %s", COLUMNS.get(index), description)
                    .isEqualTo(expectedSchemaDefault(record, storedValue));
        }
    }

    private List<String> streamingDdl(String action, String tableCharset) {
        if ("create-table".equals(action)) {
            return List.of("DROP TABLE " + TABLE, createTable(tableCharset));
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
                    // Start from another default, so that a literal that is not converted cannot keep a matching default
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

    private static String createTable(String charset) {
        return "CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, "
                + IntStream.range(0, COLUMNS.size()).mapToObj(index -> columnName(index) + " " + COLUMNS.get(index))
                        .collect(Collectors.joining(", "))
                + ")" + (charset != null ? " CHARACTER SET " + charset : "");
    }

    private static String columnName(int index) {
        return "c" + index;
    }

    private record DefaultColumn(String type, String literal, String otherLiteral) {
        @Override
        public String toString() {
            return type + " DEFAULT " + literal;
        }
    }
}
