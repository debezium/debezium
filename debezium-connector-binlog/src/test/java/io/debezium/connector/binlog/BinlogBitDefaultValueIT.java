/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.binlog.BitDefaultValueTestCases.BitDefaultValueCase;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.data.Bits;
import io.debezium.doc.FixFor;
import io.debezium.util.Testing;

public abstract class BinlogBitDefaultValueIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-bit-defaults.txt")
            .toAbsolutePath();
    private static final String TABLE = "bit_defaults";

    private UniqueDatabase database;

    @BeforeEach
    void beforeEach() {
        stopConnector();
        Testing.Files.delete(OFFSET_STORE_PATH);
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
        database = TestHelper.getUniqueDatabase("bit_defaults_server", "bit_defaults")
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
    @ValueSource(strings = { "modify-column", "alter-default", "add-column", "create-table" })
    @FixFor("debezium/dbz#2751")
    void shouldPreserveBitDefaultsFromSnapshotAndStreamingDdl(String action) throws Exception {
        final List<BitDefaultValueCase> cases = BitDefaultValueTestCases.readCases().stream()
                .filter(testCase -> action.equals(testCase.action()))
                .toList();
        assertThat(cases).isNotEmpty();

        // Batch independent literals as columns so each DDL path needs only one connector start.
        executeStatements(database.getDatabaseName(), createTable(cases), "INSERT INTO " + TABLE + " (id) VALUES (1)");
        final var config = database.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, database.qualifiedTableName(TABLE))
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .build();
        start(getConnectorClass(), config);

        final var snapshot = consumeRecord(1, "r");
        final var snapshotAssertions = new SoftAssertions();
        assertDefaultsAndValues(snapshotAssertions, snapshot, cases, 1, false);
        snapshotAssertions.assertAll();
        waitForStreamingRunning(getConnectorName(), database.getServerName());

        final var columns = IntStream.range(0, cases.size()).mapToObj(BinlogBitDefaultValueIT::columnName)
                .collect(Collectors.joining(", "));
        final var zeroes = IntStream.range(0, cases.size()).mapToObj(index -> "b'0'")
                .collect(Collectors.joining(", "));
        executeStatements(database.getDatabaseName(), "INSERT INTO " + TABLE + " (id, " + columns + ") VALUES (2, " + zeroes + ")");
        consumeRecord(2, "c");

        applyStreamingDdl(action, cases);
        executeStatements(database.getDatabaseName(), "INSERT INTO " + TABLE + " (id) VALUES (3)");
        final var streaming = consumeRecord(3, "c");
        final var streamingAssertions = new SoftAssertions();
        assertDefaultsAndValues(streamingAssertions, streaming, cases, 3, false);

        if (cases.stream().anyMatch(BitDefaultValueCase::nullable)) {
            final var values = cases.stream().map(testCase -> testCase.nullable() ? "NULL" : "DEFAULT")
                    .collect(Collectors.joining(", "));
            executeStatements(database.getDatabaseName(), "INSERT INTO " + TABLE + " (id, " + columns + ") VALUES (4, " + values + ")");
            final var explicitNull = consumeRecord(4, "c");
            assertDefaultsAndValues(streamingAssertions, explicitNull, cases, 4, true);
        }

        streamingAssertions.assertAll();
    }

    @Test
    @FixFor("debezium/dbz#2751")
    void shouldPreserveBitDefaultsAcceptedInNonStrictMode() throws Exception {
        final List<BitDefaultValueCase> cases = nonStrictBitDefaultCases();
        assertThat(cases).isNotEmpty();

        executeNonStrict(createTable(cases), "INSERT INTO " + TABLE + " (id) VALUES (1)");
        final var config = database.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, database.qualifiedTableName(TABLE))
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .build();
        start(getConnectorClass(), config);

        final var snapshot = consumeRecord(1, "r");
        final var snapshotAssertions = new SoftAssertions();
        assertDefaultsAndValues(snapshotAssertions, snapshot, cases, 1, false);
        snapshotAssertions.assertAll();
        waitForStreamingRunning(getConnectorName(), database.getServerName());

        executeStatements(database.getDatabaseName(), "INSERT INTO " + TABLE + " (id) VALUES (2)");
        consumeRecord(2, "c");

        final List<String> statements = new ArrayList<>();
        for (int index = 0; index < cases.size(); index++) {
            statements.add("ALTER TABLE " + TABLE + " ALTER COLUMN " + columnName(index)
                    + " SET DEFAULT " + cases.get(index).initialDefaultLiteral());
            statements.add("ALTER TABLE " + TABLE + " MODIFY COLUMN " + definition(columnName(index), cases.get(index)));
        }
        statements.add("INSERT INTO " + TABLE + " (id) VALUES (3)");
        executeNonStrict(statements.toArray(String[]::new));

        final var streaming = consumeRecord(3, "c");
        final var streamingAssertions = new SoftAssertions();
        assertDefaultsAndValues(streamingAssertions, streaming, cases, 3, false);
        streamingAssertions.assertAll();
    }

    private void executeNonStrict(String... statements) throws SQLException {
        // SQL mode is session-local; the DDL and INSERT must use the connection that sets it.
        try (var connection = getTestDatabaseConnection(database.getDatabaseName())) {
            connection.execute("SET SESSION sql_mode = ''");
            connection.execute(statements);
        }
    }

    private SourceRecord consumeRecord(int id, String operation) throws InterruptedException {
        final List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic(database.topicForTable(TABLE));
        assertThat(records).hasSize(1);
        final var record = records.get(0);
        final var envelope = (Struct) record.value();
        assertThat(envelope.getString("op")).isEqualTo(operation);
        assertThat(envelope.getStruct("after").getInt32("id")).isEqualTo(id);
        return record;
    }

    private void applyStreamingDdl(String action, List<BitDefaultValueCase> cases) {
        if ("create-table".equals(action)) {
            executeStatements(database.getDatabaseName(), "DROP TABLE " + TABLE, createTable(cases));
            return;
        }
        for (int index = 0; index < cases.size(); index++) {
            final var testCase = cases.get(index);
            final var column = columnName(index);
            switch (action) {
                case "modify-column" -> executeStatements(database.getDatabaseName(),
                        "ALTER TABLE " + TABLE + " ALTER COLUMN " + column + " SET DEFAULT " + testCase.initialDefaultLiteral(),
                        "ALTER TABLE " + TABLE + " MODIFY COLUMN " + definition(column, testCase));
                case "alter-default" -> {
                    // Start from a different value so an ignored literal cannot retain a matching default.
                    executeStatements(database.getDatabaseName(), "ALTER TABLE " + TABLE + " ALTER COLUMN " + column
                            + " SET DEFAULT " + testCase.initialDefaultLiteral());
                    executeStatements(database.getDatabaseName(),
                            "ALTER TABLE " + TABLE + " ALTER COLUMN " + column + " SET DEFAULT " + testCase.literal());
                }
                case "add-column" -> executeStatements(database.getDatabaseName(),
                        "ALTER TABLE " + TABLE + " DROP COLUMN " + column,
                        "ALTER TABLE " + TABLE + " ADD COLUMN " + definition(column, testCase));
                default -> throw new IllegalArgumentException("Unknown DDL action: " + action);
            }
        }
    }

    private void assertDefaultsAndValues(SoftAssertions assertions, SourceRecord record, List<BitDefaultValueCase> cases, int id, boolean explicitNull)
            throws SQLException {
        final var after = ((Struct) record.value()).getStruct("after");
        final Map<String, String> storedValues = storedValues(cases, id);
        for (int index = 0; index < cases.size(); index++) {
            final var testCase = cases.get(index);
            final var column = columnName(index);
            final var expectedDefault = "NULL".equals(testCase.expectedValue()) ? null : testCase.expectedValue();
            final var expectedRow = explicitNull && testCase.nullable() ? null : expectedDefault;
            final var schema = after.schema().field(column).schema();
            final var description = testCase.id() + ", " + testCase.action() + ", row " + id;

            assertions.assertThat(storedValues.get(column)).as("Database value: %s", description).isEqualTo(expectedRow);
            assertions.assertThat(asUnsignedDecimal(after.getWithoutDefault(column)))
                    .as("Raw row value: %s", description).isEqualTo(storedValues.get(column));
            assertions.assertThat(asUnsignedDecimal(schema.defaultValue()))
                    .as("Schema default: %s", description).isEqualTo(expectedDefault);
            assertions.assertThat(schema.isOptional()).as("Optional: %s", description).isEqualTo(testCase.nullable());
            assertions.assertThat(schema.type()).as("Schema type: %s", description)
                    .isEqualTo(testCase.width() == 1 ? Schema.Type.BOOLEAN : Schema.Type.BYTES);
            if (testCase.width() > 1) {
                assertions.assertThat(schema.name()).as("Logical type: %s", description).isEqualTo(Bits.LOGICAL_NAME);
                assertions.assertThat(schema.parameters().get(Bits.LENGTH_FIELD)).as("Bit width: %s", description)
                        .isEqualTo(Integer.toString(testCase.width()));
            }
        }
    }

    private Map<String, String> storedValues(List<BitDefaultValueCase> cases, int id) throws SQLException {
        final var selections = IntStream.range(0, cases.size())
                .mapToObj(index -> "CAST(" + columnName(index) + " AS UNSIGNED)")
                .collect(Collectors.joining(", "));
        final Map<String, String> values = new LinkedHashMap<>();
        try (var connection = getTestDatabaseConnection(database.getDatabaseName())) {
            connection.query("SELECT " + selections + " FROM " + TABLE + " WHERE id=" + id, result -> {
                assertThat(result.next()).isTrue();
                for (int index = 0; index < cases.size(); index++) {
                    values.put(columnName(index), result.getString(index + 1));
                }
                assertThat(result.next()).isFalse();
            });
        }
        return values;
    }

    private static String createTable(List<BitDefaultValueCase> cases) {
        final var definitions = IntStream.range(0, cases.size())
                .mapToObj(index -> definition(columnName(index), cases.get(index)))
                .collect(Collectors.joining(", "));
        return "CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, " + definitions + ")";
    }

    private static String definition(String column, BitDefaultValueCase testCase) {
        return column + " BIT(" + testCase.width() + ") " + (testCase.nullable() ? "NULL" : "NOT NULL") + " DEFAULT " + testCase.literal();
    }

    private static String columnName(int index) {
        return "bit" + index;
    }

    private static String asUnsignedDecimal(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean bit) {
            return bit ? "1" : "0";
        }
        assertThat(value).isInstanceOf(byte[].class);
        final var littleEndian = (byte[]) value;
        final var bigEndian = new byte[littleEndian.length];
        for (int index = 0; index < littleEndian.length; index++) {
            bigEndian[index] = littleEndian[littleEndian.length - index - 1];
        }
        return new BigInteger(1, bigEndian).toString();
    }

    protected abstract List<BitDefaultValueCase> nonStrictBitDefaultCases();
}
