/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;
import io.debezium.util.Testing;

public abstract class BinlogBinaryDefaultValueIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-binary-defaults.txt")
            .toAbsolutePath();
    private static final String TABLE = "binary_defaults";

    private UniqueDatabase database;

    @BeforeEach
    void beforeEach() {
        stopConnector();
        Testing.Files.delete(OFFSET_STORE_PATH);
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
        database = TestHelper.getUniqueDatabase("binary_defaults_server", "empty")
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
    @EnumSource(value = BinaryHandlingMode.class, names = { "BYTES", "HEX" })
    @FixFor("debezium/dbz#2766")
    void shouldMatchStoredValuesOfBinaryDefaults(BinaryHandlingMode binaryHandlingMode) throws Exception {
        final Configuration config = database.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, database.qualifiedTableName(TABLE))
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(CommonConnectorConfig.BINARY_HANDLING_MODE, binaryHandlingMode.getValue())
                .build();
        executeStatements(database.getDatabaseName(),
                "CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, b BINARY(8) NOT NULL DEFAULT 'abc', "
                        + "e BINARY(4) NOT NULL DEFAULT '', vb VARBINARY(8) NOT NULL DEFAULT 'abc')",
                "INSERT INTO " + TABLE + " (id) VALUES (1)");
        start(getConnectorClass(), config);

        final var assertions = new SoftAssertions();
        assertDefaultsMatchValues(assertions, consumeRecord(1), "snapshot", "b", "e", "vb");
        waitForStreamingRunning(getConnectorName(), database.getServerName());

        executeStatements(database.getDatabaseName(),
                "ALTER TABLE " + TABLE + " MODIFY COLUMN b BINARY(8) NOT NULL DEFAULT 'abc'",
                "ALTER TABLE " + TABLE + " ALTER COLUMN e SET DEFAULT ''",
                "ALTER TABLE " + TABLE + " ADD COLUMN s BINARY(6) NOT NULL DEFAULT 'a '",
                "INSERT INTO " + TABLE + " (id) VALUES (2)");
        assertDefaultsMatchValues(assertions, consumeRecord(2), "streaming", "b", "e", "vb", "s");

        // The schema history is recovered with the same defaults
        stopConnector();
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), database.getServerName());
        executeStatements(database.getDatabaseName(), "INSERT INTO " + TABLE + " (id) VALUES (3)");
        assertDefaultsMatchValues(assertions, consumeRecord(3), "after restart", "b", "e", "vb", "s");
        assertions.assertAll();
    }

    private SourceRecord consumeRecord(int id) throws InterruptedException {
        final List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic(database.topicForTable(TABLE));
        assertThat(records).hasSize(1);
        assertThat(((Struct) records.get(0).value()).getStruct("after").getInt32("id")).isEqualTo(id);
        return records.get(0);
    }

    private void assertDefaultsMatchValues(SoftAssertions assertions, SourceRecord record, String phase, String... columns) {
        final Struct after = ((Struct) record.value()).getStruct("after");
        for (String column : columns) {
            // The row was inserted without the column, so the raw value is the default stored by the database
            final Object storedValue = after.getWithoutDefault(column);
            assertions.assertThat(storedValue).as("Stored value of %s, %s", column, phase).isNotNull();
            assertions.assertThat(after.schema().field(column).schema().defaultValue())
                    .as("Schema default of %s, %s", column, phase)
                    .isEqualTo(storedValue);
        }
    }
}
