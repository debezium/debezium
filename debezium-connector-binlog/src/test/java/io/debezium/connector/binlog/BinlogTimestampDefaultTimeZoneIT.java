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
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;
import io.debezium.util.Testing;

/**
 * Verifies that the schema defaults of {@code TIMESTAMP} columns match the values stored by the database when the DDL
 * statements run in sessions whose time zone differs from the one of the connector, both in a snapshot, which reads the
 * defaults from SHOW CREATE TABLE, and after streamed DDL.
 */
public abstract class BinlogTimestampDefaultTimeZoneIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-timestamp-defaults.txt")
            .toAbsolutePath();
    private static final String TABLE = "timestamp_defaults";

    private UniqueDatabase database;

    @BeforeEach
    void beforeEach() {
        stopConnector();
        Testing.Files.delete(OFFSET_STORE_PATH);
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
        database = TestHelper.getUniqueDatabase("timestamp_defaults_server", "timestamp_defaults")
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

    @Test
    @FixFor("debezium/dbz#2866")
    void shouldMatchStoredValuesOfTimestampDefaults() throws Exception {
        final Configuration config = database.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, database.qualifiedTableName(TABLE))
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .build();
        executeStatements(database.getDatabaseName(),
                "SET time_zone='+09:00'",
                "CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, v TIMESTAMP NOT NULL DEFAULT '2026-09-28 12:34:56')",
                "INSERT INTO " + TABLE + " (id) VALUES (1)");
        start(getConnectorClass(), config);

        final var assertions = new SoftAssertions();
        assertDefaultMatchesValue(assertions, consumeRecord(1), "v", "snapshot");
        waitForStreamingRunning(getConnectorName(), database.getServerName());

        executeStatements(database.getDatabaseName(),
                "SET time_zone='-05:00'",
                "ALTER TABLE " + TABLE + " ALTER COLUMN v SET DEFAULT '2026-10-01 00:00:00'",
                "INSERT INTO " + TABLE + " (id) VALUES (2)");
        assertDefaultMatchesValue(assertions, consumeRecord(2), "v", "streamed SET DEFAULT");

        // A session with the time zone of the server
        executeStatements(database.getDatabaseName(),
                "ALTER TABLE " + TABLE + " ADD COLUMN w TIMESTAMP NOT NULL DEFAULT '2026-11-01 00:00:00'",
                "INSERT INTO " + TABLE + " (id) VALUES (3)");
        final SourceRecord added = consumeRecord(3);
        assertDefaultMatchesValue(assertions, added, "v", "streamed ADD COLUMN");
        assertDefaultMatchesValue(assertions, added, "w", "streamed ADD COLUMN");

        // The schema history is recovered with the time zones of the statements
        stopConnector();
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), database.getServerName());
        executeStatements(database.getDatabaseName(), "INSERT INTO " + TABLE + " (id) VALUES (4)");
        final SourceRecord afterRestart = consumeRecord(4);
        assertDefaultMatchesValue(assertions, afterRestart, "v", "after restart");
        assertDefaultMatchesValue(assertions, afterRestart, "w", "after restart");
        assertions.assertAll();
    }

    private SourceRecord consumeRecord(int id) throws InterruptedException {
        final List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic(database.topicForTable(TABLE));
        assertThat(records).hasSize(1);
        assertThat(((Struct) records.get(0).value()).getStruct("after").getInt32("id")).isEqualTo(id);
        return records.get(0);
    }

    private void assertDefaultMatchesValue(SoftAssertions assertions, SourceRecord record, String column, String description) {
        final Struct after = ((Struct) record.value()).getStruct("after");
        // The row was inserted without the column, so the raw value is the default stored by the database
        final Object storedValue = after.getWithoutDefault(column);
        assertions.assertThat(storedValue).as("Stored value of %s, %s", column, description).isNotNull();
        assertions.assertThat(after.schema().field(column).schema().defaultValue())
                .as("Schema default of %s, %s", column, description)
                .isEqualTo(storedValue);
    }
}
