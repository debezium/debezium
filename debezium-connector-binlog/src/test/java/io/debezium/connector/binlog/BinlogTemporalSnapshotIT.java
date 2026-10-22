/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;
import io.debezium.time.Conversions;
import io.debezium.util.Testing;

/**
 * Verifies that a snapshot emits {@code DATE} and {@code DATETIME} values the same way as streaming.
 */
public abstract class BinlogTemporalSnapshotIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-temporal-snapshot.txt")
            .toAbsolutePath();
    private static final String TABLE = "temporal_snapshot";

    protected UniqueDatabase database;

    @BeforeEach
    void beforeEach() {
        stopConnector();
        Testing.Files.delete(OFFSET_STORE_PATH);
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
        database = TestHelper.getUniqueDatabase("temporal_snapshot_server", "empty")
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

    @ParameterizedTest(name = "snapshot.fetch.size={0}")
    @ValueSource(ints = { 0, 10 })
    @FixFor("debezium/dbz#2921")
    void shouldSnapshotDatetimeThatDoesNotExistInJvmTimeZone(int snapshotFetchSize) throws Exception {
        final TimeZone defaultTimeZone = TimeZone.getDefault();
        try {
            // 2024-03-10 02:30:00 falls into the daylight saving time gap of this time zone, and 1582-10-10 into the gap
            // of the Julian to Gregorian calendar switch of java.util.GregorianCalendar
            TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));
            final String values = "'2024-03-10 02:30:00', '1582-10-10 00:00:00', '2024-02-29 12:34:56.000005'";
            executeStatements(database.getDatabaseName(),
                    "CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, dst DATETIME, gregorian DATETIME, micros DATETIME(6))",
                    "INSERT INTO " + TABLE + " VALUES (1, " + values + ")");
            start(getConnectorClass(), connectorConfig(snapshotFetchSize).build());

            final Struct snapshot = consumeAfter();
            waitForStreamingRunning(getConnectorName(), database.getServerName());
            executeStatements(database.getDatabaseName(), "INSERT INTO " + TABLE + " VALUES (2, " + values + ")");
            final Struct streaming = consumeAfter();

            for (Struct after : List.of(snapshot, streaming)) {
                assertThat(after.get("dst")).isEqualTo(LocalDateTime.of(2024, 3, 10, 2, 30).toInstant(ZoneOffset.UTC).toEpochMilli());
                assertThat(after.get("gregorian")).isEqualTo(LocalDateTime.of(1582, 10, 10, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli());
                assertThat(after.get("micros")).isEqualTo(Conversions.toEpochMicros(LocalDateTime.of(2024, 2, 29, 12, 34, 56, 5_000).toInstant(ZoneOffset.UTC)));
            }
        }
        finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    // With snapshot.fetch.size > 0, the server-side cursor of MySQL returns invalid dates as zero dates, so they are not covered here
    @Test
    @FixFor("debezium/dbz#2921")
    void shouldSnapshotInvalidDateAsStreamingDoesWithWarnMode() throws Exception {
        final String values = "'2020-02-31', '2020-02-31 10:00:00'";
        executeStatements(database.getDatabaseName(),
                "CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, d DATE, dt DATETIME)",
                "SET SESSION sql_mode = 'ALLOW_INVALID_DATES'",
                "INSERT INTO " + TABLE + " VALUES (1, " + values + ")");
        start(getConnectorClass(), connectorConfig(0)
                .with(CommonConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE, CommonConnectorConfig.EventProcessingFailureHandlingMode.WARN)
                .build());

        final Struct snapshot = consumeAfter();
        waitForStreamingRunning(getConnectorName(), database.getServerName());
        executeStatements(database.getDatabaseName(),
                "SET SESSION sql_mode = 'ALLOW_INVALID_DATES'",
                "INSERT INTO " + TABLE + " VALUES (2, " + values + ")");
        final Struct streaming = consumeAfter();

        // Values that are not valid dates are emitted as the epoch
        for (Struct after : List.of(snapshot, streaming)) {
            assertThat(after.get("d")).isEqualTo(0);
            assertThat(after.get("dt")).isEqualTo(0L);
        }
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource({ "DATE, 2020-02-31", "DATETIME, 2020-02-31 10:00:00" })
    @FixFor("debezium/dbz#2921")
    void shouldFailSnapshotOnInvalidDateWithFailMode(String type, String value) throws Exception {
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        executeStatements(database.getDatabaseName(),
                "CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, v " + type + ")",
                "SET SESSION sql_mode = 'ALLOW_INVALID_DATES'",
                "INSERT INTO " + TABLE + " VALUES (1, '" + value + "')");
        start(getConnectorClass(), connectorConfig(0).build(), (success, message, error) -> failure.set(error));

        // The snapshot stops instead of retrying, like streaming does
        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> !isEngineRunning.get());
        assertThat(failure.get()).hasStackTraceContaining("Invalid value in column 'v' of table '" + database.qualifiedTableName(TABLE) + "'");
    }

    private Configuration.Builder connectorConfig(int snapshotFetchSize) {
        final Configuration.Builder config = database.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, database.qualifiedTableName(TABLE))
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false);
        if (snapshotFetchSize > 0) {
            // Reads the snapshot over the binary protocol of MySQL
            config.with(CommonConnectorConfig.SNAPSHOT_FETCH_SIZE, snapshotFetchSize);
        }
        return config;
    }

    private Struct consumeAfter() throws InterruptedException {
        final List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic(database.topicForTable(TABLE));
        assertThat(records).hasSize(1);
        return ((Struct) records.get(0).value()).getStruct("after");
    }
}
