/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.converters.ZeroDateFallbackConverter;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.data.Envelope;

/**
 * Integration tests for {@link ZeroDateFallbackConverter}.
 *
 * <p>Verifies the converter's behavior across four scenarios over a fixture table {@code dt_full}
 * with 9 DATE/DATETIME/TIMESTAMP columns (no default / epoch default / non-epoch default) and 3
 * rows (real-1970 / zero-date / normal). Snapshot-only; streaming-path coverage is provided by
 * {@code RowDeserializersTest} and the lambda input-type unit tests.
 *
 * @author minleejae
 */
public abstract class BinlogZeroDateFallbackConverterIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-zero-date-converter.txt").toAbsolutePath();

    // Pre-computed wire-format constants used across all scenarios.
    private static final int EPOCH_DAYS_REAL_1970 = 0;
    private static final int EPOCH_DAYS_NORMAL = (int) LocalDate.parse("2024-06-15").toEpochDay();
    private static final long EPOCH_MS_REAL_1970 = LocalDateTime.parse("1970-01-01T00:00:00").atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    private static final long EPOCH_MS_NORMAL = LocalDateTime.parse("2024-06-15T12:34:56").atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    private static final String ISO_REAL_1970 = "1970-01-02T00:00:00Z";
    private static final String ISO_NORMAL = "2024-06-15T12:34:56Z";

    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("zdfb", "zero_date_fallback_converter")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private Configuration config;

    @BeforeEach
    void beforeEach() {
        stopConnector();
        // sql_mode='' allows zero-date INSERTs to land; time_zone='+00:00' forces the fixture
        // session to UTC so TIMESTAMP literals are stored without per-CI timezone drift.
        DATABASE.createAndInitialize(Collections.singletonMap(
                "sessionVariables", "sql_mode='',time_zone='+00:00'"));
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
    }

    @AfterEach
    void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    /**
     * Scenario 1 — all type-level fallbacks are NULL. Zero-date rows emit null on every temporal
     * column; real-1970 / normal rows still flow through normal conversion.
     */
    @Test
    void scenarioDefaultAllNullFallbackZeroDateEmitsNull() throws InterruptedException {
        config = baseConfig().build();
        start(getConnectorClass(), config);

        final List<SourceRecord> recs = readDtFullRecords();
        assertScenarioDefault(recs);
    }

    /**
     * Scenario 2 — all type-level fallbacks set to user values. Zero-date rows emit the user
     * sentinel; real-1970 / normal rows are unaffected.
     */
    @Test
    void scenarioTypedValuesUserSentinelsZeroDateEmitsUserValues() throws InterruptedException {
        config = baseConfig()
                .with("zero-date.fallback.date", "2000-01-01")
                .with("zero-date.fallback.datetime", "2000-01-01 00:00:00")
                .with("zero-date.fallback.timestamp", "2000-01-01T00:00:00Z")
                .build();
        start(getConnectorClass(), config);

        final int sentinelDays = (int) LocalDate.parse("2000-01-01").toEpochDay();
        final long sentinelMs = LocalDateTime.parse("2000-01-01T00:00:00").toInstant(ZoneOffset.UTC).toEpochMilli();
        final String sentinelIso = "2000-01-01T00:00:00Z";

        final List<SourceRecord> recs = readDtFullRecords();
        // Zero-date row: every column emits the configured sentinel.
        assertRow(recs, 2, "d_no_default", sentinelDays);
        assertRow(recs, 2, "d_epoch_default", sentinelDays);
        assertRow(recs, 2, "d_other_default", sentinelDays);
        assertRow(recs, 2, "dt_no_default", sentinelMs);
        assertRow(recs, 2, "dt_epoch_default", sentinelMs);
        assertRow(recs, 2, "dt_other_default", sentinelMs);
        assertRow(recs, 2, "ts_no_default", sentinelIso);
        assertRow(recs, 2, "ts_epoch_default", sentinelIso);
        assertRow(recs, 2, "ts_other_default", sentinelIso);

        // Real-1970 row: unchanged.
        assertRow(recs, 1, "d_no_default", EPOCH_DAYS_REAL_1970);
        assertRow(recs, 1, "dt_no_default", EPOCH_MS_REAL_1970);
        assertRow(recs, 1, "ts_no_default", ISO_REAL_1970);
        // Normal row: unchanged.
        assertRow(recs, 3, "d_no_default", EPOCH_DAYS_NORMAL);
        assertRow(recs, 3, "dt_no_default", EPOCH_MS_NORMAL);
        assertRow(recs, 3, "ts_no_default", ISO_NORMAL);
    }

    /**
     * Scenario 3 — mixed policy. DATE / TIMESTAMP are NULL, DATETIME is a user sentinel.
     */
    @Test
    void scenarioMixedDateAndTimestampNullDatetimeUserValue() throws InterruptedException {
        config = baseConfig()
                .with("zero-date.fallback.datetime", "2000-01-01 00:00:00")
                .build();
        start(getConnectorClass(), config);

        final long datetimeSentinel = LocalDateTime.parse("2000-01-01T00:00:00").toInstant(ZoneOffset.UTC).toEpochMilli();

        final List<SourceRecord> recs = readDtFullRecords();
        // DATE columns: NULL policy
        assertSchemaOptional(recs, "d_no_default", true);
        assertSchemaOptional(recs, "d_epoch_default", true);
        // TIMESTAMP columns: NULL policy
        assertSchemaOptional(recs, "ts_no_default", true);
        assertSchemaOptional(recs, "ts_epoch_default", true);
        // DATETIME columns: user-value policy → NOT NULL retained
        assertSchemaOptional(recs, "dt_no_default", false);

        // Zero-date row
        assertRow(recs, 2, "d_no_default", null);
        assertRow(recs, 2, "ts_no_default", null);
        assertRow(recs, 2, "dt_no_default", datetimeSentinel);
        assertRow(recs, 2, "dt_other_default", datetimeSentinel);
    }

    /**
     * Scenario 4 — column-level FQN override. Three DATETIME columns get three different
     * effective policies (NULL / type-level value / column-level value).
     */
    @Test
    void scenarioColumnOverridePerColumnEffectivePolicy() throws InterruptedException {
        final String db = DATABASE.getDatabaseName();
        config = baseConfig()
                .with("zero-date.fallback.datetime", "2000-01-01 00:00:00")
                .with("zero-date.column." + db + ".dt_full.dt_no_default.fallback", "NULL")
                .with("zero-date.column." + db + ".dt_full.dt_other_default.fallback", "2099-12-31 23:59:59")
                .build();
        start(getConnectorClass(), config);

        final long typeLevelSentinel = LocalDateTime.parse("2000-01-01T00:00:00").toInstant(ZoneOffset.UTC).toEpochMilli();
        final long columnLevelSentinel = LocalDateTime.parse("2099-12-31T23:59:59").toInstant(ZoneOffset.UTC).toEpochMilli();

        final List<SourceRecord> recs = readDtFullRecords();
        // dt_no_default: column-level NULL → schema optional
        assertSchemaOptional(recs, "dt_no_default", true);
        // dt_epoch_default: no column key → falls back to type-level
        assertSchemaOptional(recs, "dt_epoch_default", false);
        // dt_other_default: column-level value
        assertSchemaOptional(recs, "dt_other_default", false);

        // Zero-date row receives each column's distinct policy
        assertRow(recs, 2, "dt_no_default", null);
        assertRow(recs, 2, "dt_epoch_default", typeLevelSentinel);
        assertRow(recs, 2, "dt_other_default", columnLevelSentinel);
    }

    private Configuration.Builder baseConfig() {
        return DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("dt_full"))
                .with(BinlogConnectorConfig.CUSTOM_CONVERTERS, "zero-date")
                // Override the IT framework default (US/Samoa) so TIMESTAMP emit is deterministic
                // regardless of CI MySQL/MariaDB version. mysql-connector-j 8.0 only applies
                // connectionTimeZone to the JVM side, so also force the MySQL server session to
                // current timezone via sessionVariables (mariadb-java-client syncs both sides automatically).
                .with("database.connectionTimeZone", ZoneId.systemDefault().getId())
                .with("database.sessionVariables", "time_zone='%s'".formatted(ZoneId.systemDefault().getId()))
                .with("zero-date.type", ZeroDateFallbackConverter.class.getName());
    }

    private List<SourceRecord> readDtFullRecords() throws InterruptedException {
        final SourceRecords records = consumeRecordsByTopic(9); // 6 DDL events + 3 records from snapshot.
        return records.recordsForTopic(DATABASE.topicForTable("dt_full"));
    }

    private void assertScenarioDefault(List<SourceRecord> recs) {
        // All temporal columns are optional under the default NULL policy.
        for (String name : new String[]{
                "d_no_default", "d_epoch_default", "d_other_default",
                "dt_no_default", "dt_epoch_default", "dt_other_default",
                "ts_no_default", "ts_epoch_default", "ts_other_default" }) {
            assertSchemaOptional(recs, name, true);
        }

        // Real-1970 row (id=1): genuine values flow through, NOT replaced with NULL.
        assertRow(recs, 1, "d_no_default", EPOCH_DAYS_REAL_1970);
        assertRow(recs, 1, "dt_no_default", EPOCH_MS_REAL_1970);
        assertRow(recs, 1, "ts_no_default", ISO_REAL_1970);

        // Zero-date row (id=2): every temporal column emits null.
        assertRow(recs, 2, "d_no_default", null);
        assertRow(recs, 2, "d_epoch_default", null);
        assertRow(recs, 2, "d_other_default", null);
        assertRow(recs, 2, "dt_no_default", null);
        assertRow(recs, 2, "dt_epoch_default", null);
        assertRow(recs, 2, "dt_other_default", null);
        assertRow(recs, 2, "ts_no_default", null);
        assertRow(recs, 2, "ts_epoch_default", null);
        assertRow(recs, 2, "ts_other_default", null);

        // Normal row (id=3): genuine values unaffected.
        assertRow(recs, 3, "d_no_default", EPOCH_DAYS_NORMAL);
        assertRow(recs, 3, "dt_no_default", EPOCH_MS_NORMAL);
        assertRow(recs, 3, "ts_no_default", ISO_NORMAL);
    }

    private static void assertSchemaOptional(List<SourceRecord> recs, String fieldName, boolean expectedOptional) {
        final Schema afterSchema = recs.get(0).valueSchema().field(Envelope.FieldName.AFTER).schema();
        assertThat(afterSchema.field(fieldName).schema().isOptional())
                .as("schema.optional for field %s", fieldName)
                .isEqualTo(expectedOptional);
    }

    private static void assertRow(List<SourceRecord> recs, int id, String fieldName, Object expected) {
        final SourceRecord rec = recs.stream()
                .filter(r -> ((Struct) r.value()).getStruct(Envelope.FieldName.AFTER).getInt32("id") == id)
                .findFirst()
                .orElseThrow(() -> new AssertionError("No record with id=" + id));
        final Struct after = ((Struct) rec.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get(fieldName))
                .as("after.%s for id=%d", fieldName, id)
                .isEqualTo(expected);
    }
}
