/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Collections;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;

/**
 * @author luobo on 2018/6/8 14:16
 */
public abstract class BinlogDefaultValueAllZeroTimeIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-connect.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("myServer1", "default_value_all_zero_time")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private Configuration config;

    @BeforeEach
    void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize(Collections.singletonMap("sessionVariables", "sql_mode=''"));
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

    @Test
    void allZeroDateAndTimeTypeTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("all_zero_date_and_time_table"))
                .build();
        start(getConnectorClass(), config);

        // Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(9);
        final SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("all_zero_date_and_time_table")).get(0);

        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        Schema schemaC = record.valueSchema().fields().get(1).schema().fields().get(2).schema();
        Schema schemaD = record.valueSchema().fields().get(1).schema().fields().get(3).schema();
        Schema schemaE = record.valueSchema().fields().get(1).schema().fields().get(4).schema();
        Schema schemaF = record.valueSchema().fields().get(1).schema().fields().get(5).schema();
        Schema schemaG = record.valueSchema().fields().get(1).schema().fields().get(6).schema();
        Schema schemaH = record.valueSchema().fields().get(1).schema().fields().get(7).schema();
        Schema schemaI = record.valueSchema().fields().get(1).schema().fields().get(8).schema();
        Schema schemaJ = record.valueSchema().fields().get(1).schema().fields().get(9).schema();
        Schema schemaK = record.valueSchema().fields().get(1).schema().fields().get(10).schema();
        Schema schemaL = record.valueSchema().fields().get(1).schema().fields().get(11).schema();

        // column A, 0000-00-00 00:00:00 => 1970-01-01 00:00:00
        assertThat(schemaA.defaultValue()).isEqualTo("1970-01-01T00:00:00Z");

        // column B allows null, default value should be null
        assertThat(schemaB.isOptional()).isEqualTo(true);
        assertThat(schemaB.defaultValue()).isEqualTo(null);

        // column C, 0000-00-00 00:00:00 => 1970-01-01 00:00:00
        assertThat(schemaC.defaultValue()).isEqualTo("1970-01-01T00:00:00Z");

        // column D allows null, default value should be null
        assertThat(schemaD.isOptional()).isEqualTo(true);
        assertThat(schemaD.defaultValue()).isEqualTo(null);

        // column E, 0000-00-00 => 1970-01-01
        assertThat(schemaE.defaultValue()).isEqualTo(0);

        // column F allows null, default value should be null
        assertThat(schemaF.isOptional()).isEqualTo(true);
        assertThat(schemaF.defaultValue()).isEqualTo(null);

        // column G, 0000-00-00 => 1970-01-01
        assertThat(schemaG.defaultValue()).isEqualTo(0);

        // column H allows null, default value should be null
        assertThat(schemaH.isOptional()).isEqualTo(true);
        assertThat(schemaH.defaultValue()).isEqualTo(null);

        // column I, DATETIME: 0000-00-00 00:00:00 => 1970-01-01 00:00:00
        assertThat(schemaI.defaultValue()).isEqualTo(0L);

        // column J allows null, default value should be null
        assertThat(schemaJ.isOptional()).isEqualTo(true);
        assertThat(schemaJ.defaultValue()).isEqualTo(null);

        // column K, DATETIME: 0000-00-00 00:00:00 => 1970-01-01 00:00:00
        assertThat(schemaK.defaultValue()).isEqualTo(0L);

        // column L allows null, default value should be null
        assertThat(schemaL.isOptional()).isEqualTo(true);
        assertThat(schemaL.defaultValue()).isEqualTo(null);

    }

    @Test
    @FixFor("DBZ-4334")
    public void partZeroDateAndTimeTypeTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("part_zero_date_and_time_table"))
                .build();
        start(getConnectorClass(), config);

        // Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(9);
        final SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("part_zero_date_and_time_table")).get(0);

        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        Schema schemaC = record.valueSchema().fields().get(1).schema().fields().get(2).schema();
        Schema schemaD = record.valueSchema().fields().get(1).schema().fields().get(3).schema();
        Schema schemaE = record.valueSchema().fields().get(1).schema().fields().get(4).schema();
        Schema schemaF = record.valueSchema().fields().get(1).schema().fields().get(5).schema();
        Schema schemaG = record.valueSchema().fields().get(1).schema().fields().get(6).schema();
        Schema schemaH = record.valueSchema().fields().get(1).schema().fields().get(7).schema();

        // column A, 0000-00-00 00:00:00 => 1970-01-01 00:00:00
        assertThat(schemaA.defaultValue()).isEqualTo("1970-01-01T00:00:00Z");

        // column B allows null, default value should be null
        assertThat(schemaB.isOptional()).isEqualTo(true);
        assertThat(schemaB.defaultValue()).isEqualTo(null);

        // column C, 0000-00-00 01:00:00.000 => 1970-01-01 00:00:00
        assertThat(schemaC.defaultValue()).isEqualTo(0L);

        // column D allows null, default value should be null
        assertThat(schemaD.isOptional()).isEqualTo(true);
        assertThat(schemaD.defaultValue()).isEqualTo(null);

        // column E, 1000-00-00 01:00:00.000 => 1970-01-01
        assertThat(schemaE.defaultValue()).isEqualTo(0);

        // column F allows null, default value should be null
        assertThat(schemaF.isOptional()).isEqualTo(true);
        assertThat(schemaF.defaultValue()).isEqualTo(null);

        // column G, 01:00:00.000 => 0
        assertThat(schemaG.defaultValue()).isEqualTo(3600000000L);

        // column H allows null
        assertThat(schemaH.isOptional()).isEqualTo(true);
        assertThat(schemaH.defaultValue()).isEqualTo(3600000000L);
    }

    /**
     * Verifies that a uniform {@code zero.date.fallback.value} drives the schema {@code default}
     * for non-nullable DATE / DATETIME / TIMESTAMP columns across all three types.
     */
    @Test
    public void uniformSentinelOverridesAllTypes() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("all_zero_date_and_time_table"))
                .with(BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE, "1001-01-01")
                .build();
        start(getConnectorClass(), config);

        SourceRecords records = consumeRecordsByTopic(9);
        final SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("all_zero_date_and_time_table")).get(0);

        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaC = record.valueSchema().fields().get(1).schema().fields().get(2).schema();
        Schema schemaE = record.valueSchema().fields().get(1).schema().fields().get(4).schema();
        Schema schemaG = record.valueSchema().fields().get(1).schema().fields().get(6).schema();
        Schema schemaI = record.valueSchema().fields().get(1).schema().fields().get(8).schema();
        Schema schemaK = record.valueSchema().fields().get(1).schema().fields().get(10).schema();

        // 1001-01-01 in epoch days = -353920, in epoch millis = -30578688000000.
        final long sentinelMillis = -30578688000000L;
        final int sentinelDays = -353920;

        // TIMESTAMP columns A / C are emitted as ZonedTimestamp ISO strings
        assertThat(schemaA.defaultValue()).isEqualTo("1001-01-01T00:00:00Z");
        assertThat(schemaC.defaultValue()).isEqualTo("1001-01-01T00:00:00Z");
        // DATE columns E / G are int32 epoch days
        assertThat(schemaE.defaultValue()).isEqualTo(sentinelDays);
        assertThat(schemaG.defaultValue()).isEqualTo(sentinelDays);
        // DATETIME columns I / K are int64 epoch millis
        assertThat(schemaI.defaultValue()).isEqualTo(sentinelMillis);
        assertThat(schemaK.defaultValue()).isEqualTo(sentinelMillis);

        // Nullable companions still default to null.
        assertThat(record.valueSchema().fields().get(1).schema().fields().get(1).schema().defaultValue()).isNull();
        assertThat(record.valueSchema().fields().get(1).schema().fields().get(3).schema().defaultValue()).isNull();
        assertThat(record.valueSchema().fields().get(1).schema().fields().get(5).schema().defaultValue()).isNull();
        assertThat(record.valueSchema().fields().get(1).schema().fields().get(7).schema().defaultValue()).isNull();
        assertThat(record.valueSchema().fields().get(1).schema().fields().get(9).schema().defaultValue()).isNull();
        assertThat(record.valueSchema().fields().get(1).schema().fields().get(11).schema().defaultValue()).isNull();
    }

    /**
     * Verifies that {@code zero.date.fallback.value.datetime} affects only DATETIME columns;
     * DATE and TIMESTAMP columns inherit the (unset) general default of {@code 1970-01-01}.
     */
    @Test
    public void perTypeDatetimeOverrideLeavesOtherTypesAtDefault() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("all_zero_date_and_time_table"))
                .with(BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE_DATETIME, "1001-01-01")
                .build();
        start(getConnectorClass(), config);

        SourceRecords records = consumeRecordsByTopic(9);
        final SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("all_zero_date_and_time_table")).get(0);

        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaE = record.valueSchema().fields().get(1).schema().fields().get(4).schema();
        Schema schemaI = record.valueSchema().fields().get(1).schema().fields().get(8).schema();
        Schema schemaK = record.valueSchema().fields().get(1).schema().fields().get(10).schema();

        // TIMESTAMP — falls back to the general default (1970-01-01)
        assertThat(schemaA.defaultValue()).isEqualTo("1970-01-01T00:00:00Z");
        // DATE — falls back to the general default (epoch day = 0)
        assertThat(schemaE.defaultValue()).isEqualTo(0);
        // DATETIME — uses the per-type override (1001-01-01 epoch millis = -30578688000000)
        assertThat(schemaI.defaultValue()).isEqualTo(-30578688000000L);
        assertThat(schemaK.defaultValue()).isEqualTo(-30578688000000L);
    }

    /**
     * End-to-end verification that the configured zero-date sentinel is applied to the
     * <strong>streaming</strong> row payload (not just the snapshot schema default). Inserts a
     * row with {@code DEFAULT} values for every temporal column after the snapshot completes,
     * then asserts the resulting {@code op=c} record carries the sentinel.
     *
     * <p>This particularly exercises the {@code TIMESTAMP} column zero-date detection in
     * {@code RowDeserializers.deserializeTimestamp(V2)} — without the {@code epochSecond == 0}
     * guard, those columns would emit {@code "1970-01-01T00:00:00Z"} regardless of the
     * configured sentinel, since the binlog wire encoding for a zero-date {@code TIMESTAMP} is
     * indistinguishable from a real epoch value at the byte level.
     */
    @Test
    public void streamingZeroDateRowAppliesUniformSentinel() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("all_zero_date_and_time_table"))
                .with(BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE, "1001-01-01")
                .build();
        start(getConnectorClass(), config);

        // Drain the snapshot record(s) and wait for the connector to enter streaming mode before
        // emitting a binlog event the test cares about.
        consumeRecordsByTopic(9);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), "streaming");

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());
                JdbcConnection connection = db.connect();
                Connection jdbc = connection.connection();
                Statement statement = jdbc.createStatement()) {
            statement.executeUpdate(
                    "INSERT INTO all_zero_date_and_time_table VALUES "
                            + "(DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT)");
        }

        SourceRecords streaming = consumeRecordsByTopic(1);
        SourceRecord rec = streaming.recordsForTopic(DATABASE.topicForTable("all_zero_date_and_time_table")).get(0);
        Struct after = ((Struct) rec.value()).getStruct("after");

        // 1001-01-01 → epoch days = -353920, epoch millis = -30578688000000
        final long sentinelMillis = -30578688000000L;
        final int sentinelDays = -353920;

        // TIMESTAMP non-nullable columns A, C — the §14 streaming fix is what makes these match.
        assertThat(after.getString("A")).isEqualTo("1001-01-01T00:00:00Z");
        assertThat(after.getString("C")).isEqualTo("1001-01-01T00:00:00Z");
        // DATE non-nullable columns E, G — already worked before §14.
        assertThat(after.getInt32("E")).isEqualTo(sentinelDays);
        assertThat(after.getInt32("G")).isEqualTo(sentinelDays);
        // DATETIME non-nullable columns I, K — already worked before §14.
        assertThat(after.getInt64("I")).isEqualTo(sentinelMillis);
        assertThat(after.getInt64("K")).isEqualTo(sentinelMillis);
    }
}
