/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Time;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.util.Testing;

/**
 * @author luobo
 */
public class MysqlDefaultValueIT extends AbstractConnectorTest {
    private static final int EVENT_COUNT = 43;
    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-connect.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("myServer1", "default_value")
            .withDbHistoryPath(DB_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(DB_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        } finally {
            Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    @Test
    public void unsignedTinyIntTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(MySqlConnector.class, config);

        Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("UNSIGNED_TINYINT_TABLE")).get(0);
        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        Schema schemaC = record.valueSchema().fields().get(1).schema().fields().get(2).schema();
        Schema schemaD = record.valueSchema().fields().get(1).schema().fields().get(3).schema();
        Schema schemaE = record.valueSchema().fields().get(1).schema().fields().get(4).schema();
        Schema schemaF = record.valueSchema().fields().get(1).schema().fields().get(5).schema();
        assertThat(schemaA.isOptional()).isEqualTo(true);
        assertThat(schemaA.defaultValue()).isEqualTo((short) 0);
        assertThat(schemaB.isOptional()).isEqualTo(true);
        assertThat(schemaB.defaultValue()).isEqualTo((short) 10);
        assertThat(schemaC.isOptional()).isEqualTo(true);
        assertThat(schemaC.defaultValue()).isEqualTo(null);
        assertThat(schemaD.isOptional()).isEqualTo(false);
        assertThat(schemaE.isOptional()).isEqualTo(false);
        assertThat(schemaE.defaultValue()).isEqualTo((short) 0);
        assertThat(schemaF.isOptional()).isEqualTo(false);
        assertThat(schemaF.defaultValue()).isEqualTo((short) 0);
    }

    @Test
    public void unsignedSmallIntTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(MySqlConnector.class, config);

        Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("UNSIGNED_SMALLINT_TABLE")).get(0);
        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        Schema schemaC = record.valueSchema().fields().get(1).schema().fields().get(2).schema();
        Schema schemaD = record.valueSchema().fields().get(1).schema().fields().get(3).schema();
        Schema schemaE = record.valueSchema().fields().get(1).schema().fields().get(4).schema();
        Schema schemaF = record.valueSchema().fields().get(1).schema().fields().get(5).schema();
        assertThat(schemaA.isOptional()).isEqualTo(true);
        assertThat(schemaA.defaultValue()).isEqualTo(0);
        assertThat(schemaB.isOptional()).isEqualTo(true);
        assertThat(schemaB.defaultValue()).isEqualTo(10);
        assertThat(schemaC.isOptional()).isEqualTo(true);
        assertThat(schemaC.defaultValue()).isEqualTo(null);
        assertThat(schemaD.isOptional()).isEqualTo(false);
        assertThat(schemaE.isOptional()).isEqualTo(false);
        assertThat(schemaE.defaultValue()).isEqualTo(0);
        assertThat(schemaF.isOptional()).isEqualTo(false);
        assertThat(schemaF.defaultValue()).isEqualTo(0);
    }

    @Test
    public void unsignedMediumIntTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(MySqlConnector.class, config);

        Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("UNSIGNED_MEDIUMINT_TABLE")).get(0);
        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        Schema schemaC = record.valueSchema().fields().get(1).schema().fields().get(2).schema();
        Schema schemaD = record.valueSchema().fields().get(1).schema().fields().get(3).schema();
        Schema schemaE = record.valueSchema().fields().get(1).schema().fields().get(4).schema();
        Schema schemaF = record.valueSchema().fields().get(1).schema().fields().get(5).schema();
        assertThat(schemaA.isOptional()).isEqualTo(true);
        assertThat(schemaA.defaultValue()).isEqualTo(0);
        assertThat(schemaB.isOptional()).isEqualTo(true);
        assertThat(schemaB.defaultValue()).isEqualTo(10);
        assertThat(schemaC.isOptional()).isEqualTo(true);
        assertThat(schemaC.defaultValue()).isEqualTo(null);
        assertThat(schemaD.isOptional()).isEqualTo(false);
        assertThat(schemaE.isOptional()).isEqualTo(false);
        assertThat(schemaE.defaultValue()).isEqualTo(0);
        assertThat(schemaF.isOptional()).isEqualTo(false);
        assertThat(schemaF.defaultValue()).isEqualTo(0);
    }

    @Test
    public void unsignedIntTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(MySqlConnector.class, config);

        Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("UNSIGNED_INT_TABLE")).get(0);
        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        Schema schemaC = record.valueSchema().fields().get(1).schema().fields().get(2).schema();
        Schema schemaD = record.valueSchema().fields().get(1).schema().fields().get(3).schema();
        Schema schemaE = record.valueSchema().fields().get(1).schema().fields().get(4).schema();
        Schema schemaF = record.valueSchema().fields().get(1).schema().fields().get(5).schema();
        assertThat(schemaA.isOptional()).isEqualTo(true);
        assertThat(schemaA.defaultValue()).isEqualTo(0L);
        assertThat(schemaB.isOptional()).isEqualTo(true);
        assertThat(schemaB.defaultValue()).isEqualTo(10L);
        assertThat(schemaC.isOptional()).isEqualTo(true);
        assertThat(schemaC.defaultValue()).isEqualTo(null);
        assertThat(schemaD.isOptional()).isEqualTo(false);
        assertThat(schemaE.isOptional()).isEqualTo(false);
        assertThat(schemaE.defaultValue()).isEqualTo(0L);
        assertThat(schemaF.isOptional()).isEqualTo(false);
        assertThat(schemaF.defaultValue()).isEqualTo(0L);
    }

    @Test
    public void unsignedBigIntToLongTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(MySqlConnector.class, config);

        Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("UNSIGNED_BIGINT_TABLE")).get(0);
        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        Schema schemaC = record.valueSchema().fields().get(1).schema().fields().get(2).schema();
        Schema schemaD = record.valueSchema().fields().get(1).schema().fields().get(3).schema();
        Schema schemaE = record.valueSchema().fields().get(1).schema().fields().get(4).schema();
        Schema schemaF = record.valueSchema().fields().get(1).schema().fields().get(5).schema();
        assertThat(schemaA.isOptional()).isEqualTo(true);
        assertThat(schemaA.defaultValue()).isEqualTo(0L);
        assertThat(schemaB.isOptional()).isEqualTo(true);
        assertThat(schemaB.defaultValue()).isEqualTo(10L);
        assertThat(schemaC.isOptional()).isEqualTo(true);
        assertThat(schemaC.defaultValue()).isEqualTo(null);
        assertThat(schemaD.isOptional()).isEqualTo(false);
        assertThat(schemaE.isOptional()).isEqualTo(false);
        assertThat(schemaE.defaultValue()).isEqualTo(0L);
        assertThat(schemaF.isOptional()).isEqualTo(false);
        assertThat(schemaF.defaultValue()).isEqualTo(0L);
    }

    @Test
    public void unsignedBigIntToBigDecimalTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE, JdbcValueConverters.BigIntUnsignedMode.PRECISE)
                .build();
        start(MySqlConnector.class, config);

        Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("UNSIGNED_BIGINT_TABLE")).get(0);
        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        Schema schemaC = record.valueSchema().fields().get(1).schema().fields().get(2).schema();
        Schema schemaD = record.valueSchema().fields().get(1).schema().fields().get(3).schema();
        Schema schemaE = record.valueSchema().fields().get(1).schema().fields().get(4).schema();
        Schema schemaF = record.valueSchema().fields().get(1).schema().fields().get(5).schema();
        assertThat(schemaA.isOptional()).isEqualTo(true);
        assertThat(schemaA.defaultValue()).isEqualTo(BigDecimal.ZERO);
        assertThat(schemaB.isOptional()).isEqualTo(true);
        assertThat(schemaB.defaultValue()).isEqualTo(new BigDecimal(10));
        assertThat(schemaC.isOptional()).isEqualTo(true);
        assertThat(schemaC.defaultValue()).isEqualTo(null);
        assertThat(schemaD.isOptional()).isEqualTo(false);
        assertThat(schemaE.isOptional()).isEqualTo(false);
        assertThat(schemaE.defaultValue()).isEqualTo(BigDecimal.ZERO);
        assertThat(schemaF.isOptional()).isEqualTo(false);
        assertThat(schemaF.defaultValue()).isEqualTo(BigDecimal.ZERO);
    }

    @Test
    public void stringTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(MySqlConnector.class, config);

        Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("STRING_TABLE")).get(0);
        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        Schema schemaC = record.valueSchema().fields().get(1).schema().fields().get(2).schema();
        Schema schemaD = record.valueSchema().fields().get(1).schema().fields().get(3).schema();
        Schema schemaE = record.valueSchema().fields().get(1).schema().fields().get(4).schema();
        Schema schemaF = record.valueSchema().fields().get(1).schema().fields().get(5).schema();
        Schema schemaG = record.valueSchema().fields().get(1).schema().fields().get(6).schema();
        Schema schemaH = record.valueSchema().fields().get(1).schema().fields().get(7).schema();
        assertThat(schemaA.defaultValue()).isEqualTo("A");
        assertThat(schemaB.defaultValue()).isEqualTo("b");
        assertThat(schemaC.defaultValue()).isEqualTo("CC");
        assertThat(schemaD.defaultValue()).isEqualTo("10");
        assertThat(schemaE.defaultValue()).isEqualTo("0");
        assertThat(schemaF.defaultValue()).isEqualTo(null);
        assertThat(schemaG.defaultValue()).isEqualTo(null);
        assertThat(schemaH.defaultValue()).isEqualTo(null);
    }

    @Test
    public void unsignedBitTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(MySqlConnector.class, config);

        Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("BIT_TABLE")).get(0);
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
        assertThat(schemaA.defaultValue()).isEqualTo(null);
        assertThat(schemaB.defaultValue()).isEqualTo(false);
        assertThat(schemaC.defaultValue()).isEqualTo(true);
        assertThat(schemaD.defaultValue()).isEqualTo(false);
        assertThat(schemaE.defaultValue()).isEqualTo(true);
        assertThat(schemaF.defaultValue()).isEqualTo(true);
        assertThat(schemaG.defaultValue()).isEqualTo(false);
        assertThat(schemaH.defaultValue()).isEqualTo(new byte[] {66, 1});
        assertThat(schemaI.defaultValue()).isEqualTo(null);
        assertThat(schemaJ.defaultValue()).isEqualTo(new byte[] {15, 97, 1, 0});
    }

    @Test
    public void booleanTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(MySqlConnector.class, config);

        Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("BOOLEAN_TABLE")).get(0);
        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        Schema schemaC = record.valueSchema().fields().get(1).schema().fields().get(2).schema();
        Schema schemaD = record.valueSchema().fields().get(1).schema().fields().get(3).schema();
        Schema schemaE = record.valueSchema().fields().get(1).schema().fields().get(4).schema();
        assertThat(schemaA.defaultValue()).isEqualTo((short) 0);
        assertThat(schemaB.defaultValue()).isEqualTo((short) 1);
        assertThat(schemaC.defaultValue()).isEqualTo((short) 1);
        assertThat(schemaD.defaultValue()).isEqualTo((short) 1);
        assertThat(schemaE.defaultValue()).isEqualTo(null);
    }

    @Test
    public void numberTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(MySqlConnector.class, config);

        Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("NUMBER_TABLE")).get(0);
        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        Schema schemaC = record.valueSchema().fields().get(1).schema().fields().get(2).schema();
        Schema schemaD = record.valueSchema().fields().get(1).schema().fields().get(3).schema();
        Schema schemaE = record.valueSchema().fields().get(1).schema().fields().get(4).schema();
        assertThat(schemaA.defaultValue()).isEqualTo((short) 10);
        assertThat(schemaB.defaultValue()).isEqualTo((short) 5);
        assertThat(schemaC.defaultValue()).isEqualTo(0);
        assertThat(schemaD.defaultValue()).isEqualTo(20L);
        assertThat(schemaE.defaultValue()).isEqualTo(null);
    }

    @Test
    public void floatAndDoubleTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(MySqlConnector.class, config);

        Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("FlOAT_DOUBLE_TABLE")).get(0);
        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        assertThat(schemaA.defaultValue()).isEqualTo(0d);
        assertThat(schemaB.defaultValue()).isEqualTo(1.0d);
    }

    @Test
    public void realTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(MySqlConnector.class, config);

        Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("REAL_TABLE")).get(0);
        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        assertThat(schemaA.defaultValue()).isEqualTo(1d);
        assertThat(schemaB.defaultValue()).isEqualTo(null);
    }

    @Test
    public void numericAndDecimalToDoubleTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.DECIMAL_HANDLING_MODE, JdbcValueConverters.DecimalMode.DOUBLE)
                .build();
        start(MySqlConnector.class, config);

        Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("NUMERIC_DECIMAL_TABLE")).get(0);
        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        Schema schemaC = record.valueSchema().fields().get(1).schema().fields().get(2).schema();
        assertThat(schemaA.defaultValue()).isEqualTo(1.23d);
        assertThat(schemaB.defaultValue()).isEqualTo(2.321d);
        assertThat(schemaC.defaultValue()).isEqualTo(12.678d);
    }

    @Test
    public void numericAndDecimalToDecimalTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.DECIMAL_HANDLING_MODE, JdbcValueConverters.DecimalMode.PRECISE)
                .build();
        start(MySqlConnector.class, config);

        Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("NUMERIC_DECIMAL_TABLE")).get(0);
        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        assertThat(schemaA.defaultValue()).isEqualTo(BigDecimal.valueOf(1.23));
        assertThat(schemaB.defaultValue()).isEqualTo(BigDecimal.valueOf(2.321));
    }

    @Test
    public void dateAndTimeTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.TABLE_WHITELIST, DATABASE.qualifiedTableName("DATE_TIME_TABLE"))
                .build();
        start(MySqlConnector.class, config);

        Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(7);
        final SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("DATE_TIME_TABLE")).get(0);
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
//         Number of days since epoch for date 1976-08-23
        assertThat(schemaA.defaultValue()).isEqualTo(2426);

        String value1 = "1970-01-01 00:00:01";
        ZonedDateTime t = java.sql.Timestamp.valueOf(value1).toInstant().atZone(ZoneId.systemDefault());
        String isoString = ZonedTimestamp.toIsoString(t, ZoneId.systemDefault(), MySqlValueConverters::adjustTemporal);
        assertThat(schemaB.defaultValue()).isEqualTo(isoString);

        String value2 = "2018-01-03 00:00:10";
        long toEpochMillis1 = Timestamp.toEpochMillis(LocalDateTime.from(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").parse(value2)), MySqlValueConverters::adjustTemporal);
        assertThat(schemaC.defaultValue()).isEqualTo(toEpochMillis1);

        String value3 = "2018-01-03 00:00:10.7";
        long toEpochMillis2 = Timestamp.toEpochMillis(LocalDateTime.from(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S").parse(value3)), MySqlValueConverters::adjustTemporal);
        assertThat(schemaD.defaultValue()).isEqualTo(toEpochMillis2);

        String value4 = "2018-01-03 00:00:10.123456";
        long toEpochMicro = MicroTimestamp.toEpochMicros(LocalDateTime.from(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").parse(value4)), MySqlValueConverters::adjustTemporal);
        assertThat(schemaE.defaultValue()).isEqualTo(toEpochMicro);

        assertThat(schemaF.defaultValue()).isEqualTo(2001);
        assertThat(schemaG.defaultValue()).isEqualTo(0L);
        assertThat(schemaH.defaultValue()).isEqualTo(82800700000L);
        assertThat(schemaI.defaultValue()).isEqualTo(82800123456L);

        //current timestamp will be replaced with epoch timestamp
        String value5 = "1970-01-01 00:00:00";
        ZonedDateTime t5 = java.sql.Timestamp.valueOf(value5).toInstant().atZone(ZoneId.systemDefault());
        String isoString5 = ZonedTimestamp.toIsoString(t5, ZoneId.systemDefault(), MySqlValueConverters::adjustTemporal);
        assertThat(schemaJ.defaultValue()).isEqualTo(isoString5);
    }

    @Test
    public void timeTypeWithAdaptiveMode() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.TABLE_WHITELIST, DATABASE.qualifiedTableName("DATE_TIME_TABLE"))
                .with(MySqlConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.ADAPTIVE)
                .build();
        start(MySqlConnector.class, config);

        Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(7);
        final SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("DATE_TIME_TABLE")).get(0);
        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        Schema schemaC = record.valueSchema().fields().get(1).schema().fields().get(2).schema();
        Schema schemaD = record.valueSchema().fields().get(1).schema().fields().get(3).schema();
        Schema schemaE = record.valueSchema().fields().get(1).schema().fields().get(4).schema();
        Schema schemaF = record.valueSchema().fields().get(1).schema().fields().get(5).schema();
        Schema schemaG = record.valueSchema().fields().get(1).schema().fields().get(6).schema();
        Schema schemaH = record.valueSchema().fields().get(1).schema().fields().get(7).schema();
        Schema schemaI = record.valueSchema().fields().get(1).schema().fields().get(8).schema();

        assertThat(schemaA.defaultValue()).isEqualTo(2426);

        String value1 = "1970-01-01 00:00:01";
        ZonedDateTime t = java.sql.Timestamp.valueOf(value1).toInstant().atZone(ZoneId.systemDefault());
        String isoString = ZonedTimestamp.toIsoString(t, ZoneId.systemDefault(), MySqlValueConverters::adjustTemporal);
        assertThat(schemaB.defaultValue()).isEqualTo(isoString);

        String value2 = "2018-01-03 00:00:10";
        long toEpochMillis1 = Timestamp.toEpochMillis(LocalDateTime.from(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").parse(value2)), MySqlValueConverters::adjustTemporal);
        assertThat(schemaC.defaultValue()).isEqualTo(toEpochMillis1);

        String value3 = "2018-01-03 00:00:10.7";
        long toEpochMillis2 = Timestamp.toEpochMillis(LocalDateTime.from(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S").parse(value3)), MySqlValueConverters::adjustTemporal);
        assertThat(schemaD.defaultValue()).isEqualTo(toEpochMillis2);

        String value4 = "2018-01-03 00:00:10.123456";
        long toEpochMicro = MicroTimestamp.toEpochMicros(LocalDateTime.from(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").parse(value4)), MySqlValueConverters::adjustTemporal);
        assertThat(schemaE.defaultValue()).isEqualTo(toEpochMicro);

        assertThat(schemaF.defaultValue()).isEqualTo(2001);
        assertThat(schemaG.defaultValue()).isEqualTo(0);
        assertThat(schemaH.defaultValue()).isEqualTo(82800700);
        assertThat(schemaI.defaultValue()).isEqualTo(82800123456L);
    }

    @Test
    public void timeTypeWithConnectMode() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.TABLE_WHITELIST, DATABASE.qualifiedTableName("DATE_TIME_TABLE"))
                .with(MySqlConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.CONNECT)
                .build();
        start(MySqlConnector.class, config);

        Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(7);
        final SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("DATE_TIME_TABLE")).get(0);
        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        Schema schemaC = record.valueSchema().fields().get(1).schema().fields().get(2).schema();
        Schema schemaD = record.valueSchema().fields().get(1).schema().fields().get(3).schema();
        Schema schemaE = record.valueSchema().fields().get(1).schema().fields().get(4).schema();
        Schema schemaF = record.valueSchema().fields().get(1).schema().fields().get(5).schema();
        Schema schemaG = record.valueSchema().fields().get(1).schema().fields().get(6).schema();
        Schema schemaH = record.valueSchema().fields().get(1).schema().fields().get(7).schema();
        Schema schemaI = record.valueSchema().fields().get(1).schema().fields().get(8).schema();

        TemporalAccessor accessor = DateTimeFormatter.ofPattern("yyyy-MM-dd").parse("1976-08-23");
        Instant instant = LocalDate.from(accessor).atStartOfDay().toInstant(ZoneOffset.UTC);
        assertThat(schemaA.defaultValue()).isEqualTo(java.util.Date.from(instant));

        String value1 = "1970-01-01 00:00:01";
        ZonedDateTime t = java.sql.Timestamp.valueOf(value1).toInstant().atZone(ZoneId.systemDefault());
        String isoString = ZonedTimestamp.toIsoString(t, ZoneId.systemDefault(), MySqlValueConverters::adjustTemporal);
        assertThat(schemaB.defaultValue()).isEqualTo(isoString);

        LocalDateTime localDateTimeC = LocalDateTime.from(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").parse("2018-01-03 00:00:10"));
        assertThat(schemaC.defaultValue()).isEqualTo(new java.util.Date(Timestamp.toEpochMillis(localDateTimeC, MySqlValueConverters::adjustTemporal)));

        LocalDateTime localDateTimeD = LocalDateTime.from(DateTimeFormatter
                .ofPattern("yyyy-MM-dd HH:mm:ss.S").parse("2018-01-03 00:00:10.7"));
        assertThat(schemaD.defaultValue()).isEqualTo(new java.util.Date(Timestamp.toEpochMillis(localDateTimeD, MySqlValueConverters::adjustTemporal)));

        LocalDateTime localDateTimeE = LocalDateTime.from(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").parse("2018-01-03 00:00:10.123456"));
        assertThat(schemaE.defaultValue()).isEqualTo(new java.util.Date(Timestamp.toEpochMillis(localDateTimeE, MySqlValueConverters::adjustTemporal)));

        assertThat(schemaF.defaultValue()).isEqualTo(2001);

        LocalTime localTime = Time.valueOf("00:00:00").toLocalTime();
        java.util.Date date = new java.util.Date(Timestamp.toEpochMillis(localTime, MySqlValueConverters::adjustTemporal));
        assertThat(schemaG.defaultValue()).isEqualTo(date);

        Duration duration1 = Duration.between(LocalTime.MIN, LocalTime.from(DateTimeFormatter.ofPattern("HH:mm:ss.S").parse("23:00:00.7")));
        assertThat(schemaH.defaultValue()).isEqualTo(new java.util.Date(io.debezium.time.Time.toMilliOfDay(duration1, MySqlValueConverters::adjustTemporal)));

        Duration duration2 = Duration.between(LocalTime.MIN, LocalTime.from(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS").parse("23:00:00.123456")));
        assertThat(schemaI.defaultValue()).isEqualTo(new java.util.Date(io.debezium.time.Time.toMilliOfDay(duration2, MySqlValueConverters::adjustTemporal)));
    }
}
