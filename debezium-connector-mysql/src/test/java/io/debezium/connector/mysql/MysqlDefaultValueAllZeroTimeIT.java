/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;

import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.Testing;

/**
 * @author luobo on 2018/6/8 14:16
 */
public class MysqlDefaultValueAllZeroTimeIT extends AbstractConnectorTest {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-connect.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("myServer1", "default_value_all_zero_time")
            .withDbHistoryPath(DB_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize(Collections.singletonMap("sessionVariables", "sql_mode=''"));
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
    public void allZeroDateAndTimeTypeTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.TABLE_WHITELIST, DATABASE.qualifiedTableName("ALL_ZERO_DATE_AND_TIME_TABLE"))
                .build();
        start(MySqlConnector.class, config);

        // Testing.Print.enable();

        AbstractConnectorTest.SourceRecords records = consumeRecordsByTopic(7);
        final SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("ALL_ZERO_DATE_AND_TIME_TABLE")).get(0);

        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        Schema schemaC = record.valueSchema().fields().get(1).schema().fields().get(2).schema();
        Schema schemaD = record.valueSchema().fields().get(1).schema().fields().get(3).schema();
        Schema schemaE = record.valueSchema().fields().get(1).schema().fields().get(4).schema();
        Schema schemaF = record.valueSchema().fields().get(1).schema().fields().get(5).schema();

        //column A, 0000-00-00 00:00:00 => 1970-01-01 00:00:00
        ZonedDateTime a = ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);
        String isoStringA = ZonedTimestamp.toIsoString(a, ZoneOffset.UTC, MySqlValueConverters::adjustTemporal);
        assertThat(schemaA.defaultValue()).isEqualTo(isoStringA);

        //column B allows null, default value should be null
        assertThat(schemaB.isOptional()).isEqualTo(true);
        assertThat(schemaB.defaultValue()).isEqualTo(null);

        //column C, 0000-00-00 => 1970-01-01
        assertThat(schemaC.defaultValue()).isEqualTo(0);

        //column D allows null, default value should be null
        assertThat(schemaD.isOptional()).isEqualTo(true);
        assertThat(schemaD.defaultValue()).isEqualTo(null);

        //column E, 0000-00-00 00:00:00 => 1970-01-01 00:00:00
        String valueE = "1970-01-01 00:00:00";
        long toEpochMillisE = Timestamp.toEpochMillis(LocalDateTime.from(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").parse(valueE)), MySqlValueConverters::adjustTemporal);
        assertThat(schemaE.defaultValue()).isEqualTo(toEpochMillisE);

        //column F allows null, default value should be null
        assertThat(schemaF.isOptional()).isEqualTo(true);
        assertThat(schemaF.defaultValue()).isEqualTo(null);

    }
}
