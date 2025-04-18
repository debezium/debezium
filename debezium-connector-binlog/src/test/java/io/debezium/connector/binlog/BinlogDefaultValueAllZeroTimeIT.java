/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Collections;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;

/**
 * @author luobo on 2018/6/8 14:16
 */
public abstract class BinlogDefaultValueAllZeroTimeIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-connect.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("myServer1", "default_value_all_zero_time")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize(Collections.singletonMap("sessionVariables", "sql_mode=''"));
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    public void allZeroDateAndTimeTypeTest() throws InterruptedException {
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
}
