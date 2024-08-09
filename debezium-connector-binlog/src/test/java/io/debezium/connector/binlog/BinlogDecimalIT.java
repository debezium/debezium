/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;
import io.debezium.junit.EqualityCheck;
import io.debezium.junit.SkipWhenKafkaVersion;
import io.debezium.relational.RelationalDatabaseConnectorConfig.DecimalHandlingMode;

/**
 * Verify correct DECIMAL handling with different types of {@link DecimalHandlingMode}.
 *
 * @author René Kerner
 */
public abstract class BinlogDecimalIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final String TABLE_NAME = "DBZ730";

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-decimal.txt")
            .toAbsolutePath();
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("decimaldb", "decimal_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
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
    @FixFor("DBZ-730")
    @SkipWhenKafkaVersion(value = SkipWhenKafkaVersion.KafkaVersion.KAFKA_1XX, check = EqualityCheck.EQUAL, description = "No compatible with Kafka 1.x")
    public void testPreciseDecimalHandlingMode() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(TABLE_NAME))
                .with(BinlogConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.PRECISE)
                .build();

        start(getConnectorClass(), config);

        assertBigDecimalChangeRecord(consumeInsert());

        stopConnector();
    }

    @Test
    @FixFor("DBZ-730")
    public void testDoubleDecimalHandlingMode() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(TABLE_NAME))
                .with(BinlogConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.DOUBLE)
                .build();

        start(getConnectorClass(), config);

        assertDoubleChangeRecord(consumeInsert());

        stopConnector();
    }

    @Test
    @FixFor({ "DBZ-730", "DBZ-4730" })
    public void testStringDecimalHandlingMode() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(TABLE_NAME))
                .with(BinlogConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.STRING)
                .build();

        start(getConnectorClass(), config);

        assertStringChangeRecord(consumeInsert());

        stopConnector();
    }

    private SourceRecord consumeInsert() throws InterruptedException {
        final int numDatabase = 2;
        final int numTables = 4;
        final int numOthers = 1;

        SourceRecords records = consumeRecordsByTopic(numDatabase + numTables + numOthers);

        assertThat(records).isNotNull();

        List<SourceRecord> events = records.recordsForTopic(DATABASE.topicForTable(TABLE_NAME));
        assertThat(events).hasSize(1);

        return events.get(0);
    }

    private void assertBigDecimalChangeRecord(SourceRecord record) {
        assertThat(record).isNotNull();
        final Struct change = ((Struct) record.value()).getStruct("after");

        assertThat(change.get("A")).isEqualTo(new BigDecimal("1.33"));
        assertThat(change.get("B")).isEqualTo(new BigDecimal("-2.111"));
        assertThat(change.get("C")).isEqualTo(new BigDecimal("3.44400"));
        assertThat(change.getWithoutDefault("D")).isNull();
        assertThat(change.get("E")).isEqualTo(new BigDecimal("0.000000000000000000"));

        assertThat(record.valueSchema().field("after").schema().field("D").schema().defaultValue())
                .isEqualTo(new BigDecimal("15.28000"));
    }

    private void assertDoubleChangeRecord(SourceRecord record) {
        assertThat(record).isNotNull();
        final Struct change = ((Struct) record.value()).getStruct("after");

        assertThat(change.getFloat64("A")).isEqualTo(1.33);
        assertThat(change.getFloat64("B")).isEqualTo(-2.111);
        assertThat(change.getFloat64("C")).isEqualTo(3.44400);
        assertThat(change.getFloat64("D")).isNull();
        assertThat(change.getFloat64("E")).isEqualTo(0.000000000000000000);

        assertThat(record.valueSchema().field("after").schema().field("D").schema().defaultValue())
                .isEqualTo(15.28000);
    }

    private void assertStringChangeRecord(SourceRecord record) {
        assertThat(record).isNotNull();
        final Struct change = ((Struct) record.value()).getStruct("after");

        assertThat(change.getString("A").trim()).isEqualTo("1.33");
        assertThat(change.getString("B").trim()).isEqualTo("-2.111");
        assertThat(change.getString("C").trim()).isEqualTo("3.44400");
        assertThat(change.getString("D")).isNull();
        assertThat(change.getString("E")).isEqualTo("0.000000000000000000");

        assertThat(record.valueSchema().field("after").schema().field("D").schema().defaultValue())
                .isEqualTo("15.28000");
    }
}
