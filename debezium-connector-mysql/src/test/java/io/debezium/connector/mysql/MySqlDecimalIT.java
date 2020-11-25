/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.EqualityCheck;
import io.debezium.junit.SkipWhenKafkaVersion;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Testing;

/**
 * Verify correct DECIMAL handling with different types of io.debezium.relational.RelationalDatabaseConnectorConfig.DecimalHandlingMode.
 *
 * @author René Kerner
 */
public class MySqlDecimalIT extends AbstractConnectorTest {

    private static final String TABLE_NAME = "DBZ730";

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-decimal.txt")
            .toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("decimaldb", "decimal_test")
            .withDbHistoryPath(DB_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(DB_HISTORY_PATH);
        skipAvroValidation(); // https://github.com/confluentinc/schema-registry/issues/1693
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("DBZ-730")
    @SkipWhenKafkaVersion(value = SkipWhenKafkaVersion.KafkaVersion.KAFKA_1XX, check = EqualityCheck.EQUAL, description = "No compatible with Kafka 1.x")
    public void testPreciseDecimalHandlingMode() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(TABLE_NAME))
                .with(MySqlConnectorConfig.DECIMAL_HANDLING_MODE, RelationalDatabaseConnectorConfig.DecimalHandlingMode.PRECISE)
                .build();

        start(MySqlConnector.class, config);

        assertBigDecimalChangeRecord(consumeInsert());

        stopConnector();
    }

    @Test
    @FixFor("DBZ-730")
    public void testDoubleDecimalHandlingMode() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(TABLE_NAME))
                .with(MySqlConnectorConfig.DECIMAL_HANDLING_MODE, RelationalDatabaseConnectorConfig.DecimalHandlingMode.DOUBLE)
                .build();

        start(MySqlConnector.class, config);

        assertDoubleChangeRecord(consumeInsert());

        stopConnector();
    }

    @Test
    @FixFor("DBZ-730")
    public void testStringDecimalHandlingMode() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(TABLE_NAME))
                .with(MySqlConnectorConfig.DECIMAL_HANDLING_MODE, RelationalDatabaseConnectorConfig.DecimalHandlingMode.STRING)
                .build();

        start(MySqlConnector.class, config);

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
        Assertions.assertThat(record).isNotNull();
        final Struct change = ((Struct) record.value()).getStruct("after");

        Assertions.assertThat(change.get("A")).isEqualTo(new BigDecimal("1.33"));
        Assertions.assertThat(change.get("B")).isEqualTo(new BigDecimal("-2.111"));
        Assertions.assertThat(change.get("C")).isEqualTo(new BigDecimal("3.44400"));
        Assertions.assertThat(change.getWithoutDefault("D")).isNull();

        Assertions.assertThat(record.valueSchema().field("after").schema().field("D").schema().defaultValue())
                .isEqualTo(new BigDecimal("15.28000"));
    }

    private void assertDoubleChangeRecord(SourceRecord record) {
        Assertions.assertThat(record).isNotNull();
        final Struct change = ((Struct) record.value()).getStruct("after");

        Assertions.assertThat(change.getFloat64("A")).isEqualTo(1.33);
        Assertions.assertThat(change.getFloat64("B")).isEqualTo(-2.111);
        Assertions.assertThat(change.getFloat64("C")).isEqualTo(3.44400);
        Assertions.assertThat(change.getFloat64("D")).isNull();

        Assertions.assertThat(record.valueSchema().field("after").schema().field("D").schema().defaultValue())
                .isEqualTo(15.28000);
    }

    private void assertStringChangeRecord(SourceRecord record) {
        Assertions.assertThat(record).isNotNull();
        final Struct change = ((Struct) record.value()).getStruct("after");

        Assertions.assertThat(change.getString("A").trim()).isEqualTo("1.33");
        Assertions.assertThat(change.getString("B").trim()).isEqualTo("-2.111");
        Assertions.assertThat(change.getString("C").trim()).isEqualTo("3.44400");
        Assertions.assertThat(change.getString("D")).isNull();

        Assertions.assertThat(record.valueSchema().field("after").schema().field("D").schema().defaultValue())
                .isEqualTo("15.28000");
    }
}
