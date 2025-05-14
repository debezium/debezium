/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
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
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.jdbc.BinlogValueConverters;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.junit.EqualityCheck;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.junit.SkipWhenKafkaVersion;
import io.debezium.junit.SkipWhenKafkaVersion.KafkaVersion;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;

/**
 * @author luobo
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 6, reason = "DDL uses fractional second data types, not supported until MySQL 5.6")
public abstract class BinlogDefaultValueIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    // 4 meta events (set character_set etc.) and then 14 tables with 3 events each (drop DDL, create DDL, insert)
    private static final int EVENT_COUNT = 4 + 14 * 3;

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-connect.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("myServer1", "default_value")
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

        dropAllDatabases();
    }

    @Test
    @SkipWhenKafkaVersion(check = EqualityCheck.EQUAL, value = KafkaVersion.KAFKA_1XX, description = "Not compatible with Kafka 1.x")
    public void unsignedTinyIntTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(getConnectorClass(), config);

        // Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("UNSIGNED_TINYINT_TABLE")).get(0);
        validate(record);

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
        assertEmptyFieldValue(record, "G");
    }

    @Test
    @SkipWhenKafkaVersion(check = EqualityCheck.EQUAL, value = KafkaVersion.KAFKA_1XX, description = "Not compatible with Kafka 1.x")
    public void unsignedSmallIntTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(getConnectorClass(), config);

        // Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("UNSIGNED_SMALLINT_TABLE")).get(0);
        validate(record);

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
        assertEmptyFieldValue(record, "G");
    }

    private void assertEmptyFieldValue(SourceRecord record, String fieldName) {
        final Struct envelope = (Struct) record.value();
        final Struct after = (Struct) envelope.get("after");
        assertThat(after.getWithoutDefault(fieldName)).isNull();
    }

    @Test
    @SkipWhenKafkaVersion(check = EqualityCheck.EQUAL, value = KafkaVersion.KAFKA_1XX, description = "Not compatible with Kafka 1.x")
    public void unsignedMediumIntTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(getConnectorClass(), config);

        // Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("UNSIGNED_MEDIUMINT_TABLE")).get(0);
        validate(record);

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
        assertEmptyFieldValue(record, "G");
    }

    @Test
    @SkipWhenKafkaVersion(check = EqualityCheck.EQUAL, value = KafkaVersion.KAFKA_1XX, description = "Not compatible with Kafka 1.x")
    public void unsignedIntTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(getConnectorClass(), config);

        // Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("UNSIGNED_INT_TABLE")).get(0);
        validate(record);

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
        assertEmptyFieldValue(record, "G");
    }

    @Test
    @SkipWhenKafkaVersion(check = EqualityCheck.EQUAL, value = KafkaVersion.KAFKA_1XX, description = "Not compatible with Kafka 1.x")
    public void unsignedBigIntToLongTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(getConnectorClass(), config);

        // Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("UNSIGNED_BIGINT_TABLE")).get(0);
        validate(record);

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
        assertEmptyFieldValue(record, "G");
    }

    @Test
    @SkipWhenKafkaVersion(check = EqualityCheck.EQUAL, value = KafkaVersion.KAFKA_1XX, description = "Not compatible with Kafka 1.x")
    public void unsignedBigIntToBigDecimalTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE, JdbcValueConverters.BigIntUnsignedMode.PRECISE)
                .build();
        start(getConnectorClass(), config);

        // Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("UNSIGNED_BIGINT_TABLE")).get(0);

        validate(record);

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
        assertEmptyFieldValue(record, "G");
    }

    @Test
    @SkipWhenKafkaVersion(check = EqualityCheck.EQUAL, value = KafkaVersion.KAFKA_1XX, description = "Not compatible with Kafka 1.x")
    public void stringTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(getConnectorClass(), config);

        // Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("STRING_TABLE")).get(0);
        validate(record);

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
        assertEmptyFieldValue(record, "I");
    }

    @Test
    @SkipWhenKafkaVersion(check = EqualityCheck.EQUAL, value = KafkaVersion.KAFKA_1XX, description = "Not compatible with Kafka 1.x")
    public void schemaHistorySaveDefaultValuesTest() throws InterruptedException, SQLException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(getConnectorClass(), config);

        // Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("STRING_TABLE")).get(0);
        validate(record);

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
        assertEmptyFieldValue(record, "I");

        stopConnector();
        final String insert = "INSERT INTO STRING_TABLE\n"
                + "VALUES (DEFAULT ,DEFAULT ,DEFAULT ,DEFAULT ,DEFAULT ,DEFAULT ,DEFAULT ,DEFAULT, NULL)";
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                final Connection jdbc = connection.connection();
                final Statement statement = jdbc.createStatement();
                statement.executeUpdate(insert);
            }
        }
        start(getConnectorClass(), config);

        Print.enable();

        records = consumeRecordsByTopic(1);
        record = records.recordsForTopic(DATABASE.topicForTable("STRING_TABLE")).get(0);
        validate(record);

        schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        schemaC = record.valueSchema().fields().get(1).schema().fields().get(2).schema();
        schemaD = record.valueSchema().fields().get(1).schema().fields().get(3).schema();
        schemaE = record.valueSchema().fields().get(1).schema().fields().get(4).schema();
        schemaF = record.valueSchema().fields().get(1).schema().fields().get(5).schema();
        schemaG = record.valueSchema().fields().get(1).schema().fields().get(6).schema();
        schemaH = record.valueSchema().fields().get(1).schema().fields().get(7).schema();
        assertThat(schemaA.defaultValue()).isEqualTo("A");
        assertThat(schemaB.defaultValue()).isEqualTo("b");
        assertThat(schemaC.defaultValue()).isEqualTo("CC");
        assertThat(schemaD.defaultValue()).isEqualTo("10");
        assertThat(schemaE.defaultValue()).isEqualTo("0");
        assertThat(schemaF.defaultValue()).isEqualTo(null);
        assertThat(schemaG.defaultValue()).isEqualTo(null);
        assertThat(schemaH.defaultValue()).isEqualTo(null);
        assertEmptyFieldValue(record, "I");

    }

    @Test
    @SkipWhenKafkaVersion(check = EqualityCheck.EQUAL, value = KafkaVersion.KAFKA_1XX, description = "Not compatible with Kafka 1.x")
    public void unsignedBitTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(getConnectorClass(), config);

        // Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("BIT_TABLE")).get(0);
        validate(record);

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
        assertThat(schemaH.defaultValue()).isEqualTo(new byte[]{ 66, 1 });
        assertThat(schemaI.defaultValue()).isEqualTo(null);
        assertThat(schemaJ.defaultValue()).isEqualTo(new byte[]{ 15, 97, 1, 0 });
        assertEmptyFieldValue(record, "K");
    }

    @Test
    @SkipWhenKafkaVersion(check = EqualityCheck.EQUAL, value = KafkaVersion.KAFKA_1XX, description = "Not compatible with Kafka 1.x")
    public void booleanTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(getConnectorClass(), config);

        // Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("BOOLEAN_TABLE")).get(0);
        validate(record);

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
    @SkipWhenKafkaVersion(check = EqualityCheck.EQUAL, value = KafkaVersion.KAFKA_1XX, description = "Not compatible with Kafka 1.x")
    public void numberTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(getConnectorClass(), config);

        // Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("NUMBER_TABLE")).get(0);
        validate(record);

        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        Schema schemaC = record.valueSchema().fields().get(1).schema().fields().get(2).schema();
        Schema schemaD = record.valueSchema().fields().get(1).schema().fields().get(3).schema();
        Schema schemaE = record.valueSchema().fields().get(1).schema().fields().get(4).schema();
        Schema schemaG = record.valueSchema().fields().get(1).schema().fields().get(6).schema();
        Schema schemaH = record.valueSchema().fields().get(1).schema().fields().get(7).schema();

        assertThat(schemaA.defaultValue()).isEqualTo((short) 10);
        assertThat(schemaB.defaultValue()).isEqualTo((short) 5);
        assertThat(schemaC.defaultValue()).isEqualTo(0);
        assertThat(schemaD.defaultValue()).isEqualTo(20L);
        assertThat(schemaE.defaultValue()).isEqualTo(null);
        assertEmptyFieldValue(record, "F");
        assertThat(schemaG.defaultValue()).isEqualTo((short) 1);
        assertThat(schemaH.defaultValue()).isEqualTo((int) 1);
    }

    @Test
    @SkipWhenKafkaVersion(check = EqualityCheck.EQUAL, value = KafkaVersion.KAFKA_1XX, description = "Not compatible with Kafka 1.x")
    public void tinyIntBooleanTest() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(getConnectorClass(), config);

        // Testing.Print.enable();

        consumeRecordsByTopic(EVENT_COUNT);
        try (Connection conn = getTestDatabaseConnection(DATABASE.getDatabaseName()).connection()) {
            conn.createStatement().execute("CREATE TABLE ti_boolean_table (" +
                    "  A TINYINT(1) NOT NULL DEFAULT TRUE," +
                    "  B TINYINT(2) NOT NULL DEFAULT FALSE" +
                    ")");
            conn.createStatement().execute("INSERT INTO ti_boolean_table VALUES (default, default)");
        }

        SourceRecords records = consumeRecordsByTopic(2);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("ti_boolean_table")).get(0);
        validate(record);

        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        assertThat(schemaA.defaultValue()).isEqualTo((short) 1);
        assertThat(schemaB.defaultValue()).isEqualTo((short) 0);
    }

    @Test
    @FixFor("DBZ-1689")
    @SkipWhenKafkaVersion(check = EqualityCheck.EQUAL, value = KafkaVersion.KAFKA_1XX, description = "Not compatible with Kafka 1.x")
    public void intBooleanTest() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(getConnectorClass(), config);

        Print.enable();
        waitForSnapshotToBeCompleted(getConnectorName(), DATABASE.getServerName());
        consumeRecordsByTopic(EVENT_COUNT);
        try (Connection conn = getTestDatabaseConnection(DATABASE.getDatabaseName()).connection()) {
            conn.createStatement().execute("CREATE TABLE int_boolean_table (" +
                    "  A INT(1) NOT NULL DEFAULT TRUE," +
                    "  B INT(2) NOT NULL DEFAULT FALSE" +
                    ")");
            conn.createStatement().execute("INSERT INTO int_boolean_table VALUES (default, default)");
        }

        SourceRecords records = consumeRecordsByTopic(2);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("int_boolean_table")).get(0);
        validate(record);

        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        assertThat(schemaA.defaultValue()).isEqualTo((int) 1);
        assertThat(schemaB.defaultValue()).isEqualTo((int) 0);
    }

    @Test
    @SkipWhenKafkaVersion(check = EqualityCheck.EQUAL, value = KafkaVersion.KAFKA_1XX, description = "Not compatible with Kafka 1.x")
    public void floatAndDoubleTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(getConnectorClass(), config);

        // Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("FlOAT_DOUBLE_TABLE")).get(0);
        validate(record);

        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        assertThat(schemaA.defaultValue()).isEqualTo(0f);
        assertThat(schemaB.defaultValue()).isEqualTo(1.0d);
        assertEmptyFieldValue(record, "H");
    }

    @Test
    @SkipWhenKafkaVersion(check = EqualityCheck.EQUAL, value = KafkaVersion.KAFKA_1XX, description = "Not compatible with Kafka 1.x")
    public void realTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(getConnectorClass(), config);

        // Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("REAL_TABLE")).get(0);
        validate(record);

        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        assertThat(schemaA.defaultValue()).isEqualTo(1d);
        assertThat(schemaB.defaultValue()).isEqualTo(null);
        assertEmptyFieldValue(record, "C");
    }

    @Test
    public void numericAndDecimalToDoubleTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.DECIMAL_HANDLING_MODE, RelationalDatabaseConnectorConfig.DecimalHandlingMode.DOUBLE)
                .build();
        start(getConnectorClass(), config);

        // Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("NUMERIC_DECIMAL_TABLE")).get(0);
        validate(record);

        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        Schema schemaC = record.valueSchema().fields().get(1).schema().fields().get(2).schema();
        assertThat(schemaA.defaultValue()).isEqualTo(1.23d);
        assertThat(schemaB.defaultValue()).isEqualTo(2.321d);
        assertThat(schemaC.defaultValue()).isEqualTo(12.678d);
        assertEmptyFieldValue(record, "D");
    }

    @Test
    @SkipWhenKafkaVersion(check = EqualityCheck.EQUAL, value = KafkaVersion.KAFKA_1XX, description = "Not compatible with Kafka 1.x")
    public void numericAndDecimalToDecimalTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.DECIMAL_HANDLING_MODE, RelationalDatabaseConnectorConfig.DecimalHandlingMode.PRECISE)
                .build();
        start(getConnectorClass(), config);

        // Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("NUMERIC_DECIMAL_TABLE")).get(0);

        validate(record);

        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        assertThat(schemaA.defaultValue()).isEqualTo(BigDecimal.valueOf(1.23));
        assertThat(schemaB.defaultValue()).isEqualTo(BigDecimal.valueOf(2.321));
        assertEmptyFieldValue(record, "D");
    }

    @Test
    public void dateAndTimeTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("DATE_TIME_TABLE"))
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                .build();
        start(getConnectorClass(), config);

        // Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(7);
        final SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("DATE_TIME_TABLE")).get(0);
        validate(record);

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
        Schema schemaL = record.valueSchema().fields().get(1).schema().fields().get(11).schema();
        Schema schemaM = record.valueSchema().fields().get(1).schema().fields().get(12).schema();
        // Number of days since epoch for date 1976-08-23
        assertThat(schemaA.defaultValue()).isEqualTo(2426);

        String value1 = "1970-01-01 00:00:01";
        ZonedDateTime t = java.sql.Timestamp.valueOf(value1).toInstant().atZone(ZoneId.systemDefault());
        String isoString = getZonedDateTimeIsoString(t);
        assertThat(schemaB.defaultValue()).isEqualTo(isoString);

        String value2 = "2018-01-03 00:00:10";
        long toEpochMillis1 = Timestamp.toEpochMillis(LocalDateTime.from(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").parse(value2)),
                BinlogValueConverters::adjustTemporal);
        assertThat(schemaC.defaultValue()).isEqualTo(toEpochMillis1);

        String value3 = "2018-01-03 00:00:10.7";
        long toEpochMillis2 = Timestamp.toEpochMillis(LocalDateTime.from(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S").parse(value3)),
                BinlogValueConverters::adjustTemporal);
        assertThat(schemaD.defaultValue()).isEqualTo(toEpochMillis2);

        String value4 = "2018-01-03 00:00:10.123456";
        long toEpochMicro = MicroTimestamp.toEpochMicros(LocalDateTime.from(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").parse(value4)),
                BinlogValueConverters::adjustTemporal);
        assertThat(schemaE.defaultValue()).isEqualTo(toEpochMicro);

        assertThat(schemaF.defaultValue()).isEqualTo(2001);
        assertThat(schemaG.defaultValue()).isEqualTo(0L);
        assertThat(schemaH.defaultValue()).isEqualTo(82800700000L);
        assertThat(schemaI.defaultValue()).isEqualTo(82800123456L);

        assertThat(schemaL.defaultValue()).isEqualTo(Duration.ofHours(-23).minusMinutes(45).minusSeconds(56).minusMillis(700).toNanos() / 1_000);
        assertThat(schemaM.defaultValue()).isEqualTo(Duration.ofHours(123).plus(123456, ChronoUnit.MICROS).toNanos() / 1_000);
        // current timestamp will be replaced with epoch timestamp
        ZonedDateTime t5 = ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);
        String isoString5 = ZonedTimestamp.toIsoString(t5, ZoneOffset.UTC, BinlogValueConverters::adjustTemporal, null);
        assertThat(schemaJ.defaultValue()).isEqualTo(
                getTestDatabaseConnection(DATABASE.getDatabaseName())
                        .currentDateTimeDefaultOptional(isoString5));
        assertEmptyFieldValue(record, "K");
    }

    @Test
    @SkipWhenKafkaVersion(check = EqualityCheck.EQUAL, value = KafkaVersion.KAFKA_1XX, description = "Not compatible with Kafka 1.x")
    public void timeTypeWithConnectMode() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("DATE_TIME_TABLE"))
                .with(BinlogConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.CONNECT)
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                .build();
        start(getConnectorClass(), config);

        // Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(7);
        final SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("DATE_TIME_TABLE")).get(0);

        validate(record);

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
        String isoString = getZonedDateTimeIsoString(t);
        assertThat(schemaB.defaultValue()).isEqualTo(isoString);

        LocalDateTime localDateTimeC = LocalDateTime.from(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").parse("2018-01-03 00:00:10"));
        assertThat(schemaC.defaultValue()).isEqualTo(new java.util.Date(Timestamp.toEpochMillis(localDateTimeC, BinlogValueConverters::adjustTemporal)));

        LocalDateTime localDateTimeD = LocalDateTime.from(DateTimeFormatter
                .ofPattern("yyyy-MM-dd HH:mm:ss.S").parse("2018-01-03 00:00:10.7"));
        assertThat(schemaD.defaultValue()).isEqualTo(new java.util.Date(Timestamp.toEpochMillis(localDateTimeD, BinlogValueConverters::adjustTemporal)));

        LocalDateTime localDateTimeE = LocalDateTime.from(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").parse("2018-01-03 00:00:10.123456"));
        assertThat(schemaE.defaultValue()).isEqualTo(new java.util.Date(Timestamp.toEpochMillis(localDateTimeE, BinlogValueConverters::adjustTemporal)));

        assertThat(schemaF.defaultValue()).isEqualTo(2001);

        LocalTime localTime = Time.valueOf("00:00:00").toLocalTime();
        java.util.Date date = new java.util.Date(Timestamp.toEpochMillis(localTime, BinlogValueConverters::adjustTemporal));
        assertThat(schemaG.defaultValue()).isEqualTo(date);

        Duration duration1 = Duration.between(LocalTime.MIN, LocalTime.from(DateTimeFormatter.ofPattern("HH:mm:ss.S").parse("23:00:00.7")));
        assertThat(schemaH.defaultValue()).isEqualTo(new java.util.Date(io.debezium.time.Time.toMilliOfDay(duration1, false)));

        Duration duration2 = Duration.between(LocalTime.MIN, LocalTime.from(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS").parse("23:00:00.123456")));
        assertThat(schemaI.defaultValue()).isEqualTo(new java.util.Date(io.debezium.time.Time.toMilliOfDay(duration2, false)));
        assertEmptyFieldValue(record, "K");
    }

    @Test
    @FixFor("DBZ-7143")
    @SkipWhenKafkaVersion(check = EqualityCheck.EQUAL, value = KafkaVersion.KAFKA_1XX, description = "Not compatible with Kafka 1.x")
    public void timeTypeWithConnectModeWhenEventConvertingFail() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("DATE_TIME_TABLE_CONNECT_FAIL"))
                .with(BinlogConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.CONNECT)
                .with(CommonConnectorConfig.EVENT_CONVERTING_FAILURE_HANDLING_MODE, CommonConnectorConfig.EventConvertingFailureHandlingMode.FAIL)
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                .build();

        AtomicReference<Throwable> exception = new AtomicReference<>();
        start(getConnectorClass(), config, (success, message, error) -> exception.set(error));

        waitForSnapshotToBeCompleted(getConnectorName(), DATABASE.getServerName());

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                // CONNECT mode should be in range from 00:00:00 to 24:00:00
                // it throws exception by FAIL mode when parse DDL because default value is invalid in CONNECT mode.
                connection.execute("CREATE TABLE DATE_TIME_TABLE_CONNECT_FAIL (A TIME(1) DEFAULT '-23:45:56.7', B TIME(6) DEFAULT '123:00:00.123456');");
            }
        }
        // Testing.Print.enable();

        waitForConnectorShutdown(getConnectorName(), DATABASE.getServerName());

        final Throwable e = exception.get();
        if (e == null) {
            // it should be thrown
            fail();
        }
    }

    @Test
    @FixFor("DBZ-771")
    @SkipWhenKafkaVersion(check = EqualityCheck.EQUAL, value = KafkaVersion.KAFKA_1XX, description = "Not compatible with Kafka 1.x")
    public void columnTypeAndDefaultValueChange() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(getConnectorClass(), config);

        // Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);

        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("DBZ_771_CUSTOMERS")).get(0);
        validate(record);

        Schema customerTypeSchema = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        assertThat(customerTypeSchema.defaultValue()).isEqualTo("b2c");

        // Connect to the DB and issue our insert statement to test.
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                // Enable Query log option
                db.setBinlogRowQueryEventsOn();

                connection.execute("alter table DBZ_771_CUSTOMERS change customer_type customer_type int default 42;");
                connection.execute("insert into DBZ_771_CUSTOMERS (id) values (2);");
            }
        }

        // consume the records for the two executed statements
        records = consumeRecordsByTopic(2);

        record = records.recordsForTopic(DATABASE.topicForTable("DBZ_771_CUSTOMERS")).get(0);
        validate(record);

        customerTypeSchema = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        assertThat(customerTypeSchema.defaultValue()).isEqualTo(42);
    }

    @Test
    @FixFor({ "DBZ-771", "DBZ-1321" })
    @SkipWhenKafkaVersion(check = EqualityCheck.EQUAL, value = KafkaVersion.KAFKA_1XX, description = "Not compatible with Kafka 1.x")
    public void columnTypeChangeResetsDefaultValue() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(getConnectorClass(), config);

        // Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);

        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("DBZ_771_CUSTOMERS")).get(0);
        validate(record);

        Schema customerTypeSchema = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        assertThat(customerTypeSchema.defaultValue()).isEqualTo("b2c");

        // Connect to the DB and issue our insert statement to test.
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                // Enable Query log option
                db.setBinlogRowQueryEventsOn();

                connection.execute("alter table DBZ_771_CUSTOMERS change customer_type customer_type int;");
                connection.execute("insert into DBZ_771_CUSTOMERS (id, customer_type) values (2, 456);");

                connection.execute("alter table DBZ_771_CUSTOMERS modify customer_type int null;");
                connection.execute("alter table DBZ_771_CUSTOMERS modify customer_type int not null;");
            }
        }

        // consume the records for the two executed statements
        records = consumeRecordsByTopic(4);

        record = records.recordsForTopic(DATABASE.topicForTable("DBZ_771_CUSTOMERS")).get(0);
        validate(record);

        customerTypeSchema = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        assertThat(customerTypeSchema.defaultValue()).isNull();
    }

    @Test
    @FixFor({ "DBZ-2267", "DBZ-6029" })
    public void alterDateAndTimeTest() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("ALTER_DATE_TIME"))
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .build();
        start(getConnectorClass(), config);

        waitForSnapshotToBeCompleted(getConnectorName(), DATABASE.getServerName());
        Print.enable();

        // Connect to the DB and issue our insert statement to test.
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("create table ALTER_DATE_TIME (ID int primary key);");
                connection.execute("alter table ALTER_DATE_TIME add column (CREATED timestamp not null default current_timestamp, C time not null default '08:00')");
                connection.execute("insert into ALTER_DATE_TIME values(1000, default, default);");
            }
        }

        final SourceRecords records = consumeRecordsByTopic(1);
        final SourceRecord record = records.allRecordsInOrder().get(0);

        validate(record);

        final Schema columnSchema = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        final Schema schemaC = record.valueSchema().fields().get(1).schema().fields().get(2).schema();
        assertThat(columnSchema.defaultValue()).isEqualTo("1970-01-01T00:00:00Z");
        assertThat(schemaC.defaultValue()).isEqualTo(28800000000L);
    }

    @Test
    @FixFor("DBZ-4822")
    public void shouldConvertDefaultBoolean2Number() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("DBZ_4822_DEFAULT_BOOLEAN"))
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.DECIMAL_HANDLING_MODE, RelationalDatabaseConnectorConfig.DecimalHandlingMode.STRING)
                .build();

        start(getConnectorClass(), config);
        waitForSnapshotToBeCompleted(getConnectorName(), DATABASE.getServerName());

        // Connect to the DB and issue our alter statement to test.
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                String addColumnDdl = "CREATE TABLE DBZ_4822_DEFAULT_BOOLEAN (\n"
                        + "ID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,\n"
                        + "C0 BIGINT NULL DEFAULT TRUE,\n"
                        + "C1 INT(1) NULL DEFAULT true,\n"
                        + "C2 BIT DEFAULT true,\n"
                        + "C3 TINYINT DEFAULT false,\n"
                        + "C4 FLOAT DEFAULT false,\n"
                        + "C5 REAL DEFAULT false\n,"
                        + "C6 DOUBLE DEFAULT false\n,"
                        + "C7 NUMERIC(38, 26) DEFAULT false,\n"
                        + "C8 DECIMAL(10, 2) DEFAULT false,\n"
                        + "C9 BIGINT DEFAULT false);";
                connection.execute(addColumnDdl);
                connection.execute("insert into DBZ_4822_DEFAULT_BOOLEAN (C0) values(1000);");
            }
        }

        SourceRecords records = consumeRecordsByTopic(100);
        assertThat(records).isNotNull();

        List<SourceRecord> events = records.recordsForTopic(DATABASE.topicForTable("DBZ_4822_DEFAULT_BOOLEAN"));
        assertThat(events).hasSize(1);
        SourceRecord record = events.get(0);
        Struct change = ((Struct) record.value()).getStruct("after");
        assertThat(change.get("C7")).isEqualTo("0.00000000000000000000000000");
        assertThat(change.get("C8")).isEqualTo("0.00");
        assertThat(change.get("C9")).isEqualTo(0L);
    }

    @Test
    @FixFor("DBZ-5241")
    public void shouldConvertDefaultWithCharacterSetIntroducer() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("DBZ_5241_DEFAULT_CS_INTRO"))
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .build();

        start(getConnectorClass(), config);
        waitForSnapshotToBeCompleted(getConnectorName(), DATABASE.getServerName());

        // Connect to the DB and create our table and insert a value
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                final String createTable = "CREATE TABLE DBZ_5241_DEFAULT_CS_INTRO (\n"
                        + "ID INT NOT NULL AUTO_INCREMENT PRIMARY KEY, \n"
                        + "C0 BIGINT NOT NULL, \n"
                        + "C1 TINYINT(4) UNSIGNED DEFAULT 0, \n"
                        + "C2 TINYINT(4) UNSIGNED DEFAULT _UTF8MB4'0' COMMENT 'c2', \n"
                        + "C3 TINYINT(1) NOT NULL DEFAULT _UTF8MB4'0' COMMENT 'c3', \n"
                        + "C4 VARCHAR(25) DEFAULT _utf8'abc', \n"
                        + "C5 VARCHAR(25) NOT NULL DEFAULT _utf8'abc', \n"
                        + "C6 CHAR(25) DEFAULT _utf8'abc', \n"
                        + "C7 CHAR(25) NOT NULL DEFAULT _utf8'abc', \n"
                        + "C8 BIGINT UNSIGNED DEFAULT _UTF8MB4'0', \n"
                        + "C9 BIGINT UNSIGNED NOT NULL DEFAULT _UTF8MB4'0', \n"
                        + "C10 INT DEFAULT _utf8'0', \n"
                        + "C11 INT NOT NULL DEFAULT _utf8'0', \n"
                        + "C12 MEDIUMINT DEFAULT _utf8'0', \n"
                        + "C13 MEDIUMINT NOT NULL DEFAULT _utf8'0', \n"
                        + "C14 SMALLINT DEFAULT _utf8'0', \n"
                        + "C15 SMALLINT NOT NULL DEFAULT _utf8'0', \n"
                        + "C16 NUMERIC(3, 2) DEFAULT _UTF8MB4'1.23', \n"
                        + "C17 NUMERIC(3, 2) NOT NULL DEFAULT _UTF8MB4'1.23', \n"
                        + "C18 REAL DEFAULT _UTF8MB4'3.14', \n"
                        + "C19 REAL NOT NULL DEFAULT _UTF8MB4'3.14');";
                connection.execute(createTable);
                connection.execute("INSERT INTO DBZ_5241_DEFAULT_CS_INTRO (C0) values (1);");
            }
        }

        SourceRecords records = consumeRecordsByTopic(100);

        List<SourceRecord> events = records.recordsForTopic(DATABASE.topicForTable("DBZ_5241_DEFAULT_CS_INTRO"));
        assertThat(events).hasSize(1);
        SourceRecord record = events.get(0);
        Struct change = ((Struct) record.value()).getStruct("after");
        assertFieldDefaultValue(change, "C1", (short) 0);
        assertFieldDefaultValue(change, "C2", (short) 0);
        assertFieldDefaultValue(change, "C3", (short) 0);
        assertFieldDefaultValue(change, "C4", "abc");
        assertFieldDefaultValue(change, "C5", "abc");
        assertFieldDefaultValue(change, "C6", "abc");
        assertFieldDefaultValue(change, "C7", "abc");
        assertFieldDefaultValue(change, "C8", 0L);
        assertFieldDefaultValue(change, "C9", 0L);
        assertFieldDefaultValue(change, "C10", 0);
        assertFieldDefaultValue(change, "C11", 0);
        assertFieldDefaultValue(change, "C12", 0);
        assertFieldDefaultValue(change, "C13", 0);
        assertFieldDefaultValue(change, "C14", (short) 0);
        assertFieldDefaultValue(change, "C15", (short) 0);
        assertFieldDefaultValue(change, "C16", BigDecimal.valueOf(1.23));
        assertFieldDefaultValue(change, "C17", BigDecimal.valueOf(1.23));
        assertFieldDefaultValue(change, "C18", 3.14f);
        assertFieldDefaultValue(change, "C19", 3.14f);
    }

    private void assertFieldDefaultValue(Struct value, String fieldName, Object defaultValue) {
        assertThat(value.schema().field(fieldName).schema().defaultValue()).isEqualTo(defaultValue);
    }

    private String getZonedDateTimeIsoString(ZonedDateTime zdt) {
        return ZonedTimestamp.toIsoString(zdt, ZoneId.systemDefault(), BinlogValueConverters::adjustTemporal, null);
    }

}
