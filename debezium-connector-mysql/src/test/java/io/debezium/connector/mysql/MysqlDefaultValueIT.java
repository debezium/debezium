package io.debezium.connector.mysql;

import io.debezium.config.Configuration;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.util.Testing;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.file.Path;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author luobo
 */
public class MysqlDefaultValueIT extends AbstractConnectorTest {
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
    public void unsignedTinyintTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(MySqlConnector.class, config);

        Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(37);
        ConnectRecord record = records.recordsForTopic(DATABASE.topicForTable("UNSIGNED_TINYINT_TABLE")).get(0);
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
    public void unsignedSmallintTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(MySqlConnector.class, config);

        Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(37);
        ConnectRecord record = records.recordsForTopic(DATABASE.topicForTable("UNSIGNED_SMALLINT_TABLE")).get(0);
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
    public void unsignedMediumintTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(MySqlConnector.class, config);

        Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(37);
        ConnectRecord record = records.recordsForTopic(DATABASE.topicForTable("UNSIGNED_MEDIUMINT_TABLE")).get(0);
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

        SourceRecords records = consumeRecordsByTopic(37);
        ConnectRecord record = records.recordsForTopic(DATABASE.topicForTable("UNSIGNED_INT_TABLE")).get(0);
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

        SourceRecords records = consumeRecordsByTopic(37);
        ConnectRecord record = records.recordsForTopic(DATABASE.topicForTable("UNSIGNED_BIGINT_TABLE")).get(0);
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

        SourceRecords records = consumeRecordsByTopic(37);
        ConnectRecord record = records.recordsForTopic(DATABASE.topicForTable("UNSIGNED_BIGINT_TABLE")).get(0);
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

        SourceRecords records = consumeRecordsByTopic(37);
        ConnectRecord record = records.recordsForTopic(DATABASE.topicForTable("STRING_TABLE")).get(0);
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

//    @Test
//    public void unsignedBitTest() throws InterruptedException {
//        config = DATABASE.defaultConfig()
//                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
//                .build();
//        start(MySqlConnector.class, config);
//
//        Testing.Print.enable();
//
//        SourceRecords records = consumeRecordsByTopic(10);
//        ConnectRecord record = records.recordsForTopic(DATABASE.topicForTable("BIT_TABLE")).get(0);
//        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
//        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
//        Schema schemaC = record.valueSchema().fields().get(1).schema().fields().get(2).schema();
//        Schema schemaD = record.valueSchema().fields().get(1).schema().fields().get(3).schema();
//        Schema schemaE = record.valueSchema().fields().get(1).schema().fields().get(4).schema();
//        Schema schemaF = record.valueSchema().fields().get(1).schema().fields().get(5).schema();
//        Schema schemaG = record.valueSchema().fields().get(1).schema().fields().get(6).schema();
//        Schema schemaH = record.valueSchema().fields().get(1).schema().fields().get(7).schema();
//        Schema schemaI = record.valueSchema().fields().get(1).schema().fields().get(8).schema();
//        assertThat(schemaA.defaultValue()).isEqualTo(null);
//        assertThat(schemaB.defaultValue()).isEqualTo(false);
//        assertThat(schemaC.defaultValue()).isEqualTo(true);
//        assertThat(schemaD.defaultValue()).isEqualTo(false);
//        assertThat(schemaE.defaultValue()).isEqualTo(true);
//        assertThat(schemaF.defaultValue()).isEqualTo(true);
//        assertThat(schemaG.defaultValue()).isEqualTo(false);
//        assertThat(schemaH.defaultValue()).isEqualTo(new byte[] {1, 0});
//        assertThat(schemaI.defaultValue()).isEqualTo(null);
//    }

    @Test
    public void booleanTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(MySqlConnector.class, config);

        Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(37);
        ConnectRecord record = records.recordsForTopic(DATABASE.topicForTable("BOOLEAN_TABLE")).get(0);
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

        SourceRecords records = consumeRecordsByTopic(37);
        ConnectRecord record = records.recordsForTopic(DATABASE.topicForTable("NUMBER_TABLE")).get(0);
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

        SourceRecords records = consumeRecordsByTopic(37);
        ConnectRecord record = records.recordsForTopic(DATABASE.topicForTable("FlOAT_DOUBLE_TABLE")).get(0);
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

        SourceRecords records = consumeRecordsByTopic(37);
        ConnectRecord record = records.recordsForTopic(DATABASE.topicForTable("REAL_TABLE")).get(0);
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

        SourceRecords records = consumeRecordsByTopic(37);
        ConnectRecord record = records.recordsForTopic(DATABASE.topicForTable("NUMERIC_DECIMAL_TABLE")).get(0);
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

        SourceRecords records = consumeRecordsByTopic(37);
        ConnectRecord record = records.recordsForTopic(DATABASE.topicForTable("NUMERIC_DECIMAL_TABLE")).get(0);
        Schema schemaA = record.valueSchema().fields().get(1).schema().fields().get(0).schema();
        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        assertThat(schemaA.defaultValue()).isEqualTo(BigDecimal.valueOf(1.23));
        assertThat(schemaB.defaultValue()).isEqualTo(BigDecimal.valueOf(2.321));
    }
}
