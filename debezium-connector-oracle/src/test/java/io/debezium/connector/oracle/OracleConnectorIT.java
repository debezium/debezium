/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static io.debezium.connector.oracle.util.TestHelper.TYPE_LENGTH_PARAMETER_KEY;
import static io.debezium.connector.oracle.util.TestHelper.TYPE_NAME_PARAMETER_KEY;
import static io.debezium.connector.oracle.util.TestHelper.TYPE_SCALE_PARAMETER_KEY;
import static junit.framework.TestCase.assertEquals;
import static org.fest.assertions.Assertions.assertThat;
import static org.fest.assertions.MapAssert.entry;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig.SnapshotMode;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIs;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.util.Testing;

/**
 * Integration test for the Debezium Oracle connector.
 *
 * @author Gunnar Morling
 */
public class OracleConnectorIT extends AbstractConnectorTest {

    private static final long MICROS_PER_SECOND = TimeUnit.SECONDS.toMicros(1);
    private static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";

    @Rule
    public final TestRule skipAdapterRule = new SkipTestDependingOnAdapterNameRule();

    private static OracleConnection connection;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        connection = TestHelper.testConnection();

        TestHelper.dropTable(connection, "debezium.customer");
        TestHelper.dropTable(connection, "debezium.masked_hashed_column_table");
        TestHelper.dropTable(connection, "debezium.truncated_column_table");
        TestHelper.dropTable(connection, "debezium.dt_table");

        String ddl = "create table debezium.customer (" +
                "  id numeric(9,0) not null, " +
                "  name varchar2(1000), " +
                "  score decimal(6, 2), " +
                "  registered timestamp, " +
                "  primary key (id)" +
                ")";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.customer to  " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE debezium.customer ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

        String ddl2 = "create table debezium.masked_hashed_column_table (" +
                "  id numeric(9,0) not null, " +
                "  name varchar2(255), " +
                "  name2 varchar2(255), " +
                "  name3 varchar2(20)," +
                "  primary key (id)" +
                ")";

        connection.execute(ddl2);
        connection.execute("GRANT SELECT ON debezium.masked_hashed_column_table to  " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE debezium.masked_hashed_column_table ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

        String ddl3 = "create table debezium.truncated_column_table (" +
                "  id numeric(9,0) not null, " +
                "  name varchar2(20), " +
                "  primary key (id)" +
                ")";

        connection.execute(ddl3);
        connection.execute("GRANT SELECT ON debezium.truncated_column_table to  " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE debezium.truncated_column_table ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

        String ddl4 = "create table dt_table (" +
                "  id numeric(9,0) not null, " +
                "  c1 int, " +
                "  c2 int, " +
                "  c3a numeric(5,2), " +
                "  c3b varchar(128), " +
                "  f1 float(10), " +
                "  f2 decimal(8,4), " +
                "  primary key (id)" +
                ")";

        connection.execute(ddl4);
        connection.execute("GRANT SELECT ON debezium.dt_table to  " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE debezium.dt_table ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
    }

    @AfterClass
    public static void closeConnection() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Before
    public void before() throws SQLException {
        connection.execute("delete from debezium.customer");
        connection.execute("delete from debezium.masked_hashed_column_table");
        connection.execute("delete from debezium.truncated_column_table");
        connection.execute("delete from debezium.dt_table");
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
    }

    @Test
    @FixFor("DBZ-2452")
    public void shouldSnapshotAndStreamWithHyphenedTableName() throws Exception {
        TestHelper.dropTable(connection, "debezium.\"my-table\"");

        String ddl = "create table \"my-table\" (" +
                " id numeric(9,0) not null, " +
                " c1 int, " +
                " c2 varchar(128), " +
                " primary key (id))";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.\"my-table\" to " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE debezium.\"my-table\" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        connection.execute("INSERT INTO debezium.\"my-table\" VALUES (1, 25, 'Test')");
        connection.execute("COMMIT");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.MY-TABLE")
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("INSERT INTO debezium.\"my-table\" VALUES (2, 50, 'Test2')");
        connection.execute("COMMIT");

        SourceRecords records = consumeRecordsByTopic(2);
        List<SourceRecord> hyphenatedTableRecords = records.recordsForTopic("server1.DEBEZIUM.my-table");
        assertThat(hyphenatedTableRecords).hasSize(2);

        // read
        SourceRecord record1 = hyphenatedTableRecords.get(0);
        VerifyRecord.isValidRead(record1, "ID", 1);
        Struct after1 = (Struct) ((Struct) record1.value()).get(Envelope.FieldName.AFTER);
        assertThat(after1.get("ID")).isEqualTo(1);
        assertThat(after1.get("C1")).isEqualTo(BigDecimal.valueOf(25L));
        assertThat(after1.get("C2")).isEqualTo("Test");
        assertThat(record1.sourceOffset().get(SourceInfo.SNAPSHOT_KEY)).isEqualTo(true);
        assertThat(record1.sourceOffset().get(SNAPSHOT_COMPLETED_KEY)).isEqualTo(true);

        // insert
        SourceRecord record2 = hyphenatedTableRecords.get(1);
        VerifyRecord.isValidInsert(record2, "ID", 2);
        Struct after2 = (Struct) ((Struct) record2.value()).get(Envelope.FieldName.AFTER);
        assertThat(after2.get("ID")).isEqualTo(2);
        assertThat(after2.get("C1")).isEqualTo(BigDecimal.valueOf(50L));
        assertThat(after2.get("C2")).isEqualTo("Test2");
    }

    @Test
    public void shouldTakeSnapshot() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .build();

        int expectedRecordCount = 0;
        connection.execute("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018/02/22', 'yyyy-mm-dd'))");
        connection.execute("INSERT INTO debezium.customer VALUES (2, 'Bruce', 2345.67, null)");
        connection.execute("COMMIT");
        expectedRecordCount += 2;

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(expectedRecordCount);
        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(testTableRecords).hasSize(expectedRecordCount);

        // read
        SourceRecord record1 = testTableRecords.get(0);
        VerifyRecord.isValidRead(record1, "ID", 1);
        Struct after = (Struct) ((Struct) record1.value()).get("after");
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("NAME")).isEqualTo("Billie-Bob");
        assertThat(after.get("SCORE")).isEqualTo(BigDecimal.valueOf(1234.56));
        assertThat(after.get("REGISTERED")).isEqualTo(toMicroSecondsSinceEpoch(LocalDateTime.of(2018, 2, 22, 0, 0, 0)));

        assertThat(record1.sourceOffset().get(SourceInfo.SNAPSHOT_KEY)).isEqualTo(true);
        assertThat(record1.sourceOffset().get(SNAPSHOT_COMPLETED_KEY)).isEqualTo(false);

        Struct source = (Struct) ((Struct) record1.value()).get("source");
        assertThat(source.get(SourceInfo.SNAPSHOT_KEY)).isEqualTo("true");

        SourceRecord record2 = testTableRecords.get(1);
        VerifyRecord.isValidRead(record2, "ID", 2);
        after = (Struct) ((Struct) record2.value()).get("after");
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("NAME")).isEqualTo("Bruce");
        assertThat(after.get("SCORE")).isEqualTo(BigDecimal.valueOf(2345.67));
        assertThat(after.get("REGISTERED")).isNull();

        assertThat(record2.sourceOffset().get(SourceInfo.SNAPSHOT_KEY)).isEqualTo(true);
        assertThat(record2.sourceOffset().get(SNAPSHOT_COMPLETED_KEY)).isEqualTo(true);

        source = (Struct) ((Struct) record2.value()).get("source");
        assertThat(source.get(SourceInfo.SNAPSHOT_KEY)).isEqualTo("last");
    }

    @Test
    public void shouldContinueWithStreamingAfterSnapshot() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .build();

        continueStreamingAfterSnapshot(config);
    }

    @Test
    @FixFor("DBZ-2607")
    @SkipWhenAdapterNameIs(value = SkipWhenAdapterNameIs.AdapterName.LOGMINER, reason = "Creates a backward compatibility regression")
    public void shouldNotRequireDatabaseSchemaConfiguration() throws Exception {
        final Map<String, ?> configMap = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .build()
                .asMap();
        configMap.remove(OracleConnectorConfig.SCHEMA_NAME.name());

        continueStreamingAfterSnapshot(Configuration.from(configMap));
    }

    private void continueStreamingAfterSnapshot(Configuration config) throws Exception {
        int expectedRecordCount = 0;
        connection.execute("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018/02/22', 'yyyy-mm-dd'))");
        connection.execute("INSERT INTO debezium.customer VALUES (2, 'Bruce', 2345.67, null)");
        connection.execute("COMMIT");
        expectedRecordCount += 2;

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(expectedRecordCount);
        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(testTableRecords).hasSize(expectedRecordCount);

        // read
        SourceRecord record1 = testTableRecords.get(0);
        VerifyRecord.isValidRead(record1, "ID", 1);
        Struct after = (Struct) ((Struct) record1.value()).get("after");
        assertThat(after.get("ID")).isEqualTo(1);

        Struct source = (Struct) ((Struct) record1.value()).get("source");
        assertThat(source.get(SourceInfo.SNAPSHOT_KEY)).isEqualTo("true");
        assertThat(source.get(SourceInfo.SCN_KEY)).isNotNull();
        assertThat(source.get(SourceInfo.SERVER_NAME_KEY)).isEqualTo("server1");
        assertThat(source.get(SourceInfo.DEBEZIUM_VERSION_KEY)).isNotNull();
        assertThat(source.get(SourceInfo.TXID_KEY)).isNull();
        assertThat(source.get(SourceInfo.TIMESTAMP_KEY)).isNotNull();

        assertThat(record1.sourceOffset().get(SourceInfo.SNAPSHOT_KEY)).isEqualTo(true);
        assertThat(record1.sourceOffset().get(SNAPSHOT_COMPLETED_KEY)).isEqualTo(false);

        SourceRecord record2 = testTableRecords.get(1);
        VerifyRecord.isValidRead(record2, "ID", 2);
        after = (Struct) ((Struct) record2.value()).get("after");
        assertThat(after.get("ID")).isEqualTo(2);

        assertThat(record2.sourceOffset().get(SourceInfo.SNAPSHOT_KEY)).isEqualTo(true);
        assertThat(record2.sourceOffset().get(SNAPSHOT_COMPLETED_KEY)).isEqualTo(true);

        expectedRecordCount = 0;
        connection.execute("INSERT INTO debezium.customer VALUES (3, 'Brian', 2345.67, null)");
        connection.execute("COMMIT");
        expectedRecordCount += 1;

        records = consumeRecordsByTopic(expectedRecordCount);
        testTableRecords = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(testTableRecords).hasSize(expectedRecordCount);

        SourceRecord record3 = testTableRecords.get(0);
        VerifyRecord.isValidInsert(record3, "ID", 3);
        after = (Struct) ((Struct) record3.value()).get("after");
        assertThat(after.get("ID")).isEqualTo(3);

        assertThat(record3.sourceOffset().containsKey(SourceInfo.SNAPSHOT_KEY)).isFalse();
        assertThat(record3.sourceOffset().containsKey(SNAPSHOT_COMPLETED_KEY)).isFalse();

        source = (Struct) ((Struct) record3.value()).get("source");
        assertThat(source.get(SourceInfo.SNAPSHOT_KEY)).isEqualTo("false");
        assertThat(source.get(SourceInfo.SCN_KEY)).isNotNull();
        assertThat(source.get(SourceInfo.SERVER_NAME_KEY)).isEqualTo("server1");
        assertThat(source.get(SourceInfo.DEBEZIUM_VERSION_KEY)).isNotNull();
        assertThat(source.get(SourceInfo.TXID_KEY)).isNotNull();
        assertThat(source.get(SourceInfo.TIMESTAMP_KEY)).isNotNull();
    }

    @Test
    @FixFor("DBZ-1223")
    @SkipWhenAdapterNameIs(value = SkipWhenAdapterNameIs.AdapterName.LOGMINER, reason = "sendTxBatch randomly fails")
    public void shouldStreamTransaction() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .build();

        // Testing.Print.enable();
        int expectedRecordCount = 0;
        connection.execute("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018/02/22', 'yyyy-mm-dd'))");
        connection.execute("INSERT INTO debezium.customer VALUES (2, 'Bruce', 2345.67, null)");
        connection.execute("COMMIT");
        expectedRecordCount += 2;

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(expectedRecordCount);
        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(testTableRecords).hasSize(expectedRecordCount);

        // read
        SourceRecord record1 = testTableRecords.get(0);
        VerifyRecord.isValidRead(record1, "ID", 1);
        Struct after = (Struct) ((Struct) record1.value()).get("after");
        assertThat(after.get("ID")).isEqualTo(1);

        Struct source = (Struct) ((Struct) record1.value()).get("source");
        assertThat(source.get(SourceInfo.SNAPSHOT_KEY)).isEqualTo("true");
        assertThat(source.get(SourceInfo.SCN_KEY)).isNotNull();
        assertThat(source.get(SourceInfo.SERVER_NAME_KEY)).isEqualTo("server1");
        assertThat(source.get(SourceInfo.DEBEZIUM_VERSION_KEY)).isNotNull();
        assertThat(source.get(SourceInfo.TXID_KEY)).isNull();
        assertThat(source.get(SourceInfo.TIMESTAMP_KEY)).isNotNull();

        assertThat(record1.sourceOffset().get(SourceInfo.SNAPSHOT_KEY)).isEqualTo(true);
        assertThat(record1.sourceOffset().get(SNAPSHOT_COMPLETED_KEY)).isEqualTo(false);

        SourceRecord record2 = testTableRecords.get(1);
        VerifyRecord.isValidRead(record2, "ID", 2);
        after = (Struct) ((Struct) record2.value()).get("after");
        assertThat(after.get("ID")).isEqualTo(2);

        assertThat(record2.sourceOffset().get(SourceInfo.SNAPSHOT_KEY)).isEqualTo(true);
        assertThat(record2.sourceOffset().get(SNAPSHOT_COMPLETED_KEY)).isEqualTo(true);

        expectedRecordCount = 30;
        connection.setAutoCommit(false);
        sendTxBatch(config, expectedRecordCount, 100);
        sendTxBatch(config, expectedRecordCount, 200);
    }

    private void sendTxBatch(Configuration config, int expectedRecordCount, int offset) throws SQLException, InterruptedException {
        boolean isAutoCommit = false;
        if (connection.connection().getAutoCommit()) {
            isAutoCommit = true;
            connection.connection().setAutoCommit(false);
        }
        for (int i = offset; i < expectedRecordCount + offset; i++) {
            connection.executeWithoutCommitting(String.format("INSERT INTO debezium.customer VALUES (%s, 'Brian%s', 2345.67, null)", i, i));
        }
        connection.connection().commit();
        if (isAutoCommit) {
            connection.connection().setAutoCommit(true);
        }

        assertTxBatch(config, expectedRecordCount, offset);
    }

    private void assertTxBatch(Configuration config, int expectedRecordCount, int offset) throws InterruptedException {
        SourceRecords records;
        List<SourceRecord> testTableRecords;
        Struct after;
        Struct source;
        records = consumeRecordsByTopic(expectedRecordCount);
        testTableRecords = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(testTableRecords).hasSize(expectedRecordCount);
        final String adapter = config.getString(OracleConnectorConfig.CONNECTOR_ADAPTER);

        for (int i = 0; i < expectedRecordCount; i++) {
            SourceRecord record3 = testTableRecords.get(i);
            VerifyRecord.isValidInsert(record3, "ID", i + offset);
            after = (Struct) ((Struct) record3.value()).get("after");
            assertThat(after.get("ID")).isEqualTo(i + offset);

            assertThat(record3.sourceOffset().containsKey(SourceInfo.SNAPSHOT_KEY)).isFalse();
            assertThat(record3.sourceOffset().containsKey(SNAPSHOT_COMPLETED_KEY)).isFalse();

            if (!"LogMiner".equalsIgnoreCase(adapter)) {
                assertThat(record3.sourceOffset().containsKey(SourceInfo.LCR_POSITION_KEY)).isTrue();
                assertThat(record3.sourceOffset().containsKey(SourceInfo.SCN_KEY)).isFalse();
            }

            source = (Struct) ((Struct) record3.value()).get("source");
            assertThat(source.get(SourceInfo.SNAPSHOT_KEY)).isEqualTo("false");
            assertThat(source.get(SourceInfo.SCN_KEY)).isNotNull();
            if (!"LogMiner".equalsIgnoreCase(adapter)) {
                assertThat(source.get(SourceInfo.LCR_POSITION_KEY)).isNotNull();
            }

            assertThat(source.get(SourceInfo.SERVER_NAME_KEY)).isEqualTo("server1");
            assertThat(source.get(SourceInfo.DEBEZIUM_VERSION_KEY)).isNotNull();
            assertThat(source.get(SourceInfo.TXID_KEY)).isNotNull();
            assertThat(source.get(SourceInfo.TIMESTAMP_KEY)).isNotNull();
        }
    }

    @Test
    @SkipWhenAdapterNameIs(value = SkipWhenAdapterNameIs.AdapterName.LOGMINER, reason = "Test randomly fails in sendTxBatch")
    public void shouldStreamAfterRestart() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .build();

        // Testing.Print.enable();
        int expectedRecordCount = 0;
        connection.execute("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018/02/22', 'yyyy-mm-dd'))");
        connection.execute("INSERT INTO debezium.customer VALUES (2, 'Bruce', 2345.67, null)");
        connection.execute("COMMIT");
        expectedRecordCount += 2;

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        SourceRecords records = consumeRecordsByTopic(expectedRecordCount);
        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(testTableRecords).hasSize(expectedRecordCount);

        expectedRecordCount = 30;
        connection.setAutoCommit(false);
        sendTxBatch(config, expectedRecordCount, 100);
        sendTxBatch(config, expectedRecordCount, 200);

        stopConnector();
        final int OFFSET = 300;
        for (int i = OFFSET; i < expectedRecordCount + OFFSET; i++) {
            connection.executeWithoutCommitting(String.format("INSERT INTO debezium.customer VALUES (%s, 'Brian%s', 2345.67, null)", i, i));
        }
        connection.connection().commit();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        assertTxBatch(config, expectedRecordCount, 300);
        sendTxBatch(config, expectedRecordCount, 400);
        sendTxBatch(config, expectedRecordCount, 500);
    }

    @Test
    public void shouldStreamAfterRestartAfterSnapshot() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .build();

        // Testing.Print.enable();
        int expectedRecordCount = 0;
        connection.execute("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018/02/22', 'yyyy-mm-dd'))");
        connection.execute("INSERT INTO debezium.customer VALUES (2, 'Bruce', 2345.67, null)");
        connection.execute("COMMIT");
        expectedRecordCount += 2;

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(expectedRecordCount);
        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(testTableRecords).hasSize(expectedRecordCount);

        stopConnector();

        connection.setAutoCommit(false);
        final int OFFSET = 100;
        for (int i = OFFSET; i < expectedRecordCount + OFFSET; i++) {
            connection.executeWithoutCommitting(String.format("INSERT INTO debezium.customer VALUES (%s, 'Brian%s', 2345.67, null)", i, i));
        }
        connection.connection().commit();

        connection.setAutoCommit(true);

        Testing.print("=== Starting connector second time ===");
        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        assertTxBatch(config, expectedRecordCount, 100);
        sendTxBatch(config, expectedRecordCount, 200);
    }

    @Test
    public void shouldReadChangeStreamForExistingTable() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        int expectedRecordCount = 0;
        connection.execute("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018/02/22', 'yyyy-mm-dd'))");
        connection.execute("COMMIT");

        expectedRecordCount += 1;

        connection.execute("UPDATE debezium.customer SET name = 'Bruce', score = 2345.67, registered = TO_DATE('2018/03/23', 'yyyy-mm-dd') WHERE id = 1");
        connection.execute("COMMIT");
        expectedRecordCount += 1;

        connection.execute("UPDATE debezium.customer SET id = 2 WHERE id = 1");
        connection.execute("COMMIT");
        expectedRecordCount += 3; // deletion + tombstone + insert with new id

        connection.execute("DELETE debezium.customer WHERE id = 2");
        connection.execute("COMMIT");
        expectedRecordCount += 2; // deletion + tombstone

        SourceRecords records = consumeRecordsByTopic(expectedRecordCount);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(testTableRecords).hasSize(expectedRecordCount);

        // insert
        VerifyRecord.isValidInsert(testTableRecords.get(0), "ID", 1);
        Struct after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("NAME")).isEqualTo("Billie-Bob");
        assertThat(after.get("SCORE")).isEqualTo(BigDecimal.valueOf(1234.56));
        assertThat(after.get("REGISTERED")).isEqualTo(toMicroSecondsSinceEpoch(LocalDateTime.of(2018, 2, 22, 0, 0, 0)));

        Map<String, ?> offset = testTableRecords.get(0).sourceOffset();
        assertThat(offset.get(SourceInfo.SNAPSHOT_KEY)).isNull();
        assertThat(offset.get("snapshot_completed")).isNull();

        // update
        VerifyRecord.isValidUpdate(testTableRecords.get(1), "ID", 1);
        Struct before = (Struct) ((Struct) testTableRecords.get(1).value()).get("before");
        assertThat(before.get("ID")).isEqualTo(1);
        assertThat(before.get("NAME")).isEqualTo("Billie-Bob");
        assertThat(before.get("SCORE")).isEqualTo(BigDecimal.valueOf(1234.56));
        assertThat(before.get("REGISTERED")).isEqualTo(toMicroSecondsSinceEpoch(LocalDateTime.of(2018, 2, 22, 0, 0, 0)));

        after = (Struct) ((Struct) testTableRecords.get(1).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("NAME")).isEqualTo("Bruce");
        assertThat(after.get("SCORE")).isEqualTo(BigDecimal.valueOf(2345.67));
        assertThat(after.get("REGISTERED")).isEqualTo(toMicroSecondsSinceEpoch(LocalDateTime.of(2018, 3, 23, 0, 0, 0)));

        // update of PK
        VerifyRecord.isValidDelete(testTableRecords.get(2), "ID", 1);
        before = (Struct) ((Struct) testTableRecords.get(2).value()).get("before");
        assertThat(before.get("ID")).isEqualTo(1);
        assertThat(before.get("NAME")).isEqualTo("Bruce");
        assertThat(before.get("SCORE")).isEqualTo(BigDecimal.valueOf(2345.67));
        assertThat(before.get("REGISTERED")).isEqualTo(toMicroSecondsSinceEpoch(LocalDateTime.of(2018, 3, 23, 0, 0, 0)));

        VerifyRecord.isValidTombstone(testTableRecords.get(3));

        VerifyRecord.isValidInsert(testTableRecords.get(4), "ID", 2);
        after = (Struct) ((Struct) testTableRecords.get(4).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("NAME")).isEqualTo("Bruce");
        assertThat(after.get("SCORE")).isEqualTo(BigDecimal.valueOf(2345.67));
        assertThat(after.get("REGISTERED")).isEqualTo(toMicroSecondsSinceEpoch(LocalDateTime.of(2018, 3, 23, 0, 0, 0)));

        // delete
        VerifyRecord.isValidDelete(testTableRecords.get(5), "ID", 2);
        before = (Struct) ((Struct) testTableRecords.get(5).value()).get("before");
        assertThat(before.get("ID")).isEqualTo(2);
        assertThat(before.get("NAME")).isEqualTo("Bruce");
        assertThat(before.get("SCORE")).isEqualTo(BigDecimal.valueOf(2345.67));
        assertThat(before.get("REGISTERED")).isEqualTo(toMicroSecondsSinceEpoch(LocalDateTime.of(2018, 3, 23, 0, 0, 0)));

        VerifyRecord.isValidTombstone(testTableRecords.get(6));
    }

    @Test
    @FixFor("DBZ-835")
    public void deleteWithoutTombstone() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(OracleConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        int expectedRecordCount = 0;
        connection.execute("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018/02/22', 'yyyy-mm-dd'))");
        connection.execute("COMMIT");
        expectedRecordCount += 1;

        connection.execute("DELETE debezium.customer WHERE id = 1");
        connection.execute("COMMIT");
        expectedRecordCount += 1; // deletion, no tombstone

        connection.execute("INSERT INTO debezium.customer VALUES (2, 'Billie-Bob', 1234.56, TO_DATE('2018/02/22', 'yyyy-mm-dd'))");
        connection.execute("COMMIT");
        expectedRecordCount += 1;

        SourceRecords records = consumeRecordsByTopic(expectedRecordCount);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(testTableRecords).hasSize(expectedRecordCount);

        // delete
        VerifyRecord.isValidDelete(testTableRecords.get(1), "ID", 1);
        final Struct before = ((Struct) testTableRecords.get(1).value()).getStruct("before");
        assertThat(before.get("ID")).isEqualTo(1);
        assertThat(before.get("NAME")).isEqualTo("Billie-Bob");
        assertThat(before.get("SCORE")).isEqualTo(BigDecimal.valueOf(1234.56));
        assertThat(before.get("REGISTERED")).isEqualTo(toMicroSecondsSinceEpoch(LocalDateTime.of(2018, 2, 22, 0, 0, 0)));

        VerifyRecord.isValidInsert(testTableRecords.get(2), "ID", 2);
    }

    @Test
    @SkipWhenAdapterNameIs(value = SkipWhenAdapterNameIs.AdapterName.LOGMINER, reason = "Seems to get caught in loop?")
    public void shouldReadChangeStreamForTableCreatedWhileStreaming() throws Exception {
        TestHelper.dropTable(connection, "debezium.customer2");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER2")
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        String ddl = "create table debezium.customer2 (" +
                "  id numeric(9,0) not null, " +
                "  name varchar2(1000), " +
                "  score decimal(6, 2), " +
                "  registered timestamp, " +
                "  primary key (id)" +
                ")";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.customer2 to " + TestHelper.getConnectorUserName());

        connection.execute("INSERT INTO debezium.customer2 VALUES (2, 'Billie-Bob', 1234.56, TO_DATE('2018/02/22', 'yyyy-mm-dd'))");
        connection.execute("COMMIT");

        SourceRecords records = consumeRecordsByTopic(1);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER2");
        assertThat(testTableRecords).hasSize(1);

        VerifyRecord.isValidInsert(testTableRecords.get(0), "ID", 2);
        Struct after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("NAME")).isEqualTo("Billie-Bob");
        assertThat(after.get("SCORE")).isEqualTo(BigDecimal.valueOf(1234.56));
        assertThat(after.get("REGISTERED")).isEqualTo(toMicroSecondsSinceEpoch(LocalDateTime.of(2018, 2, 22, 0, 0, 0)));
    }

    @Test
    @FixFor("DBZ-800")
    @SkipWhenAdapterNameIs(value = SkipWhenAdapterNameIs.AdapterName.LOGMINER, reason = "Log Miner does not yet support DDL during streaming")
    public void shouldReceiveHeartbeatAlsoWhenChangingTableIncludeListTables() throws Exception {
        TestHelper.dropTable(connection, "debezium.dbz800a");
        TestHelper.dropTable(connection, "debezium.dbz800b");

        // the low heartbeat interval should make sure that a heartbeat message is emitted after each change record
        // received from Postgres
        Configuration config = TestHelper.defaultConfig()
                .with(Heartbeat.HEARTBEAT_INTERVAL, "1")
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ800B")
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("CREATE TABLE debezium.dbz800a (id NUMBER(9) NOT NULL, aaa VARCHAR2(100), PRIMARY KEY (id) )");
        connection.execute("CREATE TABLE debezium.dbz800b (id NUMBER(9) NOT NULL, bbb VARCHAR2(100), PRIMARY KEY (id) )");
        connection.execute("INSERT INTO debezium.dbz800a VALUES (1, 'AAA')");
        connection.execute("INSERT INTO debezium.dbz800b VALUES (2, 'BBB')");
        connection.execute("COMMIT");

        // expecting two heartbeat records and one actual change record
        List<SourceRecord> records = consumeRecordsByTopic(3).allRecordsInOrder();

        // expecting no change record for s1.a but a heartbeat
        verifyHeartbeatRecord(records.get(0));

        // and then a change record for s1.b and a heartbeat
        verifyHeartbeatRecord(records.get(1));
        VerifyRecord.isValidInsert(records.get(2), "ID", 2);
    }

    @Test
    @FixFor("DBZ-775")
    public void shouldConsumeEventsWithMaskedAndTruncatedColumnsWithDatabaseName() throws Exception {
        shouldConsumeEventsWithMaskedAndTruncatedColumns(true);
    }

    @Test
    @FixFor("DBZ-775")
    public void shouldConsumeEventsWithMaskedAndTruncatedColumnsWithoutDatabaseName() throws Exception {
        shouldConsumeEventsWithMaskedAndTruncatedColumns(false);
    }

    public void shouldConsumeEventsWithMaskedAndTruncatedColumns(boolean useDatabaseName) throws Exception {
        final Configuration config;
        if (useDatabaseName) {
            config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_SCHEMA_ONLY)
                    .with("column.mask.with.12.chars", "ORCLPDB1.DEBEZIUM.MASKED_HASHED_COLUMN_TABLE.NAME")
                    .with("column.mask.hash.SHA-256.with.salt.CzQMA0cB5K",
                            "ORCLPDB1.DEBEZIUM.MASKED_HASHED_COLUMN_TABLE.NAME2,ORCLPDB1.DEBEZIUM.MASKED_HASHED_COLUMN_TABLE.NAME3")
                    .with("column.truncate.to.4.chars", "ORCLPDB1.DEBEZIUM.TRUNCATED_COLUMN_TABLE.NAME")
                    .build();
        }
        else {
            config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_SCHEMA_ONLY)
                    .with("column.mask.with.12.chars", "DEBEZIUM.MASKED_HASHED_COLUMN_TABLE.NAME")
                    .with("column.mask.hash.SHA-256.with.salt.CzQMA0cB5K", "DEBEZIUM.MASKED_HASHED_COLUMN_TABLE.NAME2,DEBEZIUM.MASKED_HASHED_COLUMN_TABLE.NAME3")
                    .with("column.truncate.to.4.chars", "DEBEZIUM.TRUNCATED_COLUMN_TABLE.NAME")
                    .build();
        }

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("INSERT INTO debezium.masked_hashed_column_table (id, name, name2, name3) VALUES (10, 'some_name', 'test', 'test')");
        connection.execute("INSERT INTO debezium.truncated_column_table VALUES(11, 'some_name')");
        connection.execute("COMMIT");

        final SourceRecords records = consumeRecordsByTopic(2);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.DEBEZIUM.MASKED_HASHED_COLUMN_TABLE");
        final List<SourceRecord> tableB = records.recordsForTopic("server1.DEBEZIUM.TRUNCATED_COLUMN_TABLE");

        assertThat(tableA).hasSize(1);
        SourceRecord record = tableA.get(0);
        VerifyRecord.isValidInsert(record, "ID", 10);

        Struct value = (Struct) record.value();
        if (value.getStruct("after") != null) {
            Struct after = value.getStruct("after");
            assertThat(after.getString("NAME")).isEqualTo("************");
            assertThat(after.getString("NAME2")).isEqualTo("8e68c68edbbac316dfe2f6ada6b0d2d3e2002b487a985d4b7c7c82dd83b0f4d7");
            assertThat(after.getString("NAME3")).isEqualTo("8e68c68edbbac316dfe2");
        }

        assertThat(tableB).hasSize(1);
        record = tableB.get(0);
        VerifyRecord.isValidInsert(record, "ID", 11);

        value = (Struct) record.value();
        if (value.getStruct("after") != null) {
            assertThat(value.getStruct("after").getString("NAME")).isEqualTo("some");
        }

        stopConnector();
    }

    @Test
    @FixFor("DBZ-775")
    public void shouldRewriteIdentityKeyWithDatabaseName() throws Exception {
        shouldRewriteIdentityKey(true);
    }

    @Test
    @FixFor("DBZ-775")
    public void shouldRewriteIdentityKeyWithoutDatabaseName() throws Exception {
        shouldRewriteIdentityKey(false);
    }

    private void shouldRewriteIdentityKey(boolean useDatabaseName) throws Exception {
        final Configuration config;
        if (useDatabaseName) {
            config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_SCHEMA_ONLY)
                    .with(OracleConnectorConfig.MSG_KEY_COLUMNS, "(.*).debezium.customer:id,name")
                    .build();
        }
        else {
            config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_SCHEMA_ONLY)
                    .with(OracleConnectorConfig.MSG_KEY_COLUMNS, "debezium.customer:id,name")
                    .build();
        }

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("INSERT INTO debezium.customer VALUES (3, 'Nest', 1234.56, TO_DATE('2018/02/22', 'yyyy-mm-dd'))");
        connection.execute("COMMIT");

        SourceRecords records = consumeRecordsByTopic(1);
        List<SourceRecord> recordsForTopic = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(recordsForTopic.get(0).key()).isNotNull();
        Struct key = (Struct) recordsForTopic.get(0).key();
        assertThat(key.get("ID")).isNotNull();
        assertThat(key.get("NAME")).isNotNull();

        stopConnector();
    }

    @Test
    @FixFor({ "DBZ-1916", "DBZ-1830" })
    public void shouldPropagateSourceTypeByDatatype() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with("datatype.propagate.source.type", ".+\\.NUMBER,.+\\.VARCHAR2,.+\\.FLOAT")
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("INSERT INTO debezium.dt_table (id,c1,c2,c3a,c3b,f1,f2) values (1,123,456,789.01,'test',1.228,234.56)");
        connection.execute("COMMIT");

        final SourceRecords records = consumeRecordsByTopic(1);

        List<SourceRecord> recordsForTopic = records.recordsForTopic("server1.DEBEZIUM.DT_TABLE");
        assertThat(recordsForTopic).hasSize(1);

        final Field before = recordsForTopic.get(0).valueSchema().field("before");

        assertThat(before.schema().field("ID").schema().parameters()).includes(
                entry(TYPE_NAME_PARAMETER_KEY, "NUMBER"),
                entry(TYPE_LENGTH_PARAMETER_KEY, "9"),
                entry(TYPE_SCALE_PARAMETER_KEY, "0"));

        assertThat(before.schema().field("C1").schema().parameters()).includes(
                entry(TYPE_NAME_PARAMETER_KEY, "NUMBER"),
                entry(TYPE_LENGTH_PARAMETER_KEY, "38"),
                entry(TYPE_SCALE_PARAMETER_KEY, "0"));

        assertThat(before.schema().field("C2").schema().parameters()).includes(
                entry(TYPE_NAME_PARAMETER_KEY, "NUMBER"),
                entry(TYPE_LENGTH_PARAMETER_KEY, "38"),
                entry(TYPE_SCALE_PARAMETER_KEY, "0"));

        assertThat(before.schema().field("C3A").schema().parameters()).includes(
                entry(TYPE_NAME_PARAMETER_KEY, "NUMBER"),
                entry(TYPE_LENGTH_PARAMETER_KEY, "5"),
                entry(TYPE_SCALE_PARAMETER_KEY, "2"));

        assertThat(before.schema().field("C3B").schema().parameters()).includes(
                entry(TYPE_NAME_PARAMETER_KEY, "VARCHAR2"),
                entry(TYPE_LENGTH_PARAMETER_KEY, "128"));

        assertThat(before.schema().field("F2").schema().parameters()).includes(
                entry(TYPE_NAME_PARAMETER_KEY, "NUMBER"),
                entry(TYPE_LENGTH_PARAMETER_KEY, "8"),
                entry(TYPE_SCALE_PARAMETER_KEY, "4"));

        assertThat(before.schema().field("F1").schema().parameters()).includes(
                entry(TYPE_NAME_PARAMETER_KEY, "FLOAT"),
                entry(TYPE_LENGTH_PARAMETER_KEY, "10"));
    }

    @Test
    @FixFor("DBZ-2624")
    public void shouldSnapshotAndStreamChangesFromTableWithNumericDefaultValues() throws Exception {
        // Drop table if it exists
        TestHelper.dropTable(connection, "debezium.complex_ddl");

        try {
            // complex ddl
            final String ddl = "create table debezium.complex_ddl (" +
                    " id numeric(6) constraint customers_id_nn not null, " +
                    " name varchar2(100)," +
                    " value numeric default 1, " +
                    " constraint customers_pk primary key(id)" +
                    ")";

            // create table
            connection.execute(ddl);
            connection.execute("GRANT SELECT ON debezium.complex_ddl to " + TestHelper.getConnectorUserName());
            connection.execute("ALTER TABLE debezium.complex_ddl ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

            // Insert a snapshot record
            connection.execute("INSERT INTO debezium.complex_ddl (id, name) values (1, 'Acme')");
            connection.commit();

            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM.COMPLEX_DDL")
                    .with(OracleConnectorConfig.LOG_MINING_STRATEGY, "online_catalog")
                    .build();

            // Perform a basic startup & initial snapshot of data
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Verify record generated during snapshot
            final SourceRecords snapshotRecords = consumeRecordsByTopic(1);
            assertThat(snapshotRecords.recordsForTopic("server1.DEBEZIUM.COMPLEX_DDL").size()).isEqualTo(1);

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO debezium.complex_ddl (id, name)values (2, 'Acme2')");
            connection.commit();

            // Verify record generated during streaming
            final SourceRecords streamingRecords = consumeRecordsByTopic(1);
            assertThat(streamingRecords.recordsForTopic("server1.DEBEZIUM.COMPLEX_DDL").size()).isEqualTo(1);
        }
        catch (Exception e) {
            TestHelper.dropTable(connection, "debezium.complex_ddl");
            throw e;
        }
    }

    @Test
    @FixFor("DBZ-2683")
    public void shouldSnapshotAndStreamChangesFromPartitionedTable() throws Exception {
        try {
            final String ddl = "CREATE TABLE players (" +
                    "id NUMERIC(6), " +
                    "name VARCHAR(100), " +
                    "birth_date DATE," +
                    "primary key(id)) " +
                    "PARTITION BY RANGE (birth_date) (" +
                    "PARTITION p2019 VALUES LESS THAN (TO_DATE('01-JAN-2020', 'dd-MON-yyyy')), " +
                    "PARTITION p2020 VALUES LESS THAN (TO_DATE('01-JAN-2021', 'dd-MON-yyyy'))" +
                    ")";
            connection.execute(ddl);
            connection.execute("GRANT SELECT ON debezium.players to " + TestHelper.getConnectorUserName());
            connection.execute("ALTER TABLE debezium.players ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

            // Insert a record to be captured by snapshot
            connection.execute("INSERT INTO debezium.players (id, name, birth_date) VALUES (1, 'Roger Rabbit', '01-MAY-2019')");
            connection.commit();

            // Start connector
            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM.PLAYERS")
                    .build();
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final SourceRecords snapshotRecords = consumeRecordsByTopic(1);
            assertThat(snapshotRecords.recordsForTopic("server1.DEBEZIUM.PLAYERS").size()).isEqualTo(1);

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Insert a record to be captured during streaming
            connection.execute("INSERT INTO debezium.players (id, name, birth_date) VALUES (2, 'Bugs Bunny', '26-JUN-2019')");
            connection.execute("INSERT INTO debezium.players (id, name, birth_date) VALUES (3, 'Elmer Fud', '01-NOV-2020')");
            connection.commit();

            final SourceRecords streamRecords = consumeRecordsByTopic(2);
            assertThat(streamRecords.recordsForTopic("server1.DEBEZIUM.PLAYERS").size()).isEqualTo(2);

        }
        finally {
            try {
                connection.execute("DROP TABLE players");
            }
            catch (Exception ignored) {
                // ignored
            }
        }
    }

    private void verifyHeartbeatRecord(SourceRecord heartbeat) {
        assertEquals("__debezium-heartbeat.server1", heartbeat.topic());

        Struct key = (Struct) heartbeat.key();
        assertThat(key.get("serverName")).isEqualTo("server1");
    }

    private long toMicroSecondsSinceEpoch(LocalDateTime localDateTime) {
        return localDateTime.toEpochSecond(ZoneOffset.UTC) * MICROS_PER_SECOND;
    }
}
