/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static io.debezium.connector.oracle.util.TestHelper.TYPE_LENGTH_PARAMETER_KEY;
import static io.debezium.connector.oracle.util.TestHelper.TYPE_NAME_PARAMETER_KEY;
import static io.debezium.connector.oracle.util.TestHelper.TYPE_SCALE_PARAMETER_KEY;
import static io.debezium.connector.oracle.util.TestHelper.defaultConfig;
import static io.debezium.data.Envelope.FieldName.AFTER;
import static org.fest.assertions.Assertions.assertThat;
import static org.fest.assertions.MapAssert.entry;

import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningStrategy;
import io.debezium.connector.oracle.OracleConnectorConfig.SnapshotMode;
import io.debezium.connector.oracle.junit.RequireDatabaseOption;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipTestDependingOnDatabaseOptionRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.converters.CloudEventsConverterTest;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.FieldName;
import io.debezium.data.SchemaAndValueField;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
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
    @Rule
    public final TestRule skipOptionRule = new SkipTestDependingOnDatabaseOptionRule();

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
            TestHelper.dropTable(connection, "debezium.customer2");
            TestHelper.dropTable(connection, "customer");
            TestHelper.dropTable(connection, "masked_hashed_column_table");
            TestHelper.dropTable(connection, "truncated_column_table");
            TestHelper.dropTable(connection, "dt_table");
            connection.close();
        }
    }

    @Before
    public void before() throws SQLException {
        TestHelper.dropTable(connection, "debezium.dbz800a");
        TestHelper.dropTable(connection, "debezium.dbz800b");
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
        try {
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
            Struct after1 = (Struct) ((Struct) record1.value()).get(AFTER);
            assertThat(after1.get("ID")).isEqualTo(1);
            assertThat(after1.get("C1")).isEqualTo(BigDecimal.valueOf(25L));
            assertThat(after1.get("C2")).isEqualTo("Test");
            assertThat(record1.sourceOffset().get(SourceInfo.SNAPSHOT_KEY)).isEqualTo(true);
            assertThat(record1.sourceOffset().get(SNAPSHOT_COMPLETED_KEY)).isEqualTo(true);

            // insert
            SourceRecord record2 = hyphenatedTableRecords.get(1);
            VerifyRecord.isValidInsert(record2, "ID", 2);
            Struct after2 = (Struct) ((Struct) record2.value()).get(AFTER);
            assertThat(after2.get("ID")).isEqualTo(2);
            assertThat(after2.get("C1")).isEqualTo(BigDecimal.valueOf(50L));
            assertThat(after2.get("C2")).isEqualTo("Test2");
        }
        finally {
            TestHelper.dropTable(connection, "debezium.\"my-table\"");
        }
    }

    @Test
    public void shouldTakeSnapshot() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .build();

        int expectedRecordCount = 0;
        connection.execute("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
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

    private void continueStreamingAfterSnapshot(Configuration config) throws Exception {
        int expectedRecordCount = 0;
        connection.execute("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
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
    public void shouldStreamTransaction() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .build();

        // Testing.Print.enable();
        int expectedRecordCount = 0;
        connection.execute("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
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
    public void shouldStreamAfterRestart() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .build();

        // Testing.Print.enable();
        int expectedRecordCount = 0;
        connection.execute("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
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
        connection.execute("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
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
        connection.execute("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
        connection.execute("COMMIT");

        expectedRecordCount += 1;

        connection.execute("UPDATE debezium.customer SET name = 'Bruce', score = 2345.67, registered = TO_DATE('2018-03-23', 'yyyy-mm-dd') WHERE id = 1");
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
        connection.execute("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
        connection.execute("COMMIT");
        expectedRecordCount += 1;

        connection.execute("DELETE debezium.customer WHERE id = 1");
        connection.execute("COMMIT");
        expectedRecordCount += 1; // deletion, no tombstone

        connection.execute("INSERT INTO debezium.customer VALUES (2, 'Billie-Bob', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
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
    public void shouldReadChangeStreamForTableCreatedWhileStreaming() throws Exception {
        TestHelper.dropTable(connection, "debezium.customer2");
        try {
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
            TestHelper.streamTable(connection, "debezium.customer2");

            connection.execute("INSERT INTO debezium.customer2 VALUES (2, 'Billie-Bob', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
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
        finally {
            TestHelper.dropTable(connection, "debezium.customer2");
        }
    }

    @Test
    @FixFor("DBZ-800")
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

        // The number of heartbeat events may vary depending on how fast the events are seen in
        // LogMiner, so to compensate for the varied number of heartbeats that might be emitted
        // the following waits until we have seen the DBZ800B event before returning.
        final AtomicReference<SourceRecords> records = new AtomicReference<>();
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .until(() -> {
                    if (records.get() == null) {
                        records.set(consumeRecordsByTopic(1));
                    }
                    else {
                        consumeRecordsByTopic(1).allRecordsInOrder().forEach(records.get()::add);
                    }
                    return records.get().recordsForTopic("server1.DEBEZIUM.DBZ800B") != null;
                });

        List<SourceRecord> heartbeats = records.get().recordsForTopic("__debezium-heartbeat.server1");
        List<SourceRecord> tableA = records.get().recordsForTopic("server1.DEBEZIUM.DBZ800A");
        List<SourceRecord> tableB = records.get().recordsForTopic("server1.DEBEZIUM.DBZ800B");

        // there should be at least one heartbeat, no events for DBZ800A and one for DBZ800B
        assertThat(heartbeats).isNotEmpty();
        assertThat(tableA).isNull();
        assertThat(tableB).hasSize(1);

        VerifyRecord.isValidInsert(tableB.get(0), "ID", 2);
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
            final String dbName = TestHelper.getDatabaseName();
            config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                    .with("column.mask.with.12.chars", dbName + ".DEBEZIUM.MASKED_HASHED_COLUMN_TABLE.NAME")
                    .with("column.mask.hash.SHA-256.with.salt.CzQMA0cB5K",
                            dbName + ".DEBEZIUM.MASKED_HASHED_COLUMN_TABLE.NAME2," + dbName + ".DEBEZIUM.MASKED_HASHED_COLUMN_TABLE.NAME3")
                    .with("column.truncate.to.4.chars", dbName + ".DEBEZIUM.TRUNCATED_COLUMN_TABLE.NAME")
                    .build();
        }
        else {
            config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
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
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                    .with(OracleConnectorConfig.MSG_KEY_COLUMNS, "(.*).debezium.customer:id,name")
                    .build();
        }
        else {
            config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                    .with(OracleConnectorConfig.MSG_KEY_COLUMNS, "debezium.customer:id,name")
                    .build();
        }

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("INSERT INTO debezium.customer VALUES (3, 'Nest', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
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
        finally {
            TestHelper.dropTable(connection, "debezium.complex_ddl");
        }
    }

    @Test
    @FixFor("DBZ-2683")
    @RequireDatabaseOption("Partitioning")
    public void shouldSnapshotAndStreamChangesFromPartitionedTable() throws Exception {
        TestHelper.dropTable(connection, "players");
        try {
            final String ddl = "CREATE TABLE players (" +
                    "id NUMERIC(6), " +
                    "name VARCHAR(100), " +
                    "birth_date DATE," +
                    "primary key(id)) " +
                    "PARTITION BY RANGE (birth_date) (" +
                    "PARTITION p2019 VALUES LESS THAN (TO_DATE('2020-01-01', 'yyyy-mm-dd')), " +
                    "PARTITION p2020 VALUES LESS THAN (TO_DATE('2021-01-01', 'yyyy-mm-dd'))" +
                    ")";
            connection.execute(ddl);
            connection.execute("GRANT SELECT ON debezium.players to " + TestHelper.getConnectorUserName());
            connection.execute("ALTER TABLE debezium.players ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

            // Insert a record to be captured by snapshot
            connection.execute("INSERT INTO debezium.players (id, name, birth_date) VALUES (1, 'Roger Rabbit', TO_DATE('2019-05-01', 'yyyy-mm-dd'))");
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
            connection.execute("INSERT INTO debezium.players (id, name, birth_date) VALUES (2, 'Bugs Bunny', TO_DATE('2019-06-26', 'yyyy-mm-dd'))");
            connection.execute("INSERT INTO debezium.players (id, name, birth_date) VALUES (3, 'Elmer Fud', TO_DATE('2020-11-01', 'yyyy-mm-dd'))");
            connection.commit();

            final SourceRecords streamRecords = consumeRecordsByTopic(2);
            assertThat(streamRecords.recordsForTopic("server1.DEBEZIUM.PLAYERS").size()).isEqualTo(2);
        }
        finally {
            TestHelper.dropTable(connection, "players");
        }
    }

    @Test
    @FixFor("DBZ-2849")
    public void shouldAvroSerializeColumnsWithSpecialCharacters() throws Exception {
        // Setup environment
        TestHelper.dropTable(connection, "columns_test");
        try {
            connection.execute("CREATE TABLE columns_test (id NUMERIC(6), amount$ number not null, primary key(id))");
            connection.execute("GRANT SELECT ON debezium.columns_test to " + TestHelper.getConnectorUserName());
            connection.execute("ALTER TABLE debezium.columns_test ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

            // Insert a record for snapshot
            connection.execute("INSERT INTO debezium.columns_test (id, amount$) values (1, 12345.67)");
            connection.commit();

            // Start connector
            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM.COLUMNS_TEST")
                    .with(OracleConnectorConfig.SANITIZE_FIELD_NAMES, "true")
                    .build();
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final SourceRecords snapshots = consumeRecordsByTopic(1);
            assertThat(snapshots.recordsForTopic("server1.DEBEZIUM.COLUMNS_TEST").size()).isEqualTo(1);

            final SourceRecord snapshot = snapshots.recordsForTopic("server1.DEBEZIUM.COLUMNS_TEST").get(0);
            VerifyRecord.isValidRead(snapshot, "ID", 1);

            Struct after = ((Struct) snapshot.value()).getStruct(AFTER);
            assertThat(after.getInt32("ID")).isEqualTo(1);
            assertThat(after.get("AMOUNT_")).isEqualTo(VariableScaleDecimal.fromLogical(after.schema().field("AMOUNT_").schema(), BigDecimal.valueOf(12345.67d)));

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Insert a record for streaming
            connection.execute("INSERT INTO debezium.columns_test (id, amount$) values (2, 23456.78)");
            connection.commit();

            final SourceRecords streams = consumeRecordsByTopic(1);
            assertThat(streams.recordsForTopic("server1.DEBEZIUM.COLUMNS_TEST").size()).isEqualTo(1);

            final SourceRecord stream = streams.recordsForTopic("server1.DEBEZIUM.COLUMNS_TEST").get(0);
            VerifyRecord.isValidInsert(stream, "ID", 2);

            after = ((Struct) stream.value()).getStruct(AFTER);
            assertThat(after.getInt32("ID")).isEqualTo(2);
            assertThat(after.get("AMOUNT_")).isEqualTo(VariableScaleDecimal.fromLogical(after.schema().field("AMOUNT_").schema(), BigDecimal.valueOf(23456.78d)));
        }
        finally {
            TestHelper.dropTable(connection, "columns_test");
        }
    }

    @Test
    @FixFor("DBZ-2825")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER, reason = "Tests archive log support for LogMiner only")
    public void testArchiveLogScnBoundariesAreIncluded() throws Exception {
        // Drop table if it exists
        TestHelper.dropTable(connection, "alog_test");
        try {
            final String ddl = "CREATE TABLE alog_test (id numeric, name varchar2(50), primary key(id))";
            connection.execute(ddl);
            connection.execute("GRANT SELECT ON debezium.alog_test TO " + TestHelper.getConnectorUserName());
            connection.execute("ALTER TABLE debezium.alog_test ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
            connection.commit();

            // Insert a snapshot record
            connection.execute("INSERT INTO debezium.alog_test (id, name) VALUES (1, 'Test')");
            connection.commit();

            // start connector and take snapshot
            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM.ALOG_TEST")
                    .build();
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Validate snapshot record
            final SourceRecords snapshotRecords = consumeRecordsByTopic(1);
            assertThat(snapshotRecords.recordsForTopic("server1.DEBEZIUM.ALOG_TEST").size()).isEqualTo(1);
            SourceRecord record = snapshotRecords.recordsForTopic("server1.DEBEZIUM.ALOG_TEST").get(0);
            Struct after = (Struct) ((Struct) record.value()).get(AFTER);
            assertThat(after.get("ID")).isEqualTo(BigDecimal.valueOf(1));
            assertThat(after.get("NAME")).isEqualTo("Test");

            // stop the connector
            stopConnector();

            // Force flush of all redo logs to archive logs
            TestHelper.forceFlushOfRedoLogsToArchiveLogs();

            // Start connector and wait for streaming
            start(OracleConnector.class, config);
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Insert record for streaming
            connection.execute("INSERT INTO debezium.alog_test (id, name) values (2, 'Home')");
            connection.execute("COMMIT");

            // Validate streaming record
            final SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.ALOG_TEST").size()).isEqualTo(1);
            record = records.recordsForTopic("server1.DEBEZIUM.ALOG_TEST").get(0);
            after = (Struct) ((Struct) record.value()).get(AFTER);
            assertThat(after.get("ID")).isEqualTo(BigDecimal.valueOf(2));
            assertThat(after.get("NAME")).isEqualTo("Home");
        }
        finally {
            TestHelper.dropTable(connection, "alog_test");
        }
    }

    @Test
    @FixFor("DBZ-2784")
    public void shouldConvertDatesSpecifiedAsStringInSQL() throws Exception {
        try {
            TestHelper.dropTable(connection, "orders");

            final String ddl = "CREATE TABLE orders (" +
                    "id NUMERIC(6), " +
                    "order_date date not null," +
                    "primary key(id))";

            connection.execute(ddl);
            connection.execute("GRANT SELECT ON debezium.orders TO " + TestHelper.getConnectorUserName());
            connection.execute("ALTER TABLE debezium.orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

            connection.execute("INSERT INTO debezium.orders VALUES (9, '22-FEB-2018')");
            connection.execute("COMMIT");

            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "debezium.orders")
                    .build();

            start(OracleConnector.class, config);
            assertNoRecordsToConsume();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final SourceRecords snapshotRecords = consumeRecordsByTopic(1);
            final List<SourceRecord> snapshotOrders = snapshotRecords.recordsForTopic("server1.DEBEZIUM.ORDERS");
            assertThat(snapshotOrders.size()).isEqualTo(1);

            final Struct snapshotAfter = ((Struct) snapshotOrders.get(0).value()).getStruct(AFTER);
            assertThat(snapshotAfter.get("ID")).isEqualTo(9);
            assertThat(snapshotAfter.get("ORDER_DATE")).isEqualTo(1519257600000L);

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO debezium.orders VALUES (10, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
            connection.execute("COMMIT");

            final SourceRecords streamRecords = consumeRecordsByTopic(1);
            final List<SourceRecord> orders = streamRecords.recordsForTopic("server1.DEBEZIUM.ORDERS");
            assertThat(orders).hasSize(1);

            final Struct after = ((Struct) orders.get(0).value()).getStruct(AFTER);
            assertThat(after.get("ID")).isEqualTo(10);
            assertThat(after.get("ORDER_DATE")).isEqualTo(1519257600000L);
        }
        finally {
            TestHelper.dropTable(connection, "orders");
        }
    }

    @Test
    @FixFor("DBZ-2733")
    public void shouldConvertNumericAsStringDecimalHandlingMode() throws Exception {
        TestHelper.dropTable(connection, "table_number_pk");
        try {
            final String ddl = "CREATE TABLE table_number_pk (id NUMBER, name varchar2(255), age number, primary key (id))";
            connection.execute(ddl);
            connection.execute("GRANT SELECT ON debezium.table_number_pk TO " + TestHelper.getConnectorUserName());
            connection.execute("ALTER TABLE debezium.table_number_pk ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

            // Insert snapshot record
            connection.execute("INSERT INTO debezium.table_number_pk (id, name, age) values (1, 'Bob', 25)");
            connection.execute("COMMIT");

            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "debezium.table_number_pk")
                    .with(OracleConnectorConfig.DECIMAL_HANDLING_MODE, "string")
                    .build();

            // Start connector
            start(OracleConnector.class, config);
            assertNoRecordsToConsume();
            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Read snapshot record & verify
            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.TABLE_NUMBER_PK")).hasSize(1);

            SourceRecord record = records.recordsForTopic("server1.DEBEZIUM.TABLE_NUMBER_PK").get(0);

            List<SchemaAndValueField> expected = Arrays.asList(
                    new SchemaAndValueField("ID", Schema.STRING_SCHEMA, "1"),
                    new SchemaAndValueField("NAME", Schema.OPTIONAL_STRING_SCHEMA, "Bob"),
                    new SchemaAndValueField("AGE", Schema.OPTIONAL_STRING_SCHEMA, "25"));
            assertRecordSchemaAndValues(expected, record, AFTER);

            // Wait for streaming to have begun
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Insert streaming record
            connection.execute("INSERT INTO debezium.table_number_pk (id, name, age) values (2, 'Sue', 30)");
            connection.execute("COMMIT");

            // Read stream record & verify
            records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.TABLE_NUMBER_PK")).hasSize(1);

            record = records.recordsForTopic("server1.DEBEZIUM.TABLE_NUMBER_PK").get(0);
            expected = Arrays.asList(
                    new SchemaAndValueField("ID", Schema.STRING_SCHEMA, "2"),
                    new SchemaAndValueField("NAME", Schema.OPTIONAL_STRING_SCHEMA, "Sue"),
                    new SchemaAndValueField("AGE", Schema.OPTIONAL_STRING_SCHEMA, "30"));
            assertRecordSchemaAndValues(expected, record, AFTER);
        }
        finally {
            TestHelper.dropTable(connection, "table_number_pk");
        }
    }

    protected void assertRecordSchemaAndValues(List<SchemaAndValueField> expectedByColumn, SourceRecord record, String envelopeFieldName) {
        Struct content = ((Struct) record.value()).getStruct(envelopeFieldName);
        if (expectedByColumn == null) {
            assertThat(content).isNull();
        }
        else {
            assertThat(content).as("expected there to be content in Envelope under " + envelopeFieldName).isNotNull();
            expectedByColumn.forEach(expected -> expected.assertFor(content));
        }
    }

    @Test
    @FixFor("DBZ-2920")
    public void shouldStreamDdlThatExceeds4000() throws Exception {
        TestHelper.dropTable(connection, "large_dml");

        // Setup table
        final String ddl = "CREATE TABLE large_dml (id NUMERIC(6), value varchar2(4000), value2 varchar2(4000), primary key(id))";
        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.large_dml TO " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE debezium.large_dml ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

        // Prepare snapshot record
        String largeValue = generateAlphaNumericStringColumn(4000);
        String largeValue2 = generateAlphaNumericStringColumn(4000);
        connection.execute("INSERT INTO large_dml (id, value, value2) values (1, '" + largeValue + "', '" + largeValue2 + "')");
        connection.commit();

        // Start connector
        final Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "debezium.large_dml")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();
        start(OracleConnector.class, config);
        assertNoRecordsToConsume();

        // Verify snapshot
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.topics()).hasSize(1);
        assertThat(records.recordsForTopic("server1.DEBEZIUM.LARGE_DML")).hasSize(1);

        Struct after = ((Struct) records.recordsForTopic("server1.DEBEZIUM.LARGE_DML").get(0).value()).getStruct(AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VALUE")).isEqualTo(largeValue);
        assertThat(after.get("VALUE2")).isEqualTo(largeValue2);

        // Prepare stream records
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        List<String> largeValues = new ArrayList<>();
        List<String> largeValues2 = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            largeValues.add(generateAlphaNumericStringColumn(4000));
            largeValues2.add(generateAlphaNumericStringColumn(4000));
            connection.execute("INSERT INTO large_dml (id, value, value2) values (" + (2 + i) + ", '" + largeValues.get(largeValues.size() - 1) + "', '"
                    + largeValues2.get(largeValues2.size() - 1) + "')");
        }
        connection.commit();

        // Verify stream
        records = consumeRecordsByTopic(10);
        assertThat(records.topics()).hasSize(1);
        assertThat(records.recordsForTopic("server1.DEBEZIUM.LARGE_DML")).hasSize(10);

        List<SourceRecord> entries = records.recordsForTopic("server1.DEBEZIUM.LARGE_DML");
        for (int i = 0; i < 10; ++i) {
            SourceRecord record = entries.get(i);
            after = ((Struct) record.value()).getStruct(AFTER);
            assertThat(after.get("ID")).isEqualTo(2 + i);
            assertThat(after.get("VALUE")).isEqualTo(largeValues.get(i));
            assertThat(after.get("VALUE2")).isEqualTo(largeValues2.get(i));
        }

        // Stop connector
        stopConnector((r) -> TestHelper.dropTable(connection, "large_dml"));
    }

    @Test
    @FixFor("DBZ-2891")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.XSTREAM, reason = "Only applies to Xstreams")
    public void shouldNotObserveDeadlockWhileStreamingWithXstream() throws Exception {
        long oldPollTimeInMs = pollTimeoutInMs;
        TestHelper.dropTable(connection, "deadlock_test");
        try {
            final String ddl = "CREATE TABLE deadlock_test (id numeric(9), name varchar2(50), primary key(id))";
            connection.execute(ddl);
            connection.execute("GRANT SELECT ON debezium.deadlock_test TO " + TestHelper.getConnectorUserName());
            connection.execute("ALTER TABLE debezium.deadlock_test ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

            // Set connector to poll very slowly
            this.pollTimeoutInMs = TimeUnit.SECONDS.toMillis(20);

            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "debezium.deadlock_test")
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                    .with(OracleConnectorConfig.MAX_QUEUE_SIZE, 2)
                    .with(RelationalDatabaseConnectorConfig.MAX_BATCH_SIZE, 1)
                    .build();

            start(OracleConnector.class, config);
            assertNoRecordsToConsume();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            for (int i = 0; i < 10; ++i) {
                connection.execute("INSERT INTO deadlock_test (id, name) values (" + i + ", 'Test " + i + "')");
                connection.execute("COMMIT");
            }

            SourceRecords records = consumeRecordsByTopic(10, 24);
            assertThat(records.topics()).hasSize(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DEADLOCK_TEST")).hasSize(10);

        }
        finally {
            // Reset poll time
            this.pollTimeoutInMs = oldPollTimeInMs;
            TestHelper.dropTable(connection, "deadlock_test");
        }
    }

    @Test
    @FixFor("DBZ-3057")
    public void shouldReadTableUniqueIndicesWithCharactersThatRequireExplicitQuotes() throws Exception {
        final String TABLE_NAME = "debezium.\"#T70_Sid:582003931_1_ConnConne\"";
        try {
            TestHelper.dropTable(connection, TABLE_NAME);

            final String ddl = "CREATE GLOBAL TEMPORARY TABLE " + TABLE_NAME + " (id number, name varchar2(50))";
            connection.execute(ddl);
            connection.execute("GRANT SELECT ON " + TABLE_NAME + " TO " + TestHelper.getConnectorUserName());
            connection.execute("ALTER TABLE " + TABLE_NAME + " ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, OracleConnectorConfig.SnapshotMode.INITIAL)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.\\#T70_Sid\\:582003931_1_ConnConne")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        }
        finally {
            TestHelper.dropTable(connection, TABLE_NAME);
        }
    }

    @Test
    @FixFor("DBZ-3151")
    public void testSnapshotCompletesWithSystemGeneratedUniqueIndexOnKeylessTable() throws Exception {
        TestHelper.dropTable(connection, "XML_TABLE");
        try {
            final String ddl = "CREATE TABLE XML_TABLE of XMLTYPE";
            connection.execute(ddl);
            connection.execute("GRANT SELECT ON DEBEZIUM.XML_TABLE TO " + TestHelper.getConnectorUserName());
            connection.execute("ALTER TABLE DEBEZIUM.XML_TABLE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

            connection.execute("INSERT INTO DEBEZIUM.XML_TABLE values (xmltype('<?xml version=\"1.0\"?><tab><name>Hi</name></tab>'))");
            connection.execute("COMMIT");

            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.XML_TABLE")
                    .build();

            start(OracleConnector.class, config);
            assertNoRecordsToConsume();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        }
        finally {
            TestHelper.dropTable(connection, "XML_TABLE");
        }
    }

    @Test
    @FixFor("DBZ-3001")
    public void shouldGetOracleDatabaseVersion() throws Exception {
        OracleDatabaseVersion version = connection.getOracleVersion();
        assertThat(version).isNotNull();
        assertThat(version.getMajor()).isGreaterThan(0);
    }

    @Test
    @FixFor("DBZ-3109")
    public void shouldStreamChangesForTableWithMultipleLogGroupTypes() throws Exception {
        try {
            TestHelper.dropTable(connection, "log_group_test");

            final String ddl = "CREATE TABLE log_group_test (id numeric(9,0) primary key, name varchar2(50))";
            connection.execute(ddl);
            connection.execute("GRANT SELECT ON debezium.log_group_test TO " + TestHelper.getConnectorUserName());
            connection.execute("ALTER TABLE debezium.log_group_test ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
            connection.execute("ALTER TABLE debezium.log_group_test ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS");

            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.LOG_GROUP_TEST")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO debezium.log_group_test (id, name) values (1,'Test')");
            connection.execute("COMMIT");

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.LOG_GROUP_TEST")).hasSize(1);
        }
        finally {
            TestHelper.dropTable(connection, "log_group_test");
        }
    }

    @Test
    @FixFor("DBZ-2875")
    public void shouldResumeStreamingAtCorrectScnOffset() throws Exception {
        TestHelper.dropTable(connection, "offset_test");
        try {
            Testing.Debug.enable();

            connection.execute("CREATE TABLE offset_test (id numeric(9,0) primary key, name varchar2(50))");
            connection.execute("GRANT SELECT ON debezium.offset_test TO " + TestHelper.getConnectorUserName());
            connection.execute("ALTER TABLE debezium.offset_test ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.OFFSET_TEST")
                    .build();

            start(OracleConnector.class, config);
            assertNoRecordsToConsume();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO debezium.offset_test (id, name) values (1, 'Bob')");

            SourceRecords records1 = consumeRecordsByTopic(1);
            assertThat(records1.recordsForTopic("server1.DEBEZIUM.OFFSET_TEST")).hasSize(1);

            Struct after = (Struct) ((Struct) records1.allRecordsInOrder().get(0).value()).get("after");
            Testing.print(after);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("NAME")).isEqualTo("Bob");

            stopConnector();

            start(OracleConnector.class, config);
            assertNoRecordsToConsume();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO debezium.offset_test (id, name) values (2, 'Bill')");

            SourceRecords records2 = consumeRecordsByTopic(1);
            assertThat(records2.recordsForTopic("server1.DEBEZIUM.OFFSET_TEST")).hasSize(1);

            after = (Struct) ((Struct) records2.allRecordsInOrder().get(0).value()).get("after");
            Testing.print(after);
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get("NAME")).isEqualTo("Bill");
        }
        finally {
            TestHelper.dropTable(connection, "offset_test");
        }
    }

    @Test
    @FixFor("DBZ-3036")
    public void shouldHandleParentChildIndexOrganizedTables() throws Exception {
        TestHelper.dropTable(connection, "test_iot");
        try {
            String ddl = "CREATE TABLE test_iot (" +
                    "id numeric(9,0), " +
                    "description varchar2(50) not null, " +
                    "primary key(id)) " +
                    "ORGANIZATION INDEX " +
                    "INCLUDING description " +
                    "OVERFLOW";
            connection.execute(ddl);
            TestHelper.streamTable(connection, "debezium.test_iot");

            // Insert data for snapshot
            connection.executeWithoutCommitting("INSERT INTO debezium.test_iot VALUES ('1', 'Hello World')");
            connection.execute("COMMIT");

            Configuration config = defaultConfig()
                    .with(OracleConnectorConfig.SCHEMA_INCLUDE_LIST, "DEBEZIUM")
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "(.)*IOT(.)*")
                    .build();

            start(OracleConnector.class, config);
            assertNoRecordsToConsume();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.TEST_IOT")).hasSize(1);

            SourceRecord record = records.recordsForTopic("server1.DEBEZIUM.TEST_IOT").get(0);
            Struct after = (Struct) ((Struct) record.value()).get(FieldName.AFTER);
            VerifyRecord.isValidRead(record, "ID", 1);
            assertThat(after.get("DESCRIPTION")).isEqualTo("Hello World");

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Insert data for streaming
            connection.executeWithoutCommitting("INSERT INTO debezium.test_iot VALUES ('2', 'Goodbye')");
            connection.execute("COMMIT");

            records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.TEST_IOT")).hasSize(1);

            record = records.recordsForTopic("server1.DEBEZIUM.TEST_IOT").get(0);
            after = (Struct) ((Struct) record.value()).get(FieldName.AFTER);
            VerifyRecord.isValidInsert(record, "ID", 2);
            assertThat(after.get("DESCRIPTION")).isEqualTo("Goodbye");
        }
        finally {
            TestHelper.dropTable(connection, "test_iot");
            // This makes sure all index-organized tables are cleared after dropping parent table
            TestHelper.purgeRecycleBin(connection);
        }
    }

    // todo: should this test be removed since its now covered in OracleClobDataTypesIT?
    @Test
    @FixFor("DBZ-3257")
    public void shouldSnapshotAndStreamClobDataTypes() throws Exception {
        TestHelper.dropTable(connection, "clob_test");
        try {
            String ddl = "CREATE TABLE clob_test(id numeric(9,0) primary key, val_clob clob, val_nclob nclob)";
            connection.execute(ddl);
            TestHelper.streamTable(connection, "clob_test");

            connection.execute("INSERT INTO clob_test values (1, 'TestClob', 'TestNClob')");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CLOB_TEST")
                    .with(OracleConnectorConfig.LOB_ENABLED, true)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords sourceRecords = consumeRecordsByTopic(1);
            assertThat(sourceRecords.recordsForTopic("server1.DEBEZIUM.CLOB_TEST")).hasSize(1);

            List<SourceRecord> records = sourceRecords.recordsForTopic("server1.DEBEZIUM.CLOB_TEST");
            VerifyRecord.isValidRead(records.get(0), "ID", 1);
            Struct after = (Struct) ((Struct) records.get(0).value()).get(Envelope.FieldName.AFTER);
            assertThat(after.get("VAL_CLOB")).isEqualTo("TestClob");
            assertThat(after.get("VAL_NCLOB")).isEqualTo("TestNClob");

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("UPDATE clob_test SET val_clob = 'TestClob2', val_nclob = 'TestNClob2' WHERE ID = 1");

            sourceRecords = consumeRecordsByTopic(1);
            assertThat(sourceRecords.recordsForTopic("server1.DEBEZIUM.CLOB_TEST")).hasSize(1);

            records = sourceRecords.recordsForTopic("server1.DEBEZIUM.CLOB_TEST");
            VerifyRecord.isValidUpdate(records.get(0), "ID", 1);
            after = (Struct) ((Struct) records.get(0).value()).get(Envelope.FieldName.AFTER);
            assertThat(after.get("VAL_CLOB")).isEqualTo("TestClob2");
            assertThat(after.get("VAL_NCLOB")).isEqualTo("TestNClob2");
        }
        finally {
            TestHelper.dropTable(connection, "clob_test");
        }
    }

    @Test
    @FixFor("DBZ-3347")
    public void shouldContainPartitionInSchemaChangeEvent() throws Exception {
        TestHelper.dropTable(connection, "dbz3347");
        try {
            connection.execute("create table dbz3347 (id number primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz3347");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3347")
                    .with(OracleConnectorConfig.LOG_MINING_STRATEGY, "online_catalog")
                    .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords schemaChanges = consumeRecordsByTopic(1);
            SourceRecord change = schemaChanges.recordsForTopic(TestHelper.SERVER_NAME).get(0);
            assertThat(change.sourcePartition()).isEqualTo(Collections.singletonMap("server", TestHelper.SERVER_NAME));
        }
        finally {
            TestHelper.dropTable(connection, "dbz3347");
        }
    }

    @Test
    @FixFor("DBZ-832")
    public void shouldSnapshotAndStreamTablesWithNoPrimaryKey() throws Exception {
        TestHelper.dropTable(connection, "dbz832");
        try {
            connection.execute("create table dbz832 (id numeric(9,0), data varchar2(50))");
            TestHelper.streamTable(connection, "dbz832");

            connection.execute("INSERT INTO dbz832 values (1, 'Test')");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ832")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ832")).hasSize(1);
            SourceRecord record = records.recordsForTopic("server1.DEBEZIUM.DBZ832").get(0);
            assertThat(record.key()).isNull();
            Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo("Test");

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz832 values (2, 'Test2')");
            records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ832")).hasSize(1);
            record = records.recordsForTopic("server1.DEBEZIUM.DBZ832").get(0);
            assertThat(record.key()).isNull();
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get("DATA")).isEqualTo("Test2");

        }
        finally {
            TestHelper.dropTable(connection, "dbz832");
        }
    }

    @Test
    @FixFor("DBZ-1211")
    public void shouldSnapshotAndStreamTablesWithUniqueIndexPrimaryKey() throws Exception {
        TestHelper.dropTables(connection, "dbz1211_child", "dbz1211");
        try {
            connection.execute("create table dbz1211 (id numeric(9,0), data varchar2(50), constraint pkdbz1211 primary key (id) using index)");
            connection.execute("alter table dbz1211 add constraint xdbz1211 unique (id,data) using index");
            connection
                    .execute("create table dbz1211_child (id numeric(9,0), data varchar2(50), constraint fk1211 foreign key (id) references dbz1211 on delete cascade)");
            connection.execute("alter table dbz1211_child add constraint ydbz1211 unique (id,data) using index");
            TestHelper.streamTable(connection, "dbz1211");
            TestHelper.streamTable(connection, "dbz1211_child");

            connection.executeWithoutCommitting("INSERT INTO dbz1211 values (1, 'Test')");
            connection.executeWithoutCommitting("INSERT INTO dbz1211_child values (1, 'Child')");
            connection.commit();

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ1211,DEBEZIUM\\.DBZ1211\\_CHILD")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(2);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ1211")).hasSize(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ1211_CHILD")).hasSize(1);

            SourceRecord record = records.recordsForTopic("server1.DEBEZIUM.DBZ1211").get(0);
            Struct key = (Struct) record.key();
            assertThat(key).isNotNull();
            assertThat(key.get("ID")).isEqualTo(1);
            assertThat(key.schema().field("DATA")).isNull();
            Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo("Test");

            record = records.recordsForTopic("server1.DEBEZIUM.DBZ1211_CHILD").get(0);
            key = (Struct) record.key();
            assertThat(key).isNotNull();
            assertThat(key.get("ID")).isEqualTo(1);
            assertThat(key.get("DATA")).isEqualTo("Child");
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo("Child");

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz1211 values (2, 'Test2')");
            connection.executeWithoutCommitting("INSERT INTO dbz1211_child values (1, 'Child1-2')");
            connection.executeWithoutCommitting("INSERT INTO dbz1211_child values (2, 'Child2-1')");
            connection.commit();

            records = consumeRecordsByTopic(3);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ1211")).hasSize(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ1211_CHILD")).hasSize(2);

            record = records.recordsForTopic("server1.DEBEZIUM.DBZ1211").get(0);
            key = (Struct) record.key();
            assertThat(key).isNotNull();
            assertThat(key.get("ID")).isEqualTo(2);
            assertThat(key.schema().field("DATA")).isNull();
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get("DATA")).isEqualTo("Test2");

            record = records.recordsForTopic("server1.DEBEZIUM.DBZ1211_CHILD").get(0);
            key = (Struct) record.key();
            assertThat(key).isNotNull();
            assertThat(key.get("ID")).isEqualTo(1);
            assertThat(key.get("DATA")).isEqualTo("Child1-2");
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo("Child1-2");

            record = records.recordsForTopic("server1.DEBEZIUM.DBZ1211_CHILD").get(1);
            key = (Struct) record.key();
            assertThat(key).isNotNull();
            assertThat(key.get("ID")).isEqualTo(2);
            assertThat(key.get("DATA")).isEqualTo("Child2-1");
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get("DATA")).isEqualTo("Child2-1");
        }
        finally {
            TestHelper.dropTables(connection, "dbz1211_child", "dbz1211");
        }
    }

    @Test
    @FixFor("DBZ-3322")
    public void shouldNotEmitEventsOnConstraintViolations() throws Exception {
        TestHelper.dropTable(connection, "dbz3322");
        try {
            connection.execute("CREATE TABLE dbz3322 (id number(9,0), data varchar2(50))");
            connection.execute("CREATE UNIQUE INDEX uk_dbz3322 ON dbz3322 (id)");
            TestHelper.streamTable(connection, "dbz3322");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3322")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            try {
                connection.executeWithoutCommitting("INSERT INTO dbz3322 (id,data) values (1, 'Test1')");
                connection.executeWithoutCommitting("INSERT INTO dbz3322 (id,data) values (1, 'Test2')");
            }
            catch (SQLException e) {
                // ignore unique constraint violation
                if (!e.getMessage().startsWith("ORA-00001")) {
                    throw e;
                }
            }
            finally {
                connection.executeWithoutCommitting("COMMIT");
            }

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ3322")).hasSize(1);

            final Struct after = (((Struct) records.recordsForTopic("server1.DEBEZIUM.DBZ3322").get(0).value()).getStruct("after"));
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo("Test1");

            assertNoRecordsToConsume();

        }
        finally {
            TestHelper.dropTable(connection, "dbz3322");
        }
    }

    @Test
    @FixFor("DBZ-3322")
    public void shouldNotEmitEventsInRollbackTransaction() throws Exception {
        TestHelper.dropTable(connection, "dbz3322");
        try {
            connection.execute("CREATE TABLE dbz3322 (id number(9,0), data varchar2(50))");
            connection.execute("CREATE UNIQUE INDEX uk_dbz3322 ON dbz3322 (id)");
            TestHelper.streamTable(connection, "dbz3322");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3322")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.executeWithoutCommitting("INSERT INTO dbz3322 (id,data) values (1, 'Test')");
            connection.executeWithoutCommitting("INSERT INTO dbz3322 (id,data) values (2, 'Test')");
            connection.executeWithoutCommitting("ROLLBACK");

            connection.executeWithoutCommitting("INSERT INTO dbz3322 (id,data) values (3, 'Test')");
            connection.executeWithoutCommitting("COMMIT");

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ3322")).hasSize(1);
            Struct value = (Struct) records.recordsForTopic("server1.DEBEZIUM.DBZ3322").get(0).value();
            assertThat(value.getStruct(Envelope.FieldName.AFTER).get("ID")).isEqualTo(3);
            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz3322");
        }
    }

    @Test
    @FixFor("DBZ-3062")
    public void shouldSelectivelySnapshotTables() throws Exception {
        TestHelper.dropTables(connection, "dbz3062a", "dbz3062b");
        try {
            connection.execute("CREATE TABLE dbz3062a (id number(9,0), data varchar2(50))");
            connection.execute("CREATE TABLE dbz3062b (id number(9,0), data varchar2(50))");
            TestHelper.streamTable(connection, "dbz3062a");
            TestHelper.streamTable(connection, "dbz3062b");

            connection.execute("INSERT INTO dbz3062a VALUES (1, 'Test1')");
            connection.execute("INSERT INTO dbz3062b VALUES (2, 'Test2')");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3062.*")
                    .with(OracleConnectorConfig.SNAPSHOT_MODE_TABLES, "[A-z].*DEBEZIUM\\.DBZ3062A")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> tableA = records.recordsForTopic("server1.DEBEZIUM.DBZ3062A");
            List<SourceRecord> tableB = records.recordsForTopic("server1.DEBEZIUM.DBZ3062B");

            assertThat(tableA).hasSize(1);
            assertThat(tableB).isNull();

            Struct after = ((Struct) tableA.get(0).value()).getStruct(AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo("Test1");

            assertNoRecordsToConsume();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.executeWithoutCommitting("INSERT INTO dbz3062a VALUES (3, 'Test3')");
            connection.executeWithoutCommitting("INSERT INTO dbz3062b VALUES (4, 'Test4')");
            connection.commit();

            records = consumeRecordsByTopic(2);
            tableA = records.recordsForTopic("server1.DEBEZIUM.DBZ3062A");
            tableB = records.recordsForTopic("server1.DEBEZIUM.DBZ3062B");

            assertThat(tableA).hasSize(1);
            assertThat(tableB).hasSize(1);

            after = ((Struct) tableA.get(0).value()).getStruct(AFTER);
            assertThat(after.get("ID")).isEqualTo(3);
            assertThat(after.get("DATA")).isEqualTo("Test3");

            after = ((Struct) tableB.get(0).value()).getStruct(AFTER);
            assertThat(after.get("ID")).isEqualTo(4);
            assertThat(after.get("DATA")).isEqualTo("Test4");
        }
        finally {
            TestHelper.dropTables(connection, "dbz3062a", "dbz3062b");
        }
    }

    @Test
    @FixFor("DBZ-3616")
    public void shouldNotLogWarningsAboutCommittedTransactionsWhileStreamingNormally() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor();
        TestHelper.dropTables(connection, "dbz3616", "dbz3616");
        try {
            connection.execute("CREATE TABLE dbz3616 (id number(9,0), data varchar2(50))");
            TestHelper.streamTable(connection, "dbz3616");
            connection.commit();

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3616.*")
                    // use online_catalog mode explicitly due to Awaitility timer below.
                    .with(OracleConnectorConfig.LOG_MINING_STRATEGY, "online_catalog")
                    .build();

            // Start connector
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            // Wait for streaming
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Start second connection and write an insert, without commit.
            OracleConnection connection2 = TestHelper.testConnection();
            connection2.executeWithoutCommitting("INSERT INTO dbz3616 (id,data) values (1,'Conn2')");

            // One first connection write an insert without commit, then explicitly commit it.
            connection.executeWithoutCommitting("INSERT INTO dbz3616 (id,data) values (2,'Conn1')");
            connection.commit();

            // Connector should continually re-mine the first transaction until committed.
            // During this time, there should not be these extra log messages that we
            // will assert against later via LogInterceptor.
            Awaitility.await()
                    .pollDelay(Durations.ONE_MINUTE)
                    .timeout(Durations.TWO_MINUTES)
                    .until(() -> true);

            // Now commit connection2, this means we should get 2 inserts.
            connection2.commit();

            // Now get the 2 records, two inserts from both transactions
            SourceRecords records = consumeRecordsByTopic(2);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ3616")).hasSize(2);

            List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ3616");
            assertThat(((Struct) tableRecords.get(0).value()).getStruct("after").get("ID")).isEqualTo(2);
            assertThat(((Struct) tableRecords.get(1).value()).getStruct("after").get("ID")).isEqualTo(1);

            // Now check that we didn't get the unnecessary warning messages.
            assertThat(logInterceptor.containsWarnMessage("was already processed, ignore.")).isFalse();
        }
        finally {
            TestHelper.dropTables(connection, "dbz3616", "dbz3616");
        }
    }

    @Test
    @FixFor("DBZ-3668")
    public void shouldOutputRecordsInCloudEventsFormat() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .build();

        connection.execute("INSERT INTO customer (id,name,score) values (1001, 'DBZ3668', 100)");

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        SourceRecords records = consumeRecordsByTopic(1);

        List<SourceRecord> customers = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(customers).hasSize(1);

        for (SourceRecord customer : customers) {
            CloudEventsConverterTest.shouldConvertToCloudEventsInJson(customer, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(customer, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInAvro(customer, "oracle", "server1", false);
        }

        connection.execute("INSERT INTO customer (id,name,score) values (1002, 'DBZ3668', 95)");
        records = consumeRecordsByTopic(1);

        customers = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(customers).hasSize(1);

        for (SourceRecord customer : customers) {
            CloudEventsConverterTest.shouldConvertToCloudEventsInJson(customer, false, jsonNode -> {
                assertThat(jsonNode.get(CloudEventsMaker.FieldName.ID).asText()).contains("scn:");
            });
            CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(customer, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInAvro(customer, "oracle", "server1", false);
        }
    }

    @Test
    @FixFor("DBZ-3896")
    public void shouldCaptureTableMetadataWithMultipleStatements() throws Exception {
        try {
            Configuration config = TestHelper.defaultConfig().with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3896").build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("CREATE TABLE dbz3896 (id number(9,0), name varchar2(50), data varchar2(50))",
                    "CREATE UNIQUE INDEX dbz3896_pk ON dbz3896 (\"ID\", \"NAME\")",
                    "ALTER TABLE dbz3896 ADD CONSTRAINT idx_dbz3896 PRIMARY KEY (\"ID\", \"NAME\") USING INDEX \"DBZ3896_PK\"");
            TestHelper.streamTable(connection, "dbz3896");
            connection.execute("INSERT INTO dbz3896 (id,name,data) values (1,'First','Test')");

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ3896")).hasSize(1);

            SourceRecord record = records.recordsForTopic("server1.DEBEZIUM.DBZ3896").get(0);
            assertThat(record.key()).isNotNull();
            assertThat(record.keySchema().field("ID")).isNotNull();
            assertThat(record.keySchema().field("NAME")).isNotNull();
            assertThat(((Struct) record.key()).get("ID")).isEqualTo(1);
            assertThat(((Struct) record.key()).get("NAME")).isEqualTo("First");
        }
        finally {
            TestHelper.dropTable(connection, "dbz3896");
        }
    }

    @Test
    @FixFor("DBZ-3898")
    public void shouldIgnoreAllTablesInExcludedSchemas() throws Exception {
        try {
            TestHelper.dropTable(connection, "dbz3898");

            connection.execute("CREATE TABLE dbz3898 (id number(9,0), data varchar2(50))");
            TestHelper.streamTable(connection, "dbz3898");

            // Explicitly uses CATALOG_IN_REDO mining strategy
            // This strategy makes changes to several LOGMNR tables as DDL tracking gets enabled
            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.LOG_MINING_STRATEGY, LogMiningStrategy.CATALOG_IN_REDO)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz3898 (id,data) values (1,'Test')");

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ3898")).hasSize(1);

            // Wait 2 minutes to let the connector run a few cycles
            // Then check that there is absolutely nothing to consume and that no exceptions are thrown
            Awaitility.await().atMost(Duration.ofMinutes(3)).pollDelay(Duration.ofMinutes(2)).until(() -> true);
            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz3898");
        }
    }

    @Test
    @FixFor("DBZ-3712")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER, reason = "Tests archive log support for LogMiner only")
    public void shouldStartWithArchiveLogOnlyModeAndStreamWhenRecordsBecomeAvailable() throws Exception {
        TestHelper.dropTable(connection, "dbz3712");
        try {
            connection.execute("CREATE TABLE dbz3712 (id number(9,0), data varchar2(50))");
            TestHelper.streamTable(connection, "dbz3712");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.LOG_MINING_ARCHIVE_LOG_ONLY_MODE, true)
                    .with(OracleConnectorConfig.LOG_MINING_ARCHIVE_LOG_ONLY_SCN_POLL_INTERVAL_MS, 2000)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3712")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // At this point the connector is new and should not emit any records as the SCN offset
            // obtained from the snapshot is in the redo logs.
            waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
            assertNoRecordsToConsume();

            // We will insert a new record but this record won't be emitted right away and will
            // require that a log switch happen so that it can be emitted.
            connection.execute("INSERT INTO dbz3712 (id,data) values (1, 'Test')");
            waitForLogSwitchOrForceOneAfterTimeout();

            // We should now be able to consume a record
            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ3712")).hasSize(1);
        }
        finally {
            TestHelper.dropTable(connection, "dbz3712");
        }
    }

    @Test
    @FixFor("DBZ-3712")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER, reason = "Tests archive log support for LogMiner only")
    public void shouldPermitChangingToArchiveLogOnlyModeOnExistingConnector() throws Exception {
        TestHelper.dropTable(connection, "dbz3712");
        try {
            connection.execute("CREATE TABLE dbz3712 (id number(9,0), data varchar2(50))");
            TestHelper.streamTable(connection, "dbz3712");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.LOG_MINING_ARCHIVE_LOG_ONLY_SCN_POLL_INTERVAL_MS, 2000)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3712")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // The connector was started with archive.log.only.mode disabled so this record should
            // be emitted immediately once its written to the redo logs.
            connection.execute("INSERT INTO dbz3712 (id,data) values (1, 'Test1')");

            // We should now be able to consume a record
            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ3712")).hasSize(1);

            // Restart connector using the same offsets but with archive log only mode
            stopConnector();

            config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.LOG_MINING_ARCHIVE_LOG_ONLY_MODE, true)
                    .with(OracleConnectorConfig.LOG_MINING_ARCHIVE_LOG_ONLY_SCN_POLL_INTERVAL_MS, 2000)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3712")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // At this point the connector was restarted with archive log only mode. The SCN offset
            // was previously in the redo logs and may likely not be in the archive logs on start so
            // we'll give the connector a moment and verify it has no records to consume.
            waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
            assertNoRecordsToConsume();

            // Insert a new record
            // This should not be picked up until after a log switch
            connection.execute("INSERT INTO dbz3712 (id,data) values (2, 'Test2')");
            waitForLogSwitchOrForceOneAfterTimeout();

            // We should now be able to consume a record
            records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ3712")).hasSize(1);

            // Insert a new record
            // This should not be picked up until after a log switch
            connection.execute("INSERT INTO dbz3712 (id,data) values (3, 'Test2')");
            waitForLogSwitchOrForceOneAfterTimeout();

            // We should now be able to consume a record
            records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ3712")).hasSize(1);
        }
        finally {
            TestHelper.dropTable(connection, "dbz3712");
        }
    }

    private void waitForLogSwitchOrForceOneAfterTimeout() throws SQLException {
        List<BigInteger> sequences = TestHelper.getCurrentRedoLogSequences();
        try {
            Awaitility.await()
                    .pollInterval(Duration.of(5, ChronoUnit.SECONDS))
                    .atMost(Duration.of(20, ChronoUnit.SECONDS))
                    .until(() -> {
                        if (TestHelper.getCurrentRedoLogSequences().equals(sequences)) {
                            assertNoRecordsToConsume();
                            return false;
                        }
                        // Oracle triggered its on log switch
                        return true;
                    });

            // In this use case Oracle triggered its own log switch
            // We don't need to trigger one on our own.
        }
        catch (ConditionTimeoutException e) {
            // expected if Oracle doesn't trigger its own log switch
            TestHelper.forceLogfileSwitch();
        }
    }

    private String generateAlphaNumericStringColumn(int size) {
        final String alphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz";
        final StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; ++i) {
            int index = (int) (alphaNumericString.length() * Math.random());
            sb.append(alphaNumericString.charAt(index));
        }
        return sb.toString();
    }

    private long toMicroSecondsSinceEpoch(LocalDateTime localDateTime) {
        return localDateTime.toEpochSecond(ZoneOffset.UTC) * MICROS_PER_SECOND;
    }

    @Test
    @FixFor("DBZ-4161")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER, reason = "Applies to LogMiner only")
    public void shouldWarnAboutTableNameLengthExceeded() throws Exception {
        try {
            TestHelper.dropTable(connection, "dbz4161_with_a_name_that_is_greater_than_30");

            connection.execute("CREATE TABLE dbz4161_with_a_name_that_is_greater_than_30 (id numeric(9,0), data varchar2(30))");
            TestHelper.streamTable(connection, "dbz4161_with_a_name_that_is_greater_than_30");

            connection.execute("INSERT INTO dbz4161_with_a_name_that_is_greater_than_30 values (1, 'snapshot')");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4161_WITH_A_NAME_THAT_IS_GREATER_THAN_30")
                    .build();

            LogInterceptor logInterceptor = new LogInterceptor();
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ4161_WITH_A_NAME_THAT_IS_GREATER_THAN_30")).hasSize(1);

            SourceRecord record = records.recordsForTopic("server1.DEBEZIUM.DBZ4161_WITH_A_NAME_THAT_IS_GREATER_THAN_30").get(0);
            Struct after = ((Struct) record.value()).getStruct(AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo("snapshot");

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz4161_with_a_name_that_is_greater_than_30 values (2, 'streaming')");
            waitForCurrentScnToHaveBeenSeenByConnector();

            assertNoRecordsToConsume();
            assertThat(logInterceptor.containsWarnMessage("Table 'DBZ4161_WITH_A_NAME_THAT_IS_GREATER_THAN_30' won't be captured by Oracle LogMiner")).isTrue();
        }
        finally {
            TestHelper.dropTable(connection, "dbz4161_with_a_name_that_is_greater_than_30");
        }
    }

    @Test
    @FixFor("DBZ-4161")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER, reason = "Applies to LogMiner only")
    public void shouldWarnAboutColumnNameLengthExceeded() throws Exception {
        try {
            TestHelper.dropTable(connection, "dbz4161");

            connection.execute("CREATE TABLE dbz4161 (id numeric(9,0), a_very_long_column_name_that_is_greater_than_30 varchar2(30))");
            TestHelper.streamTable(connection, "dbz4161");

            connection.execute("INSERT INTO dbz4161 values (1, 'snapshot')");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4161")
                    .build();

            LogInterceptor logInterceptor = new LogInterceptor();
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ4161")).hasSize(1);

            SourceRecord record = records.recordsForTopic("server1.DEBEZIUM.DBZ4161").get(0);
            Struct after = ((Struct) record.value()).getStruct(AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("A_VERY_LONG_COLUMN_NAME_THAT_IS_GREATER_THAN_30")).isEqualTo("snapshot");

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz4161 values (2, 'streaming')");
            waitForCurrentScnToHaveBeenSeenByConnector();

            assertNoRecordsToConsume();
            assertThat(logInterceptor.containsWarnMessage("Table 'DBZ4161' won't be captured by Oracle LogMiner")).isTrue();
        }
        finally {
            TestHelper.dropTable(connection, "dbz4161");
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T getStreamingMetric(String metricName) throws JMException {
        final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

        final ObjectName objectName = getStreamingMetricsObjectName(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        return (T) mbeanServer.getAttribute(objectName, metricName);
    }

    private void waitForCurrentScnToHaveBeenSeenByConnector() throws SQLException {
        try (OracleConnection admin = TestHelper.adminConnection()) {
            admin.resetSessionToCdb();
            final Scn scn = admin.getCurrentScn();
            Awaitility.await()
                    .atMost(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS)
                    .until(() -> {
                        final String scnValue = getStreamingMetric("CurrentScn");
                        if (scnValue == null) {
                            return false;
                        }
                        return Scn.valueOf(scnValue).compareTo(scn) > 0;
                    });
        }
    }
}
