/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static junit.framework.TestCase.assertEquals;
import static org.fest.assertions.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig.SnapshotMode;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.heartbeat.Heartbeat;
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

    private static OracleConnection connection;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        connection = TestHelper.testConnection();

        TestHelper.dropTable(connection, "debezium.customer");

        String ddl = "create table debezium.customer (" +
                "  id numeric(9,0) not null, " +
                "  name varchar2(1000), " +
                "  score decimal(6, 2), " +
                "  registered timestamp, " +
                "  primary key (id)" +
                ")";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.customer to  " + TestHelper.CONNECTOR_USER);
        connection.execute("ALTER TABLE debezium.customer ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
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
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
    }

    @Test
    public void shouldTakeSnapshot() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(RelationalDatabaseConnectorConfig.TABLE_WHITELIST, "ORCLPDB1\\.DEBEZIUM\\.CUSTOMER")
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
                .with(RelationalDatabaseConnectorConfig.TABLE_WHITELIST, "ORCLPDB1\\.DEBEZIUM\\.CUSTOMER")
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
                .with(RelationalDatabaseConnectorConfig.TABLE_WHITELIST, "ORCLPDB1\\.DEBEZIUM\\.CUSTOMER")
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
        sendTxBatch(expectedRecordCount, 100);
        sendTxBatch(expectedRecordCount, 200);
    }

    private void sendTxBatch(int expectedRecordCount, int offset) throws SQLException, InterruptedException {
        for (int i = offset; i < expectedRecordCount + offset; i++) {
            connection.executeWithoutCommitting(String.format("INSERT INTO debezium.customer VALUES (%s, 'Brian%s', 2345.67, null)", i, i));
        }
        connection.connection().commit();

        assertTxBatch(expectedRecordCount, offset);
    }

    private void assertTxBatch(int expectedRecordCount, int offset) throws InterruptedException {
        SourceRecords records;
        List<SourceRecord> testTableRecords;
        Struct after;
        Struct source;
        records = consumeRecordsByTopic(expectedRecordCount);
        testTableRecords = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(testTableRecords).hasSize(expectedRecordCount);

        for (int i = 0; i < expectedRecordCount; i++) {
            SourceRecord record3 = testTableRecords.get(i);
            VerifyRecord.isValidInsert(record3, "ID", i + offset);
            after = (Struct) ((Struct) record3.value()).get("after");
            assertThat(after.get("ID")).isEqualTo(i + offset);

            assertThat(record3.sourceOffset().containsKey(SourceInfo.SNAPSHOT_KEY)).isFalse();
            assertThat(record3.sourceOffset().containsKey(SNAPSHOT_COMPLETED_KEY)).isFalse();
            assertThat(record3.sourceOffset().containsKey(SourceInfo.LCR_POSITION_KEY)).isTrue();
            assertThat(record3.sourceOffset().containsKey(SourceInfo.SCN_KEY)).isFalse();

            source = (Struct) ((Struct) record3.value()).get("source");
            assertThat(source.get(SourceInfo.SNAPSHOT_KEY)).isEqualTo("false");
            assertThat(source.get(SourceInfo.SCN_KEY)).isNotNull();
            assertThat(source.get(SourceInfo.LCR_POSITION_KEY)).isNotNull();
            assertThat(source.get(SourceInfo.SERVER_NAME_KEY)).isEqualTo("server1");
            assertThat(source.get(SourceInfo.DEBEZIUM_VERSION_KEY)).isNotNull();
            assertThat(source.get(SourceInfo.TXID_KEY)).isNotNull();
            assertThat(source.get(SourceInfo.TIMESTAMP_KEY)).isNotNull();
        }
    }

    @Test
    public void shouldStreamAfterRestart() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(RelationalDatabaseConnectorConfig.TABLE_WHITELIST, "ORCLPDB1\\.DEBEZIUM\\.CUSTOMER")
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

        expectedRecordCount = 30;
        connection.setAutoCommit(false);
        sendTxBatch(expectedRecordCount, 100);
        sendTxBatch(expectedRecordCount, 200);

        stopConnector();
        final int OFFSET = 300;
        for (int i = OFFSET; i < expectedRecordCount + OFFSET; i++) {
            connection.executeWithoutCommitting(String.format("INSERT INTO debezium.customer VALUES (%s, 'Brian%s', 2345.67, null)", i, i));
        }
        connection.connection().commit();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        assertTxBatch(expectedRecordCount, 300);
        sendTxBatch(expectedRecordCount, 400);
        sendTxBatch(expectedRecordCount, 500);
    }

    @Test
    public void shouldStreamAfterRestartAfterSnapshot() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(RelationalDatabaseConnectorConfig.TABLE_WHITELIST, "ORCLPDB1\\.DEBEZIUM\\.CUSTOMER")
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

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        assertTxBatch(expectedRecordCount, 100);
        sendTxBatch(expectedRecordCount, 200);
    }

    @Test
    public void shouldReadChangeStreamForExistingTable() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(RelationalDatabaseConnectorConfig.TABLE_WHITELIST, "ORCLPDB1\\.DEBEZIUM\\.CUSTOMER")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_SCHEMA_ONLY)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        Thread.sleep(1000);

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
                .with(RelationalDatabaseConnectorConfig.TABLE_WHITELIST, "ORCLPDB1\\.DEBEZIUM\\.CUSTOMER")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_SCHEMA_ONLY)
                .with(OracleConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        Thread.sleep(1000);

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
    public void shouldReadChangeStreamForTableCreatedWhileStreaming() throws Exception {
        TestHelper.dropTable(connection, "debezium.customer2");

        Configuration config = TestHelper.defaultConfig()
                .with(RelationalDatabaseConnectorConfig.TABLE_WHITELIST, "ORCLPDB1\\.DEBEZIUM\\.CUSTOMER2")
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        Thread.sleep(1000);

        String ddl = "create table debezium.customer2 (" +
                "  id numeric(9,0) not null, " +
                "  name varchar2(1000), " +
                "  score decimal(6, 2), " +
                "  registered timestamp, " +
                "  primary key (id)" +
                ")";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.customer2 to " + TestHelper.CONNECTOR_USER);

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
    public void shouldReceiveHeartbeatAlsoWhenChangingNonWhitelistedTable() throws Exception {
        TestHelper.dropTable(connection, "debezium.dbz800a");
        TestHelper.dropTable(connection, "debezium.dbz800b");

        // the low heartbeat interval should make sure that a heartbeat message is emitted after each change record
        // received from Postgres
        Configuration config = TestHelper.defaultConfig()
                .with(Heartbeat.HEARTBEAT_INTERVAL, "1")
                .with(OracleConnectorConfig.TABLE_WHITELIST, "ORCLPDB1\\.DEBEZIUM\\.DBZ800B")
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        Thread.sleep(1000);

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

    private void verifyHeartbeatRecord(SourceRecord heartbeat) {
        assertEquals("__debezium-heartbeat.server1", heartbeat.topic());

        Struct key = (Struct) heartbeat.key();
        assertThat(key.get("serverName")).isEqualTo("server1");
    }

    private long toMicroSecondsSinceEpoch(LocalDateTime localDateTime) {
        return localDateTime.toEpochSecond(ZoneOffset.UTC) * MICROS_PER_SECOND;
    }
}
