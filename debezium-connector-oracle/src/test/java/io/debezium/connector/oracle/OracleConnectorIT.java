/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.fest.assertions.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.AbstractConnectorTest;
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
                "  id int not null, " +
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
        VerifyRecord.isValidRead(record1);
        Struct after = (Struct) ((Struct)record1.value()).get("after");
        assertThat(after.get("ID")).isEqualTo(BigDecimal.valueOf(1));
        assertThat(after.get("NAME")).isEqualTo("Billie-Bob");
        assertThat(after.get("SCORE")).isEqualTo(BigDecimal.valueOf(1234.56));
        assertThat(after.get("REGISTERED")).isEqualTo(toMicroSecondsSinceEpoch(LocalDateTime.of(2018, 2, 22, 0, 0, 0)));

        assertThat(record1.sourceOffset().get(SourceInfo.SNAPSHOT_KEY)).isEqualTo(true);
        assertThat(record1.sourceOffset().get(SNAPSHOT_COMPLETED_KEY)).isEqualTo(false);

        Struct source = (Struct) ((Struct)record1.value()).get("source");
        assertThat(source.get(SourceInfo.SNAPSHOT_KEY)).isEqualTo(true);

        SourceRecord record2 = testTableRecords.get(1);
        VerifyRecord.isValidRead(record2);
        after = (Struct) ((Struct)record2.value()).get("after");
        assertThat(after.get("ID")).isEqualTo(BigDecimal.valueOf(2));
        assertThat(after.get("NAME")).isEqualTo("Bruce");
        assertThat(after.get("SCORE")).isEqualTo(BigDecimal.valueOf(2345.67));
        assertThat(after.get("REGISTERED")).isNull();

        assertThat(record2.sourceOffset().get(SourceInfo.SNAPSHOT_KEY)).isEqualTo(true);
        assertThat(record2.sourceOffset().get(SNAPSHOT_COMPLETED_KEY)).isEqualTo(true);

        source = (Struct) ((Struct)record2.value()).get("source");
        assertThat(source.get(SourceInfo.SNAPSHOT_KEY)).isEqualTo(true);
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
        VerifyRecord.isValidRead(record1);
        Struct after = (Struct) ((Struct)record1.value()).get("after");
        assertThat(after.get("ID")).isEqualTo(BigDecimal.valueOf(1));

        Struct source = (Struct) ((Struct)record1.value()).get("source");
        assertThat(source.get(SourceInfo.SNAPSHOT_KEY)).isEqualTo(true);
        assertThat(source.get(SourceInfo.SCN_KEY)).isNotNull();
        assertThat(source.get(SourceInfo.SERVER_NAME_KEY)).isEqualTo("server1");
        assertThat(source.get(SourceInfo.DEBEZIUM_VERSION_KEY)).isNotNull();
        assertThat(source.get(SourceInfo.TXID_KEY)).isNull();
        assertThat(source.get(SourceInfo.TIMESTAMP_KEY)).isNotNull();

        assertThat(record1.sourceOffset().get(SourceInfo.SNAPSHOT_KEY)).isEqualTo(true);
        assertThat(record1.sourceOffset().get(SNAPSHOT_COMPLETED_KEY)).isEqualTo(false);

        SourceRecord record2 = testTableRecords.get(1);
        VerifyRecord.isValidRead(record2);
        after = (Struct) ((Struct)record2.value()).get("after");
        assertThat(after.get("ID")).isEqualTo(BigDecimal.valueOf(2));

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
        VerifyRecord.isValidInsert(record3);
        after = (Struct) ((Struct)record3.value()).get("after");
        assertThat(after.get("ID")).isEqualTo(BigDecimal.valueOf(3));

        assertThat(record3.sourceOffset().containsKey(SourceInfo.SNAPSHOT_KEY)).isFalse();
        assertThat(record3.sourceOffset().containsKey(SNAPSHOT_COMPLETED_KEY)).isFalse();

        source = (Struct) ((Struct)record3.value()).get("source");
        assertThat(source.get(SourceInfo.SNAPSHOT_KEY)).isEqualTo(false);
        assertThat(source.get(SourceInfo.SCN_KEY)).isNotNull();
        assertThat(source.get(SourceInfo.SERVER_NAME_KEY)).isEqualTo("server1");
        assertThat(source.get(SourceInfo.DEBEZIUM_VERSION_KEY)).isNotNull();
        assertThat(source.get(SourceInfo.TXID_KEY)).isNotNull();
        assertThat(source.get(SourceInfo.TIMESTAMP_KEY)).isNotNull();
    }

    @Test
    public void shouldReadChangeStreamForExistingTable() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(RelationalDatabaseConnectorConfig.TABLE_WHITELIST, "ORCLPDB1\\.DEBEZIUM\\.CUSTOMER")
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
        VerifyRecord.isValidInsert(testTableRecords.get(0));
        Struct after = (Struct) ((Struct)testTableRecords.get(0).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(BigDecimal.valueOf(1));
        assertThat(after.get("NAME")).isEqualTo("Billie-Bob");
        assertThat(after.get("SCORE")).isEqualTo(BigDecimal.valueOf(1234.56));
        assertThat(after.get("REGISTERED")).isEqualTo(toMicroSecondsSinceEpoch(LocalDateTime.of(2018, 2, 22, 0, 0, 0)));

        // update
        VerifyRecord.isValidUpdate(testTableRecords.get(1));
        Struct before = (Struct) ((Struct)testTableRecords.get(1).value()).get("before");
        assertThat(before.get("ID")).isEqualTo(BigDecimal.valueOf(1));
        assertThat(before.get("NAME")).isEqualTo("Billie-Bob");
        assertThat(before.get("SCORE")).isEqualTo(BigDecimal.valueOf(1234.56));
        assertThat(before.get("REGISTERED")).isEqualTo(toMicroSecondsSinceEpoch(LocalDateTime.of(2018, 2, 22, 0, 0, 0)));

        after = (Struct) ((Struct)testTableRecords.get(1).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(BigDecimal.valueOf(1));
        assertThat(after.get("NAME")).isEqualTo("Bruce");
        assertThat(after.get("SCORE")).isEqualTo(BigDecimal.valueOf(2345.67));
        assertThat(after.get("REGISTERED")).isEqualTo(toMicroSecondsSinceEpoch(LocalDateTime.of(2018, 3, 23, 0, 0, 0)));

        // update of PK
        VerifyRecord.isValidDelete(testTableRecords.get(2));
        before = (Struct) ((Struct)testTableRecords.get(2).value()).get("before");
        assertThat(before.get("ID")).isEqualTo(BigDecimal.valueOf(1));
        assertThat(before.get("NAME")).isEqualTo("Bruce");
        assertThat(before.get("SCORE")).isEqualTo(BigDecimal.valueOf(2345.67));
        assertThat(before.get("REGISTERED")).isEqualTo(toMicroSecondsSinceEpoch(LocalDateTime.of(2018, 3, 23, 0, 0, 0)));

        VerifyRecord.isValidTombstone(testTableRecords.get(3));

        VerifyRecord.isValidInsert(testTableRecords.get(4));
        after = (Struct) ((Struct)testTableRecords.get(4).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(BigDecimal.valueOf(2));
        assertThat(after.get("NAME")).isEqualTo("Bruce");
        assertThat(after.get("SCORE")).isEqualTo(BigDecimal.valueOf(2345.67));
        assertThat(after.get("REGISTERED")).isEqualTo(toMicroSecondsSinceEpoch(LocalDateTime.of(2018, 3, 23, 0, 0, 0)));

        // delete
        VerifyRecord.isValidDelete(testTableRecords.get(5));
        before = (Struct) ((Struct)testTableRecords.get(5).value()).get("before");
        assertThat(before.get("ID")).isEqualTo(BigDecimal.valueOf(2));
        assertThat(before.get("NAME")).isEqualTo("Bruce");
        assertThat(before.get("SCORE")).isEqualTo(BigDecimal.valueOf(2345.67));
        assertThat(before.get("REGISTERED")).isEqualTo(toMicroSecondsSinceEpoch(LocalDateTime.of(2018, 3, 23, 0, 0, 0)));

        VerifyRecord.isValidTombstone(testTableRecords.get(6));
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
                "  id int not null, " +
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

        VerifyRecord.isValidInsert(testTableRecords.get(0));
        Struct after = (Struct) ((Struct)testTableRecords.get(0).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(BigDecimal.valueOf(2));
        assertThat(after.get("NAME")).isEqualTo("Billie-Bob");
        assertThat(after.get("SCORE")).isEqualTo(BigDecimal.valueOf(1234.56));
        assertThat(after.get("REGISTERED")).isEqualTo(toMicroSecondsSinceEpoch(LocalDateTime.of(2018, 2, 22, 0, 0, 0)));
    }

    private long toMicroSecondsSinceEpoch(LocalDateTime localDateTime) {
        return localDateTime.toEpochSecond(ZoneOffset.UTC) * MICROS_PER_SECOND;
    }
}
