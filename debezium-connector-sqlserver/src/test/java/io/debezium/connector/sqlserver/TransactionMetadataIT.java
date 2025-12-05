/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNull;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.SchemaAndValueField;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.EqualityCheck;
import io.debezium.junit.SkipTestRule;
import io.debezium.junit.SkipWhenKafkaVersion;
import io.debezium.junit.SkipWhenKafkaVersion.KafkaVersion;
import io.debezium.util.Collect;
import io.debezium.util.Testing;

/**
 * Integration test for the Debezium SQL Server connector.
 *
 * @author Jiri Pechanec
 */
@SkipWhenKafkaVersion(check = EqualityCheck.EQUAL, value = KafkaVersion.KAFKA_1XX, description = "Not compatible with Kafka 1.x")
public class TransactionMetadataIT extends AbstractAsyncEngineConnectorTest {

    private SqlServerConnection connection;

    @Rule
    public SkipTestRule skipRule = new SkipTestRule();

    @Before
    public void before() throws SQLException {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        connection.execute(
                "CREATE TABLE tablea (id int primary key, cola varchar(30))",
                "CREATE TABLE tableb (id int primary key, colb varchar(30))",
                "INSERT INTO tablea VALUES(1, 'a')");
        TestHelper.enableTableCdc(connection, "tablea");
        TestHelper.enableTableCdc(connection, "tableb");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        // Testing.Print.enable();
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void transactionMetadata() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Testing.Print.enable();
        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        connection.setAutoCommit(false);
        final String[] inserts = new String[RECORDS_PER_TABLE * 2];
        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            inserts[2 * i] = "INSERT INTO tablea VALUES(" + id + ", 'a')";
            inserts[2 * i + 1] = "INSERT INTO tableb VALUES(" + id + ", 'b')";
        }
        connection.execute(inserts);
        connection.setAutoCommit(true);

        connection.execute("INSERT INTO tableb VALUES(1000, 'b')");

        // BEGIN, data, END, BEGIN, data
        final SourceRecords records = consumeRecordsByTopic(1 + RECORDS_PER_TABLE * 2 + 1 + 1 + 1);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("server1.testDB1.dbo.tableb");
        final List<SourceRecord> tx = records.recordsForTopic("server1.transaction");
        assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        assertThat(tableB).hasSize(RECORDS_PER_TABLE + 1);
        assertThat(tx).hasSize(3);

        final List<SourceRecord> all = records.allRecordsInOrder();
        final String txId = assertBeginTransaction(all.get(0));

        long counter = 1;
        for (int i = 1; i <= 2 * RECORDS_PER_TABLE; i++) {
            assertRecordTransactionMetadata(all.get(i), txId, counter, (counter + 1) / 2);
            counter++;
        }

        assertEndTransaction(all.get(2 * RECORDS_PER_TABLE + 1), txId, 2 * RECORDS_PER_TABLE,
                Collect.hashMapOf("testDB1.dbo.tablea", RECORDS_PER_TABLE, "testDB1.dbo.tableb", RECORDS_PER_TABLE));
    }

    private void restartInTheMiddleOfTx(boolean restartJustAfterSnapshot, boolean afterStreaming) throws Exception {
        final int RECORDS_PER_TABLE = 30;
        final int TABLES = 2;
        final int ID_START = 10;
        final int ID_RESTART = 1000;
        final int HALF_ID = ID_START + RECORDS_PER_TABLE / 2;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .build();

        // Testing.Print.enable();

        if (restartJustAfterSnapshot) {
            start(SqlServerConnector.class, config);
            assertConnectorIsRunning();

            // Wait for snapshot to be completed
            consumeRecordsByTopic(1);
            stopConnector();
            connection.execute("INSERT INTO tablea VALUES(-1, '-a')");

            Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> {
                if (!connection.getMaxLsn(TestHelper.TEST_DATABASE_1).isAvailable()) {
                    return false;
                }

                for (SqlServerChangeTable ct : connection.getChangeTables(TestHelper.TEST_DATABASE_1)) {
                    final String tableName = ct.getChangeTableId().table();
                    if (tableName.endsWith("dbo_" + connection.getNameOfChangeTable("tablea"))) {
                        try {
                            final Lsn minLsn = connection.getMinLsn(TestHelper.TEST_DATABASE_1, tableName);
                            final Lsn maxLsn = connection.getMaxLsn(TestHelper.TEST_DATABASE_1);
                            final AtomicReference<Boolean> found = new AtomicReference<>(false);
                            try (ResultSet rs = connection.getChangesForTable(ct, minLsn, maxLsn)) {
                                while (rs.next()) {
                                    if (rs.getInt("id") == -1) {
                                        found.set(true);
                                        break;
                                    }
                                }
                            }
                            return found.get();
                        }
                        catch (Exception e) {
                            org.junit.Assert.fail("Failed to fetch changes for tablea: " + e.getMessage());
                        }
                    }
                }
                return false;
            });
        }

        start(SqlServerConnector.class, config, record -> {
            if (!"server1.testDB1.dbo.tablea.Envelope".equals(record.valueSchema().name())) {
                return false;
            }
            final Struct envelope = (Struct) record.value();
            final Struct after = envelope.getStruct("after");
            final Integer id = after.getInt32("id");
            final String value = after.getString("cola");
            return id != null && id == HALF_ID && "a".equals(value);
        });
        assertConnectorIsRunning();

        String firstTxId = null;
        if (restartJustAfterSnapshot) {
            // Transaction begin
            SourceRecord begin = consumeRecordsByTopic(1).allRecordsInOrder().get(0);
            firstTxId = assertBeginTransaction(begin);
        }

        // Wait for snapshot to be completed or a first streaming message delivered
        consumeRecordsByTopic(1);

        if (afterStreaming) {
            connection.execute("INSERT INTO tablea VALUES(-2, '-a')");
            final SourceRecords records = consumeRecordsByTopic(2);
            final List<SchemaAndValueField> expectedRow = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, -2),
                    new SchemaAndValueField("cola", Schema.OPTIONAL_STRING_SCHEMA, "-a"));
            assertRecord(((Struct) records.allRecordsInOrder().get(1).value()).getStruct(Envelope.FieldName.AFTER), expectedRow);
            SourceRecord begin = records.allRecordsInOrder().get(0);
            firstTxId = assertBeginTransaction(begin);
        }

        connection.setAutoCommit(false);
        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.executeWithoutCommitting(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.executeWithoutCommitting(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }
        connection.connection().commit();

        // End of previous TX, BEGIN of new TX, change records
        final int txBeginIndex = firstTxId != null ? 1 : 0;
        int expectedRecords = txBeginIndex + 1 + RECORDS_PER_TABLE;
        List<SourceRecord> records = consumeRecordsByTopic(expectedRecords).allRecordsInOrder();

        assertThat(records).hasSize(expectedRecords);

        if (firstTxId != null) {
            assertEndTransaction(records.get(0), firstTxId, 1, Collect.hashMapOf("testDB1.dbo.tablea", 1));
        }
        final String batchTxId = assertBeginTransaction(records.get(txBeginIndex));

        SourceRecord lastRecordForOffset = records.get(RECORDS_PER_TABLE + txBeginIndex);
        Struct value = (Struct) lastRecordForOffset.value();
        final List<SchemaAndValueField> expectedLastRow = Arrays.asList(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, HALF_ID - 1),
                new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));
        assertRecord((Struct) value.get("after"), expectedLastRow);
        assertRecordTransactionMetadata(lastRecordForOffset, batchTxId, RECORDS_PER_TABLE, RECORDS_PER_TABLE / 2);

        waitForEngineShutdown();
        cleanupTestFwkState();
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords sourceRecords = consumeRecordsByTopic(RECORDS_PER_TABLE);
        records = sourceRecords.allRecordsInOrder();
        assertThat(records).hasSize(RECORDS_PER_TABLE);

        List<SourceRecord> tableA = sourceRecords.recordsForTopic("server1.testDB1.dbo.tablea");
        List<SourceRecord> tableB = sourceRecords.recordsForTopic("server1.testDB1.dbo.tableb");
        for (int i = 0; i < RECORDS_PER_TABLE / 2; i++) {
            final int id = HALF_ID + i;
            final SourceRecord recordA = tableA.get(i);
            final SourceRecord recordB = tableB.get(i);
            final List<SchemaAndValueField> expectedRowA = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, id),
                    new SchemaAndValueField("cola", Schema.OPTIONAL_STRING_SCHEMA, "a"));
            final List<SchemaAndValueField> expectedRowB = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, id),
                    new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));

            final Struct valueA = (Struct) recordA.value();
            assertRecord((Struct) valueA.get("after"), expectedRowA);
            assertNull(valueA.get("before"));

            final Struct valueB = (Struct) recordB.value();
            assertRecord((Struct) valueB.get("after"), expectedRowB);
            assertNull(valueB.get("before"));

            assertRecordTransactionMetadata(recordA, batchTxId, RECORDS_PER_TABLE + 2 * i + 1, RECORDS_PER_TABLE / 2 + i + 1);
            assertRecordTransactionMetadata(recordB, batchTxId, RECORDS_PER_TABLE + 2 * i + 2, RECORDS_PER_TABLE / 2 + i + 1);
        }

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_RESTART + i;
            connection.executeWithoutCommitting(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.executeWithoutCommitting(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
            connection.connection().commit();
        }

        // END of previous TX, data records, BEGIN of TX for every pair of record, END of TX for every pair of record but last
        sourceRecords = consumeRecordsByTopic(1 + RECORDS_PER_TABLE * TABLES + (2 * RECORDS_PER_TABLE - 1));
        tableA = sourceRecords.recordsForTopic("server1.testDB1.dbo.tablea");
        tableB = sourceRecords.recordsForTopic("server1.testDB1.dbo.tableb");
        List<SourceRecord> txMetadata = sourceRecords.recordsForTopic("server1.transaction");

        assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        assertThat(tableB).hasSize(RECORDS_PER_TABLE);
        assertThat(txMetadata).hasSize(1 + 2 * RECORDS_PER_TABLE - 1);
        assertEndTransaction(txMetadata.get(0), batchTxId, 2 * RECORDS_PER_TABLE,
                Collect.hashMapOf("testDB1.dbo.tablea", RECORDS_PER_TABLE, "testDB1.dbo.tableb", RECORDS_PER_TABLE));

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = i + ID_RESTART;
            final SourceRecord recordA = tableA.get(i);
            final SourceRecord recordB = tableB.get(i);
            final List<SchemaAndValueField> expectedRowA = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, id),
                    new SchemaAndValueField("cola", Schema.OPTIONAL_STRING_SCHEMA, "a"));
            final List<SchemaAndValueField> expectedRowB = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, id),
                    new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));

            final Struct valueA = (Struct) recordA.value();
            assertRecord((Struct) valueA.get("after"), expectedRowA);
            assertNull(valueA.get("before"));

            final Struct valueB = (Struct) recordB.value();
            assertRecord((Struct) valueB.get("after"), expectedRowB);
            assertNull(valueB.get("before"));

            final String txId = assertBeginTransaction(txMetadata.get(2 * i + 1));
            assertRecordTransactionMetadata(recordA, txId, 1, 1);
            assertRecordTransactionMetadata(recordB, txId, 2, 1);
            if (i < RECORDS_PER_TABLE - 1) {
                assertEndTransaction(txMetadata.get(2 * i + 2), txId, 2, Collect.hashMapOf("testDB1.dbo.tablea", 1, "testDB1.dbo.tableb", 1));
            }
        }
    }

    @Test
    public void restartInTheMiddleOfTxAfterSnapshot() throws Exception {
        restartInTheMiddleOfTx(true, false);
    }

    @Test
    public void restartInTheMiddleOfTxAfterCompletedTx() throws Exception {
        restartInTheMiddleOfTx(false, true);
    }

    @Test
    public void restartInTheMiddleOfTx() throws Exception {
        restartInTheMiddleOfTx(false, false);
    }

    private void assertRecord(Struct record, List<SchemaAndValueField> expected) {
        expected.forEach(schemaAndValueField -> schemaAndValueField.assertFor(record));
    }
}
