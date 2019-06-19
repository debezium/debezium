/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.SchemaAndValueField;
import io.debezium.data.SourceRecordAssert;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.util.Testing;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertNull;

/**
 * Integration test for the Debezium SQL Server connector.
 *
 * @author Jiri Pechanec
 */
public class SqlServerConnectorIT extends AbstractConnectorTest {

    private SqlServerConnection connection;

    @Before
    public void before() throws SQLException {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        connection.execute(
                "CREATE TABLE tablea (id int primary key, cola varchar(30))",
                "CREATE TABLE tableb (id int primary key, colb varchar(30))",
                "INSERT INTO tablea VALUES(1, 'a')"
        );
        TestHelper.enableTableCdc(connection, "tablea");
        TestHelper.enableTableCdc(connection, "tableb");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
        // Testing.Print.enable();
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void createAndDelete() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')"
            );
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')"
            );
        }

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.dbo.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("server1.dbo.tableb");
        Assertions.assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(tableB).hasSize(RECORDS_PER_TABLE);
        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final SourceRecord recordA = tableA.get(i);
            final SourceRecord recordB = tableB.get(i);
            final List<SchemaAndValueField> expectedRowA = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, i + ID_START),
                    new SchemaAndValueField("cola", Schema.OPTIONAL_STRING_SCHEMA, "a"));
            final List<SchemaAndValueField> expectedRowB = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, i + ID_START),
                    new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));

            final Struct keyA = (Struct) recordA.key();
            final Struct valueA = (Struct) recordA.value();
            assertRecord((Struct) valueA.get("after"), expectedRowA);
            assertNull(valueA.get("before"));

            final Struct keyB = (Struct) recordB.key();
            final Struct valueB = (Struct) recordB.value();
            assertRecord((Struct) valueB.get("after"), expectedRowB);
            assertNull(valueB.get("before"));
        }

        connection.execute("DELETE FROM tableB");
        final SourceRecords deleteRecords = consumeRecordsByTopic(2 * RECORDS_PER_TABLE);
        final List<SourceRecord> deleteTableA = deleteRecords.recordsForTopic("server1.dbo.tablea");
        final List<SourceRecord> deleteTableB = deleteRecords.recordsForTopic("server1.dbo.tableb");
        Assertions.assertThat(deleteTableA).isNullOrEmpty();
        Assertions.assertThat(deleteTableB).hasSize(2 * RECORDS_PER_TABLE);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final SourceRecord deleteRecord = deleteTableB.get(i * 2);
            final SourceRecord tombstoneRecord = deleteTableB.get(i * 2 + 1);
            final List<SchemaAndValueField> expectedDeleteRow = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, i + ID_START),
                    new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));

            final Struct deleteKey = (Struct) deleteRecord.key();
            final Struct deleteValue = (Struct) deleteRecord.value();
            assertRecord((Struct) deleteValue.get("before"), expectedDeleteRow);
            assertNull(deleteValue.get("after"));

            final Struct tombstoneKey = (Struct) tombstoneRecord.key();
            final Struct tombstoneValue = (Struct) tombstoneRecord.value();
            assertNull(tombstoneValue);
        }

        stopConnector();
    }

    @Test
    public void deleteWithoutTombstone() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')"
            );
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')"
            );
        }

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);

        connection.execute("DELETE FROM tableB");
        final SourceRecords deleteRecords = consumeRecordsByTopic(RECORDS_PER_TABLE);
        final List<SourceRecord> deleteTableA = deleteRecords.recordsForTopic("server1.dbo.tablea");
        final List<SourceRecord> deleteTableB = deleteRecords.recordsForTopic("server1.dbo.tableb");
        Assertions.assertThat(deleteTableA).isNullOrEmpty();
        Assertions.assertThat(deleteTableB).hasSize(RECORDS_PER_TABLE);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final SourceRecord deleteRecord = deleteTableB.get(i);
            final List<SchemaAndValueField> expectedDeleteRow = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, i + ID_START),
                    new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));

            final Struct deleteKey = (Struct) deleteRecord.key();
            final Struct deleteValue = (Struct) deleteRecord.value();
            assertRecord((Struct) deleteValue.get("before"), expectedDeleteRow);
            assertNull(deleteValue.get("after"));
        }

        stopConnector();
    }

    @Test
    public void update() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        connection.setAutoCommit(false);
        final String[] tableBInserts = new String[RECORDS_PER_TABLE];
        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            tableBInserts[i] = "INSERT INTO tableb VALUES(" + id + ", 'b')";
        }
        connection.execute(tableBInserts);
        connection.setAutoCommit(true);

        connection.execute("UPDATE tableb SET colb='z'");

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
        final List<SourceRecord> tableB = records.recordsForTopic("server1.dbo.tableb");
        Assertions.assertThat(tableB).hasSize(RECORDS_PER_TABLE * 2);
        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final SourceRecord recordB = tableB.get(i);
            final List<SchemaAndValueField> expectedRowB = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, i + ID_START),
                    new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));

            final Struct keyB = (Struct) recordB.key();
            final Struct valueB = (Struct) recordB.value();
            assertRecord((Struct) valueB.get("after"), expectedRowB);
            assertNull(valueB.get("before"));
        }

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final SourceRecord recordB = tableB.get(i + RECORDS_PER_TABLE);
            final List<SchemaAndValueField> expectedBefore = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, i + ID_START),
                    new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));
            final List<SchemaAndValueField> expectedAfter = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, i + ID_START),
                    new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "z"));

            final Struct keyB = (Struct) recordB.key();
            final Struct valueB = (Struct) recordB.value();
            assertRecord((Struct) valueB.get("before"), expectedBefore);
            assertRecord((Struct) valueB.get("after"), expectedAfter);
        }

        stopConnector();
    }

    @Test
    public void updatePrimaryKey() throws Exception {

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Testing.Print.enable();
        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        connection.execute("INSERT INTO tableb VALUES(1, 'b')");
        consumeRecordsByTopic(1);

        connection.setAutoCommit(false);

        connection.execute(
                "UPDATE tablea SET id=100 WHERE id=1",
                "UPDATE tableb SET id=100 WHERE id=1"
        );

        final SourceRecords records = consumeRecordsByTopic(6);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.dbo.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("server1.dbo.tableb");
        Assertions.assertThat(tableA).hasSize(3);
        Assertions.assertThat(tableB).hasSize(3);

        final List<SchemaAndValueField> expectedDeleteRowA = Arrays.asList(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 1),
                new SchemaAndValueField("cola", Schema.OPTIONAL_STRING_SCHEMA, "a"));
        final List<SchemaAndValueField> expectedDeleteKeyA = Arrays.asList(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 1));
        final List<SchemaAndValueField> expectedInsertRowA = Arrays.asList(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 100),
                new SchemaAndValueField("cola", Schema.OPTIONAL_STRING_SCHEMA, "a"));
        final List<SchemaAndValueField> expectedInsertKeyA = Arrays.asList(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 100));

        final SourceRecord deleteRecordA = tableA.get(0);
        final SourceRecord tombstoneRecordA = tableA.get(1);
        final SourceRecord insertRecordA = tableA.get(2);

        final Struct deleteKeyA = (Struct) deleteRecordA.key();
        final Struct deleteValueA = (Struct) deleteRecordA.value();
        assertRecord(deleteValueA.getStruct("before"), expectedDeleteRowA);
        assertRecord(deleteKeyA, expectedDeleteKeyA);
        assertNull(deleteValueA.get("after"));

        final Struct tombstoneKeyA = (Struct) tombstoneRecordA.key();
        final Struct tombstoneValueA = (Struct) tombstoneRecordA.value();
        assertRecord(tombstoneKeyA, expectedDeleteKeyA);
        assertNull(tombstoneValueA);

        final Struct insertKeyA = (Struct) insertRecordA.key();
        final Struct insertValueA = (Struct) insertRecordA.value();
        assertRecord(insertValueA.getStruct("after"), expectedInsertRowA);
        assertRecord(insertKeyA, expectedInsertKeyA);
        assertNull(insertValueA.get("before"));

        final List<SchemaAndValueField> expectedDeleteRowB = Arrays.asList(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 1),
                new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));
        final List<SchemaAndValueField> expectedDeleteKeyB = Arrays.asList(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 1));
        final List<SchemaAndValueField> expectedInsertRowB = Arrays.asList(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 100),
                new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));
        final List<SchemaAndValueField> expectedInsertKeyB = Arrays.asList(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 100));

        final SourceRecord deleteRecordB = tableB.get(0);
        final SourceRecord tombstoneRecordB = tableB.get(1);
        final SourceRecord insertRecordB = tableB.get(2);

        final Struct deletekeyB = (Struct) deleteRecordB.key();
        final Struct deleteValueB = (Struct) deleteRecordB.value();
        assertRecord(deleteValueB.getStruct("before"), expectedDeleteRowB);
        assertRecord(deletekeyB, expectedDeleteKeyB);
        assertNull(deleteValueB.get("after"));

        final Struct tombstonekeyB = (Struct) tombstoneRecordB.key();
        final Struct tombstoneValueB = (Struct) tombstoneRecordB.value();
        assertRecord(tombstonekeyB, expectedDeleteKeyB);
        assertNull(tombstoneValueB);

        final Struct insertkeyB = (Struct) insertRecordB.key();
        final Struct insertValueB = (Struct) insertRecordB.value();
        assertRecord(insertValueB.getStruct("after"), expectedInsertRowB);
        assertRecord(insertkeyB, expectedInsertKeyB);
        assertNull(insertValueB.get("before"));

        stopConnector();
    }

    @Test
    @FixFor("DBZ-1152")
    public void updatePrimaryKeyWithRestartInMiddle() throws Exception {

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        start(SqlServerConnector.class, config, record -> {
            final Struct envelope = (Struct) record.value();
            return envelope != null && "c".equals(envelope.get("op")) && (envelope.getStruct("after").getInt32("id") == 100);
        });
        assertConnectorIsRunning();

        // Testing.Print.enable();
        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        connection.execute("INSERT INTO tableb VALUES(1, 'b')");
        consumeRecordsByTopic(1);

        connection.setAutoCommit(false);

        connection.execute(
                "UPDATE tablea SET id=100 WHERE id=1",
                "UPDATE tableb SET id=100 WHERE id=1"
        );

        final SourceRecords records1 = consumeRecordsByTopic(2);
        stopConnector();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        final SourceRecords records2 = consumeRecordsByTopic(4);

        final List<SourceRecord> tableA = records1.recordsForTopic("server1.dbo.tablea");
        tableA.addAll(records2.recordsForTopic("server1.dbo.tablea"));
        final List<SourceRecord> tableB = records2.recordsForTopic("server1.dbo.tableb");
        Assertions.assertThat(tableA).hasSize(3);
        Assertions.assertThat(tableB).hasSize(3);

        final List<SchemaAndValueField> expectedDeleteRowA = Arrays.asList(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 1),
                new SchemaAndValueField("cola", Schema.OPTIONAL_STRING_SCHEMA, "a"));
        final List<SchemaAndValueField> expectedDeleteKeyA = Arrays.asList(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 1));
        final List<SchemaAndValueField> expectedInsertRowA = Arrays.asList(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 100),
                new SchemaAndValueField("cola", Schema.OPTIONAL_STRING_SCHEMA, "a"));
        final List<SchemaAndValueField> expectedInsertKeyA = Arrays.asList(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 100));

        final SourceRecord deleteRecordA = tableA.get(0);
        final SourceRecord tombstoneRecordA = tableA.get(1);
        final SourceRecord insertRecordA = tableA.get(2);

        final Struct deleteKeyA = (Struct) deleteRecordA.key();
        final Struct deleteValueA = (Struct) deleteRecordA.value();
        assertRecord(deleteValueA.getStruct("before"), expectedDeleteRowA);
        assertRecord(deleteKeyA, expectedDeleteKeyA);
        assertNull(deleteValueA.get("after"));

        final Struct tombstoneKeyA = (Struct) tombstoneRecordA.key();
        final Struct tombstoneValueA = (Struct) tombstoneRecordA.value();
        assertRecord(tombstoneKeyA, expectedDeleteKeyA);
        assertNull(tombstoneValueA);

        final Struct insertKeyA = (Struct) insertRecordA.key();
        final Struct insertValueA = (Struct) insertRecordA.value();
        assertRecord(insertValueA.getStruct("after"), expectedInsertRowA);
        assertRecord(insertKeyA, expectedInsertKeyA);
        assertNull(insertValueA.get("before"));

        final List<SchemaAndValueField> expectedDeleteRowB = Arrays.asList(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 1),
                new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));
        final List<SchemaAndValueField> expectedDeleteKeyB = Arrays.asList(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 1));
        final List<SchemaAndValueField> expectedInsertRowB = Arrays.asList(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 100),
                new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));
        final List<SchemaAndValueField> expectedInsertKeyB = Arrays.asList(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, 100));

        final SourceRecord deleteRecordB = tableB.get(0);
        final SourceRecord tombstoneRecordB = tableB.get(1);
        final SourceRecord insertRecordB = tableB.get(2);

        final Struct deletekeyB = (Struct) deleteRecordB.key();
        final Struct deleteValueB = (Struct) deleteRecordB.value();
        assertRecord(deleteValueB.getStruct("before"), expectedDeleteRowB);
        assertRecord(deletekeyB, expectedDeleteKeyB);
        assertNull(deleteValueB.get("after"));

        final Struct tombstonekeyB = (Struct) tombstoneRecordB.key();
        final Struct tombstoneValueB = (Struct) tombstoneRecordB.value();
        assertRecord(tombstonekeyB, expectedDeleteKeyB);
        assertNull(tombstoneValueB);

        final Struct insertkeyB = (Struct) insertRecordB.key();
        final Struct insertValueB = (Struct) insertRecordB.value();
        assertRecord(insertValueB.getStruct("after"), expectedInsertRowB);
        assertRecord(insertkeyB, expectedInsertKeyB);
        assertNull(insertValueB.get("before"));

        stopConnector();
    }

    @Test
    public void streamChangesWhileStopped() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 10;
        final int ID_RESTART = 100;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')"
            );
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')"
            );
        }

        consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        stopConnector();
        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_RESTART + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')"
            );
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')"
            );
        }

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.dbo.tablea");
        List<SourceRecord> tableB = records.recordsForTopic("server1.dbo.tableb");

        Assertions.assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(tableB).hasSize(RECORDS_PER_TABLE);

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
        }
    }

    @Test
    @FixFor("DBZ-1069")
    public void verifyOffsets() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 10;
        final int ID_RESTART = 100;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')"
            );
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')"
            );
        }

        for (int i = 0; !connection.getMaxLsn().isAvailable(); i++) {
            if (i == 30) {
                org.junit.Assert.fail("Initial changes not written to CDC structures");
            }
            Testing.debug("Waiting for initial changes to be propagated to CDC structures");
            Thread.sleep(1000);
        }
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        List<SourceRecord> records = consumeRecordsByTopic(1 + RECORDS_PER_TABLE * TABLES).allRecordsInOrder();
        records = records.subList(1, records.size());
        for (Iterator<SourceRecord> it = records.iterator(); it.hasNext();) {
            SourceRecord record = it.next();
            assertThat(record.sourceOffset().get("snapshot")).as("Snapshot phase").isEqualTo(true);
            if (it.hasNext()) {
                assertThat(record.sourceOffset().get("snapshot_completed")).as("Snapshot in progress").isEqualTo(false);
            }
            else {
                assertThat(record.sourceOffset().get("snapshot_completed")).as("Snapshot completed").isEqualTo(true);
            }
        }

        stopConnector();
        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_RESTART + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')"
            );
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')"
            );
        }

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final SourceRecords sourceRecords = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        final List<SourceRecord> tableA = sourceRecords.recordsForTopic("server1.dbo.tablea");
        final List<SourceRecord> tableB = sourceRecords.recordsForTopic("server1.dbo.tableb");

        Assertions.assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(tableB).hasSize(RECORDS_PER_TABLE);

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

            assertThat(recordA.sourceOffset().get("snapshot")).as("Streaming phase").isNull();
            assertThat(recordA.sourceOffset().get("snapshot_completed")).as("Streaming phase").isNull();
            assertThat(recordA.sourceOffset().get("change_lsn")).as("LSN present").isNotNull();

            assertThat(recordB.sourceOffset().get("snapshot")).as("Streaming phase").isNull();
            assertThat(recordB.sourceOffset().get("snapshot_completed")).as("Streaming phase").isNull();
            assertThat(recordB.sourceOffset().get("change_lsn")).as("LSN present").isNotNull();
        }
    }

    @Test
    public void whitelistTable() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 1;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_SCHEMA_ONLY)
                .with(SqlServerConnectorConfig.TABLE_WHITELIST, "dbo.tableb")
                .build();
        connection.execute(
                "INSERT INTO tableb VALUES(1, 'b')"
        );

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')"
            );
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')"
            );
        }

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.dbo.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("server1.dbo.tableb");
        Assertions.assertThat(tableA == null || tableA.isEmpty()).isTrue();
        Assertions.assertThat(tableB).hasSize(RECORDS_PER_TABLE);

        stopConnector();
    }

    @Test
    public void blacklistTable() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 1;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_BLACKLIST, "dbo.tablea")
                .build();
        connection.execute(
                "INSERT INTO tableb VALUES(1, 'b')"
        );

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')"
            );
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')"
            );
        }

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.dbo.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("server1.dbo.tableb");
        Assertions.assertThat(tableA == null || tableA.isEmpty()).isTrue();
        Assertions.assertThat(tableB).hasSize(RECORDS_PER_TABLE);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-1067")
    public void blacklistColumn() throws Exception {
        connection.execute(
                "CREATE TABLE blacklist_column_table_a (id int, name varchar(30), amount integer primary key(id))",
                "CREATE TABLE blacklist_column_table_b (id int, name varchar(30), amount integer primary key(id))"
        );
        TestHelper.enableTableCdc(connection, "blacklist_column_table_a");
        TestHelper.enableTableCdc(connection, "blacklist_column_table_b");

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_SCHEMA_ONLY)
                .with(SqlServerConnectorConfig.COLUMN_BLACKLIST, "dbo.blacklist_column_table_a.amount")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        connection.execute("INSERT INTO blacklist_column_table_a VALUES(10, 'some_name', 120)");
        connection.execute("INSERT INTO blacklist_column_table_b VALUES(11, 'some_name', 447)");

        final SourceRecords records = consumeRecordsByTopic(2);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.dbo.blacklist_column_table_a");
        final List<SourceRecord> tableB = records.recordsForTopic("server1.dbo.blacklist_column_table_b");

        Schema expectedSchemaA = SchemaBuilder.struct()
                .optional()
                .name("server1.dbo.blacklist_column_table_a.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Struct expectedValueA = new Struct(expectedSchemaA)
                .put("id", 10)
                .put("name", "some_name");

        Schema expectedSchemaB = SchemaBuilder.struct()
                .optional()
                .name("server1.dbo.blacklist_column_table_b.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("amount", Schema.OPTIONAL_INT32_SCHEMA)
                .build();
        Struct expectedValueB = new Struct(expectedSchemaB)
                .put("id", 11)
                .put("name", "some_name")
                .put("amount", 447);

        Assertions.assertThat(tableA).hasSize(1);
        SourceRecordAssert.assertThat(tableA.get(0))
                .valueAfterFieldIsEqualTo(expectedValueA)
                .valueAfterFieldSchemaIsEqualTo(expectedSchemaA);

        Assertions.assertThat(tableB).hasSize(1);
        SourceRecordAssert.assertThat(tableB.get(0))
                .valueAfterFieldIsEqualTo(expectedValueB)
                .valueAfterFieldSchemaIsEqualTo(expectedSchemaB);

        stopConnector();
    }

    /**
     * Passing the "applicationName" property which can be asserted from the connected sessions".
     */
    @Test
    @FixFor("DBZ-964")
    public void shouldPropagateDatabaseDriverProperties() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_SCHEMA_ONLY)
                .with("database.applicationName", "Debezium App DBZ-964")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // consuming one record to make sure the connector establishes the DB connection which happens asynchronously
        // after the start() call
        connection.execute("INSERT INTO tablea VALUES(964, 'a')");
        consumeRecordsByTopic(1);

        connection.query("select count(1) from sys.dm_exec_sessions where program_name = 'Debezium App DBZ-964'", rs -> {
            rs.next();
            assertThat(rs.getInt(1)).isGreaterThanOrEqualTo(1);
        });
    }

    private void restartInTheMiddleOfTx(boolean restartJustAfterSnapshot, boolean afterStreaming) throws Exception {
        final int RECORDS_PER_TABLE = 30;
        final int TABLES = 2;
        final int ID_START = 10;
        final int ID_RESTART = 1000;
        final int HALF_ID = ID_START + RECORDS_PER_TABLE / 2;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        if (restartJustAfterSnapshot) {
            start(SqlServerConnector.class, config);
            assertConnectorIsRunning();

            // Wait for snapshot to be completed
            consumeRecordsByTopic(1);
            stopConnector();
            connection.execute("INSERT INTO tablea VALUES(-1, '-a')");
        }

        start(SqlServerConnector.class, config, record -> {
            if (!"server1.dbo.tablea.Envelope".equals(record.valueSchema().name())) {
                return false;
            }
            final Struct envelope = (Struct) record.value();
            final Struct after = envelope.getStruct("after");
            final Integer id = after.getInt32("id");
            final String value = after.getString("cola");
            return id != null && id == HALF_ID && "a".equals(value);
        });
        assertConnectorIsRunning();

        // Wait for snapshot to be completed or a first streaming message delivered
        consumeRecordsByTopic(1);

        if (afterStreaming) {
            connection.execute("INSERT INTO tablea VALUES(-2, '-a')");
            final SourceRecords records = consumeRecordsByTopic(1);
            final List<SchemaAndValueField> expectedRow = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, -2),
                    new SchemaAndValueField("cola", Schema.OPTIONAL_STRING_SCHEMA, "-a"));
            assertRecord(((Struct) records.allRecordsInOrder().get(0).value()).getStruct(Envelope.FieldName.AFTER), expectedRow);
        }

        connection.setAutoCommit(false);
        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.executeWithoutCommitting(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')"
            );
            connection.executeWithoutCommitting(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')"
            );
        }
        connection.connection().commit();
        List<SourceRecord> records = consumeRecordsByTopic(RECORDS_PER_TABLE).allRecordsInOrder();

        assertThat(records).hasSize(RECORDS_PER_TABLE);
        SourceRecord lastRecordForOffset = records.get(RECORDS_PER_TABLE - 1);
        Struct value = (Struct) lastRecordForOffset.value();
        final List<SchemaAndValueField> expectedLastRow = Arrays.asList(
                new SchemaAndValueField("id", Schema.INT32_SCHEMA, HALF_ID - 1),
                new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));
        assertRecord((Struct) value.get("after"), expectedLastRow);

        stopConnector();
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords sourceRecords = consumeRecordsByTopic(RECORDS_PER_TABLE);
        records = sourceRecords.allRecordsInOrder();
        assertThat(records).hasSize(RECORDS_PER_TABLE);

        List<SourceRecord> tableA = sourceRecords.recordsForTopic("server1.dbo.tablea");
        List<SourceRecord> tableB = sourceRecords.recordsForTopic("server1.dbo.tableb");
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
        }

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_RESTART + i;
            connection.executeWithoutCommitting(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')"
            );
            connection.executeWithoutCommitting(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')"
            );
            connection.connection().commit();
        }

        sourceRecords = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        tableA = sourceRecords.recordsForTopic("server1.dbo.tablea");
        tableB = sourceRecords.recordsForTopic("server1.dbo.tableb");

        Assertions.assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(tableB).hasSize(RECORDS_PER_TABLE);

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
        }
    }

    @Test
    @FixFor("DBZ-1128")
    public void restartInTheMiddleOfTxAfterSnapshot() throws Exception {
        restartInTheMiddleOfTx(true, false);
    }

    @Test
    @FixFor("DBZ-1128")
    public void restartInTheMiddleOfTxAfterCompletedTx() throws Exception {
        restartInTheMiddleOfTx(false, true);
    }

    @Test
    @FixFor("DBZ-1128")
    public void restartInTheMiddleOfTx() throws Exception {
        restartInTheMiddleOfTx(false, false);
    }

    @Test
    @FixFor("DBZ-1242")
    public void testEmptySchemaWarningAfterApplyingFilters() throws Exception {
        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor();

        Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_WHITELIST, "my_products")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);

        stopConnector(value -> assertThat(logInterceptor.containsWarnMessage(NO_MONITORED_TABLES_WARNING)).isTrue());
    }

    @Test
    @FixFor("DBZ-1242")
    public void testNoEmptySchemaWarningAfterApplyingFilters() throws Exception {
        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor();

        Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);

        stopConnector(value -> assertThat(logInterceptor.containsWarnMessage(NO_MONITORED_TABLES_WARNING)).isFalse());
    }

    private void assertRecord(Struct record, List<SchemaAndValueField> expected) {
        expected.forEach(schemaAndValueField -> schemaAndValueField.assertFor(record));
    }
}
