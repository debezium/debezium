/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertNull;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.db2.Db2ConnectorConfig.SnapshotMode;
import io.debezium.connector.db2.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.SchemaAndValueField;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.util.Testing;

/**
 * Integration test for the Debezium DB2 connector.
 *
 * @author Jiri Pechanec, Luis Garcés-Erice, Peter Urbanetz
 */
public class Db2ConnectorIT extends AbstractConnectorTest {

    private Db2Connection connection;

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        connection.execute("DELETE FROM ASNCDC.IBMSNAP_REGISTER");
        connection.execute(
                "CREATE TABLE tablea (id int not null, cola varchar(30), primary key (id))",
                "CREATE TABLE tableb (id int not null, colb varchar(30), primary key (id))",
                "INSERT INTO tablea VALUES(1, 'a')");
        TestHelper.enableTableCdc(connection, "TABLEA");
        TestHelper.enableTableCdc(connection, "TABLEB");
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
        Testing.Print.enable();
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            TestHelper.disableDbCdc(connection);
            TestHelper.disableTableCdc(connection, "TABLEB");
            TestHelper.disableTableCdc(connection, "TABLEA");
            connection.execute("DROP TABLE tablea", "DROP TABLE tableb");
            connection.execute("DELETE FROM ASNCDC.IBMSNAP_REGISTER");
            connection.execute("DELETE FROM ASNCDC.IBMQREP_COLVERSION");
            connection.execute("DELETE FROM ASNCDC.IBMQREP_TABVERSION");
            connection.close();
        }
    }

    @Test
    public void deleteWithoutTombstone() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(Db2ConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(Db2ConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .build();

        start(Db2Connector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        TestHelper.enableDbCdc(connection);
        connection.execute("UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE = 'A' WHERE SOURCE_OWNER = 'DB2INST1'");
        TestHelper.refreshAndWait(connection);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        TestHelper.refreshAndWait(connection);

        consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);

        connection.execute("DELETE FROM tableB");

        TestHelper.refreshAndWait(connection);

        final SourceRecords deleteRecords = consumeRecordsByTopic(RECORDS_PER_TABLE);
        final List<SourceRecord> deleteTableA = deleteRecords.recordsForTopic("testdb.DB2INST1.TABLEA");
        final List<SourceRecord> deleteTableB = deleteRecords.recordsForTopic("testdb.DB2INST1.TABLEB");
        Assertions.assertThat(deleteTableA).isNullOrEmpty();
        Assertions.assertThat(deleteTableB).hasSize(RECORDS_PER_TABLE);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final SourceRecord deleteRecord = deleteTableB.get(i);
            final List<SchemaAndValueField> expectedDeleteRow = Arrays.asList(
                    new SchemaAndValueField("ID", Schema.INT32_SCHEMA, i + ID_START),
                    new SchemaAndValueField("COLB", Schema.OPTIONAL_STRING_SCHEMA, "b"));

            final Struct deleteValue = (Struct) deleteRecord.value();
            assertRecord((Struct) deleteValue.get("before"), expectedDeleteRow);
            assertNull(deleteValue.get("after"));
        }

        stopConnector();
    }

    @Test
    public void updatePrimaryKey() throws Exception {

        final Configuration config = TestHelper.defaultConfig()
                .with(Db2ConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        start(Db2Connector.class, config);
        assertConnectorIsRunning();

        connection.execute("INSERT INTO tableb VALUES(1, 'b')");
        consumeRecordsByTopic(2);

        TestHelper.enableDbCdc(connection);
        connection.execute("UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE = 'A' WHERE SOURCE_OWNER = 'DB2INST1'");
        TestHelper.refreshAndWait(connection);

        connection.setAutoCommit(false);

        connection.execute(
                "UPDATE tablea SET id=100 WHERE id=1",
                "UPDATE tableb SET id=100 WHERE id=1");

        TestHelper.refreshAndWait(connection);

        final SourceRecords records = consumeRecordsByTopic(6);
        final List<SourceRecord> tableA = records.recordsForTopic("testdb.DB2INST1.TABLEA");
        final List<SourceRecord> tableB = records.recordsForTopic("testdb.DB2INST1.TABLEB");
        Assertions.assertThat(tableA).hasSize(3);
        Assertions.assertThat(tableB).hasSize(3);

        final List<SchemaAndValueField> expectedDeleteRowA = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 1),
                new SchemaAndValueField("COLA", Schema.OPTIONAL_STRING_SCHEMA, "a"));
        final List<SchemaAndValueField> expectedDeleteKeyA = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 1));
        final List<SchemaAndValueField> expectedInsertRowA = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 100),
                new SchemaAndValueField("COLA", Schema.OPTIONAL_STRING_SCHEMA, "a"));
        final List<SchemaAndValueField> expectedInsertKeyA = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 100));

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
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 1),
                new SchemaAndValueField("COLB", Schema.OPTIONAL_STRING_SCHEMA, "b"));
        final List<SchemaAndValueField> expectedDeleteKeyB = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 1));
        final List<SchemaAndValueField> expectedInsertRowB = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 100),
                new SchemaAndValueField("COLB", Schema.OPTIONAL_STRING_SCHEMA, "b"));
        final List<SchemaAndValueField> expectedInsertKeyB = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 100));

        final SourceRecord deleteRecordB = tableB.get(0);
        final SourceRecord tombstoneRecordB = tableB.get(1);
        final SourceRecord insertRecordB = tableB.get(2);

        final Struct deletekeyB = (Struct) deleteRecordB.key();
        final Struct deleteValueB = (Struct) deleteRecordB.value();
        assertRecord(deleteValueB.getStruct("before"), expectedDeleteRowB);
        assertRecord(deletekeyB, expectedDeleteKeyB);
        assertNull(deleteValueB.get("after"));
        // assertThat(deleteValueB.getStruct("source").getInt64("event_serial_no")).isEqualTo(1L);

        final Struct tombstonekeyB = (Struct) tombstoneRecordB.key();
        final Struct tombstoneValueB = (Struct) tombstoneRecordB.value();
        assertRecord(tombstonekeyB, expectedDeleteKeyB);
        assertNull(tombstoneValueB);

        final Struct insertkeyB = (Struct) insertRecordB.key();
        final Struct insertValueB = (Struct) insertRecordB.value();
        assertRecord(insertValueB.getStruct("after"), expectedInsertRowB);
        assertRecord(insertkeyB, expectedInsertKeyB);
        assertNull(insertValueB.get("before"));
        // assertThat(insertValueB.getStruct("source").getInt64("event_serial_no")).isEqualTo(2L);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-1152")
    public void updatePrimaryKeyWithRestartInMiddle() throws Exception {

        final Configuration config = TestHelper.defaultConfig()
                .with(Db2ConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        start(Db2Connector.class, config);
        assertConnectorIsRunning();

        connection.execute("INSERT INTO tableb VALUES(1, 'b')");

        TestHelper.enableDbCdc(connection);
        connection.execute("UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE = 'A' WHERE SOURCE_OWNER = 'DB2INST1'");
        TestHelper.refreshAndWait(connection);

        consumeRecordsByTopic(2);

        connection.setAutoCommit(false);

        connection.execute(
                "UPDATE tablea SET id=100 WHERE id=1",
                "UPDATE tableb SET id=100 WHERE id=1");

        TestHelper.refreshAndWait(connection);

        final SourceRecords records1 = consumeRecordsByTopic(2);
        stopConnector();

        start(Db2Connector.class, config);
        assertConnectorIsRunning();

        final SourceRecords records2 = consumeRecordsByTopic(4);

        final List<SourceRecord> tableA = records1.recordsForTopic("testdb.DB2INST1.TABLEA");
        tableA.addAll(records2.recordsForTopic("testdb.DB2INST1.TABLEA"));
        final List<SourceRecord> tableB = records2.recordsForTopic("testdb.DB2INST1.TABLEB");
        Assertions.assertThat(tableA).hasSize(3);
        Assertions.assertThat(tableB).hasSize(3);

        final List<SchemaAndValueField> expectedDeleteRowA = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 1),
                new SchemaAndValueField("COLA", Schema.OPTIONAL_STRING_SCHEMA, "a"));
        final List<SchemaAndValueField> expectedDeleteKeyA = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 1));
        final List<SchemaAndValueField> expectedInsertRowA = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 100),
                new SchemaAndValueField("COLA", Schema.OPTIONAL_STRING_SCHEMA, "a"));
        final List<SchemaAndValueField> expectedInsertKeyA = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 100));

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
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 1),
                new SchemaAndValueField("COLB", Schema.OPTIONAL_STRING_SCHEMA, "b"));
        final List<SchemaAndValueField> expectedDeleteKeyB = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 1));
        final List<SchemaAndValueField> expectedInsertRowB = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 100),
                new SchemaAndValueField("COLB", Schema.OPTIONAL_STRING_SCHEMA, "b"));
        final List<SchemaAndValueField> expectedInsertKeyB = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 100));

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
    @FixFor("DBZ-1069")
    public void verifyOffsets() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 10;
        final int ID_RESTART = 100;
        final Configuration config = TestHelper.defaultConfig()
                .with(Db2ConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        TestHelper.enableDbCdc(connection);
        connection.execute("UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE = 'A' WHERE SOURCE_OWNER = 'DB2INST1'");
        TestHelper.refreshAndWait(connection);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }
        TestHelper.refreshAndWait(connection);
        for (int i = 0; !connection.getMaxLsn().isAvailable(); i++) {
            if (i == 30) {
                org.junit.Assert.fail("Initial changes not written to CDC structures");
            }
            Testing.debug("Waiting for initial changes to be propagated to CDC structures");
            Thread.sleep(1000);
        }
        start(Db2Connector.class, config);
        assertConnectorIsRunning();
        TestHelper.refreshAndWait(connection);

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
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        start(Db2Connector.class, config);
        assertConnectorIsRunning();
        TestHelper.refreshAndWait(connection);

        final SourceRecords sourceRecords = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        final List<SourceRecord> tableA = sourceRecords.recordsForTopic("testdb.DB2INST1.TABLEA");
        final List<SourceRecord> tableB = sourceRecords.recordsForTopic("testdb.DB2INST1.TABLEB");

        Assertions.assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(tableB).hasSize(RECORDS_PER_TABLE);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = i + ID_RESTART;
            final SourceRecord recordA = tableA.get(i);
            final SourceRecord recordB = tableB.get(i);
            final List<SchemaAndValueField> expectedRowA = Arrays.asList(
                    new SchemaAndValueField("ID", Schema.INT32_SCHEMA, id),
                    new SchemaAndValueField("COLA", Schema.OPTIONAL_STRING_SCHEMA, "a"));
            final List<SchemaAndValueField> expectedRowB = Arrays.asList(
                    new SchemaAndValueField("ID", Schema.INT32_SCHEMA, id),
                    new SchemaAndValueField("COLB", Schema.OPTIONAL_STRING_SCHEMA, "b"));

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
                .with(Db2ConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_SCHEMA_ONLY)
                .with(Db2ConnectorConfig.TABLE_WHITELIST, "db2inst1.tableb")
                .build();
        connection.execute(
                "INSERT INTO tableb VALUES(1, 'b')");

        start(Db2Connector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        TestHelper.enableDbCdc(connection);
        connection.execute("UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE = 'A' WHERE SOURCE_OWNER = 'DB2INST1'");
        TestHelper.refreshAndWait(connection);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        TestHelper.refreshAndWait(connection);

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        final List<SourceRecord> tableA = records.recordsForTopic("testdb.DB2INST1.TABLEA");
        final List<SourceRecord> tableB = records.recordsForTopic("testdb.DB2INST1.TABLEB");
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
                .with(Db2ConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(Db2ConnectorConfig.TABLE_BLACKLIST, "db2inst1.tablea")
                .build();
        connection.execute(
                "INSERT INTO tableb VALUES(1, 'b')");

        start(Db2Connector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        TestHelper.enableDbCdc(connection);
        connection.execute("UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE = 'A' WHERE SOURCE_OWNER = 'DB2INST1'");
        TestHelper.refreshAndWait(connection);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        TestHelper.refreshAndWait(connection);

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        final List<SourceRecord> tableA = records.recordsForTopic("testdb.DB2INST1.TABLEA");
        final List<SourceRecord> tableB = records.recordsForTopic("testdb.DB2INST1.TABLEB");
        Assertions.assertThat(tableA == null || tableA.isEmpty()).isTrue();
        Assertions.assertThat(tableB).hasSize(RECORDS_PER_TABLE);

        stopConnector();
    }

    private void restartInTheMiddleOfTx(boolean restartJustAfterSnapshot, boolean afterStreaming) throws Exception {
        final int RECORDS_PER_TABLE = 30;
        final int TABLES = 2;
        final int ID_START = 10;
        final int ID_RESTART = 1000;
        final int HALF_ID = ID_START + RECORDS_PER_TABLE / 2;
        final Configuration config = TestHelper.defaultConfig()
                .with(Db2ConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        if (restartJustAfterSnapshot) {
            start(Db2Connector.class, config);
            assertConnectorIsRunning();

            // Wait for snapshot to be completed
            consumeRecordsByTopic(1);
            stopConnector();
            connection.execute("INSERT INTO tablea VALUES(-1, '-a')");
            TestHelper.refreshAndWait(connection);

        }

        start(Db2Connector.class, config, record -> {
            if (!"testdb.DB2INST1.TABLEA.Envelope".equals(record.valueSchema().name())) {
                return false;
            }
            final Struct envelope = (Struct) record.value();
            final Struct after = envelope.getStruct("after");
            final Integer id = after.getInt32("ID");
            final String value = after.getString("COLA");
            return id != null && id == HALF_ID && "a".equals(value);
        });
        assertConnectorIsRunning();

        // Wait for snapshot to be completed or a first streaming message delivered
        consumeRecordsByTopic(1);

        TestHelper.enableDbCdc(connection);
        connection.execute("UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE = 'A' WHERE SOURCE_OWNER = 'DB2INST1'");
        TestHelper.refreshAndWait(connection);

        if (afterStreaming) {
            connection.execute("INSERT INTO tablea VALUES(-2, '-a')");
            TestHelper.refreshAndWait(connection);
            final SourceRecords records = consumeRecordsByTopic(1);
            final List<SchemaAndValueField> expectedRow = Arrays.asList(
                    new SchemaAndValueField("ID", Schema.INT32_SCHEMA, -2),
                    new SchemaAndValueField("COLA", Schema.OPTIONAL_STRING_SCHEMA, "-a"));
            assertRecord(((Struct) records.allRecordsInOrder().get(0).value()).getStruct(Envelope.FieldName.AFTER), expectedRow);
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

        // TestHelper.waitForCDC();
        TestHelper.refreshAndWait(connection);

        List<SourceRecord> records = consumeRecordsByTopic(RECORDS_PER_TABLE).allRecordsInOrder();

        assertThat(records).hasSize(RECORDS_PER_TABLE);
        SourceRecord lastRecordForOffset = records.get(RECORDS_PER_TABLE - 1);
        Struct value = (Struct) lastRecordForOffset.value();
        final List<SchemaAndValueField> expectedLastRow = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, HALF_ID - 1),
                new SchemaAndValueField("COLB", Schema.OPTIONAL_STRING_SCHEMA, "b"));
        assertRecord((Struct) value.get("after"), expectedLastRow);

        stopConnector();
        start(Db2Connector.class, config);
        assertConnectorIsRunning();

        // TestHelper.waitForCDC();
        TestHelper.refreshAndWait(connection);

        SourceRecords sourceRecords = consumeRecordsByTopic(RECORDS_PER_TABLE);
        records = sourceRecords.allRecordsInOrder();
        assertThat(records).hasSize(RECORDS_PER_TABLE);

        List<SourceRecord> tableA = sourceRecords.recordsForTopic("testdb.DB2INST1.TABLEA");
        List<SourceRecord> tableB = sourceRecords.recordsForTopic("testdb.DB2INST1.TABLEB");
        for (int i = 0; i < RECORDS_PER_TABLE / 2; i++) {
            final int id = HALF_ID + i;
            final SourceRecord recordA = tableA.get(i);
            final SourceRecord recordB = tableB.get(i);
            final List<SchemaAndValueField> expectedRowA = Arrays.asList(
                    new SchemaAndValueField("ID", Schema.INT32_SCHEMA, id),
                    new SchemaAndValueField("COLA", Schema.OPTIONAL_STRING_SCHEMA, "a"));
            final List<SchemaAndValueField> expectedRowB = Arrays.asList(
                    new SchemaAndValueField("ID", Schema.INT32_SCHEMA, id),
                    new SchemaAndValueField("COLB", Schema.OPTIONAL_STRING_SCHEMA, "b"));

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
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.executeWithoutCommitting(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
            connection.connection().commit();
        }

        TestHelper.refreshAndWait(connection);

        sourceRecords = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        tableA = sourceRecords.recordsForTopic("testdb.DB2INST1.TABLEA");
        tableB = sourceRecords.recordsForTopic("testdb.DB2INST1.TABLEB");

        Assertions.assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(tableB).hasSize(RECORDS_PER_TABLE);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = i + ID_RESTART;
            final SourceRecord recordA = tableA.get(i);
            final SourceRecord recordB = tableB.get(i);
            final List<SchemaAndValueField> expectedRowA = Arrays.asList(
                    new SchemaAndValueField("ID", Schema.INT32_SCHEMA, id),
                    new SchemaAndValueField("COLA", Schema.OPTIONAL_STRING_SCHEMA, "a"));
            final List<SchemaAndValueField> expectedRowB = Arrays.asList(
                    new SchemaAndValueField("ID", Schema.INT32_SCHEMA, id),
                    new SchemaAndValueField("COLB", Schema.OPTIONAL_STRING_SCHEMA, "b"));

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
    // @FixFor("DBZ-1128")
    public void restartInTheMiddleOfTx() throws Exception {
        restartInTheMiddleOfTx(false, false);
    }

    @Test
    @FixFor("DBZ-1242")
    public void testEmptySchemaWarningAfterApplyingFilters() throws Exception {
        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor();

        Configuration config = TestHelper.defaultConfig()
                .with(Db2ConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(Db2ConnectorConfig.TABLE_WHITELIST, "my_products")
                .build();

        start(Db2Connector.class, config);
        assertConnectorIsRunning();
        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);

        stopConnector(value -> assertThat(logInterceptor.containsWarnMessage(NO_MONITORED_TABLES_WARNING)).isTrue());
    }

    private void assertRecord(Struct record, List<SchemaAndValueField> expected) {
        expected.forEach(schemaAndValueField -> schemaAndValueField.assertFor(record));
    }
}
