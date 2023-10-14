/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static io.debezium.connector.sqlserver.util.TestHelper.TEST_DATABASE_2;
import static io.debezium.connector.sqlserver.util.TestHelper.TYPE_LENGTH_PARAMETER_KEY;
import static io.debezium.connector.sqlserver.util.TestHelper.TYPE_NAME_PARAMETER_KEY;
import static io.debezium.connector.sqlserver.util.TestHelper.TYPE_SCALE_PARAMETER_KEY;
import static io.debezium.connector.sqlserver.util.TestHelper.waitForStreamingStarted;
import static io.debezium.relational.RelationalDatabaseConnectorConfig.SCHEMA_EXCLUDE_LIST;
import static io.debezium.relational.RelationalDatabaseConnectorConfig.SCHEMA_INCLUDE_LIST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceNotFoundException;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.SchemaAndValueField;
import io.debezium.data.SourceRecordAssert;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.Flaky;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.DatabaseSchema;
import io.debezium.storage.file.history.FileSchemaHistory;
import io.debezium.util.Testing;

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
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("server1.testDB1.dbo.tableb");
        assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        assertThat(tableB).hasSize(RECORDS_PER_TABLE);
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
        final List<SourceRecord> deleteTableA = deleteRecords.recordsForTopic("server1.testDB1.dbo.tablea");
        final List<SourceRecord> deleteTableB = deleteRecords.recordsForTopic("server1.testDB1.dbo.tableb");
        assertThat(deleteTableA).isNullOrEmpty();
        assertThat(deleteTableB).hasSize(2 * RECORDS_PER_TABLE);

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
    @FixFor("DBZ-1642")
    public void readOnlyApplicationIntent() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor(SqlServerSnapshotChangeEventSource.class);
        final String appId = "readOnlyApplicationIntent-" + UUID.randomUUID();

        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with("database.applicationIntent", "ReadOnly")
                .with("database.applicationName", appId)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        TestHelper.waitForSnapshotToBeCompleted();
        consumeRecordsByTopic(1);

        TestHelper.waitForStreamingStarted();
        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES, 24);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("server1.testDB1.dbo.tableb");
        assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        assertThat(tableB).hasSize(RECORDS_PER_TABLE);
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

        assertThat(logInterceptor.containsMessage("Schema locking was disabled in connector configuration")).isTrue();

        // Verify that multiple subsequent transactions are used in streaming phase with read-only intent
        try (SqlServerConnection admin = TestHelper.adminConnection()) {
            final Set<Long> txIds = new HashSet<>();
            Awaitility.await().atMost(TestHelper.waitTimeForRecords() * 5, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> {
                admin.query(
                        "SELECT (SELECT transaction_id FROM sys.dm_tran_session_transactions AS t WHERE s.session_id=t.session_id) FROM sys.dm_exec_sessions AS s WHERE program_name='"
                                + appId + "'",
                        rs -> {
                            while (rs.next()) {
                                final long txId = rs.getLong(1);
                                if (txId != 0) {
                                    txIds.add(txId);
                                }
                            }
                        });
                return txIds.size() > 2;
            });
        }
        stopConnector();
    }

    @Test
    @FixFor("DBZ-1643")
    public void timestampAndTimezone() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 10;

        final TimeZone currentTimeZone = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("Australia/Canberra"));
            final Configuration config = TestHelper.defaultConfig()
                    .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                    .build();

            start(SqlServerConnector.class, config);
            assertConnectorIsRunning();

            // Wait for snapshot completion
            consumeRecordsByTopic(1);

            final Instant now = Instant.now();
            final Instant lowerBound = now.minusSeconds(5 * 60);
            final Instant upperBound = now.plusSeconds(5 * 60);
            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_START + i;
                connection.execute(
                        "INSERT INTO tablea VALUES(" + id + ", 'a')");
                connection.execute(
                        "INSERT INTO tableb VALUES(" + id + ", 'b')");
            }

            final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
            final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.tablea");
            final List<SourceRecord> tableB = records.recordsForTopic("server1.testDB1.dbo.tableb");
            assertThat(tableA).hasSize(RECORDS_PER_TABLE);
            assertThat(tableB).hasSize(RECORDS_PER_TABLE);
            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final SourceRecord recordA = tableA.get(i);
                final long timestamp = ((Struct) recordA.value()).getStruct("source").getInt64("ts_ms");
                final Instant instant = Instant.ofEpochMilli(timestamp);
                assertThat(instant.isAfter(lowerBound) && instant.isBefore(upperBound)).isTrue();
            }
            stopConnector();
        }
        finally {
            TimeZone.setDefault(currentTimeZone);
        }
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
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);

        connection.execute("DELETE FROM tableB");
        final SourceRecords deleteRecords = consumeRecordsByTopic(RECORDS_PER_TABLE);
        final List<SourceRecord> deleteTableA = deleteRecords.recordsForTopic("server1.testDB1.dbo.tablea");
        final List<SourceRecord> deleteTableB = deleteRecords.recordsForTopic("server1.testDB1.dbo.tableb");
        assertThat(deleteTableA).isNullOrEmpty();
        assertThat(deleteTableB).hasSize(RECORDS_PER_TABLE);

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

        // Testing.Print.enable();
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
        final List<SourceRecord> tableB = records.recordsForTopic("server1.testDB1.dbo.tableb");
        assertThat(tableB).hasSize(RECORDS_PER_TABLE * 2);
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
                "UPDATE tableb SET id=100 WHERE id=1");

        final SourceRecords records = consumeRecordsByTopic(6);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("server1.testDB1.dbo.tableb");
        assertThat(tableA).hasSize(3);
        assertThat(tableB).hasSize(3);

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
        assertThat(deleteValueB.getStruct("source").getInt64("event_serial_no")).isEqualTo(1L);

        final Struct tombstonekeyB = (Struct) tombstoneRecordB.key();
        final Struct tombstoneValueB = (Struct) tombstoneRecordB.value();
        assertRecord(tombstonekeyB, expectedDeleteKeyB);
        assertNull(tombstoneValueB);

        final Struct insertkeyB = (Struct) insertRecordB.key();
        final Struct insertValueB = (Struct) insertRecordB.value();
        assertRecord(insertValueB.getStruct("after"), expectedInsertRowB);
        assertRecord(insertkeyB, expectedInsertKeyB);
        assertNull(insertValueB.get("before"));
        assertThat(insertValueB.getStruct("source").getInt64("event_serial_no")).isEqualTo(2L);

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
        TestHelper.waitForDatabaseSnapshotToBeCompleted(TestHelper.TEST_DATABASE_1);
        final SourceRecords snapshotRecords = consumeRecordsByTopic(1);
        assertThat(snapshotRecords.allRecordsInOrder()).hasSize(1);

        connection.execute("INSERT INTO tableb VALUES(1, 'b')");
        final SourceRecords insertRecords = consumeRecordsByTopic(1);
        assertThat(insertRecords.allRecordsInOrder()).hasSize(1);

        connection.setAutoCommit(false);

        connection.execute(
                "UPDATE tablea SET id=100 WHERE id=1",
                "UPDATE tableb SET id=100 WHERE id=1");

        final SourceRecords records1 = consumeRecordsByTopic(2);
        stopConnector();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        final SourceRecords records2 = consumeRecordsByTopic(4);

        final List<SourceRecord> tableA = records1.recordsForTopic("server1.testDB1.dbo.tablea");
        tableA.addAll(records2.recordsForTopic("server1.testDB1.dbo.tablea"));
        final List<SourceRecord> tableB = records2.recordsForTopic("server1.testDB1.dbo.tableb");
        assertThat(tableA).hasSize(3);
        assertThat(tableB).hasSize(3);

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
    @FixFor("DBZ-2329")
    @Flaky("DBZ-4475")
    public void updatePrimaryKeyTwiceWithRestartInMiddleOfTx() throws Exception {

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.MAX_QUEUE_SIZE, 2)
                .with(SqlServerConnectorConfig.MAX_BATCH_SIZE, 1)
                .with(SqlServerConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .build();

        // Testing.Print.enable();
        // Wait for snapshot completion
        start(SqlServerConnector.class, config, record -> {
            final Struct envelope = (Struct) record.value();
            boolean stop = envelope != null && "d".equals(envelope.get("op")) && (envelope.getStruct("before").getInt32("id") == 305);
            return stop;
        });
        assertConnectorIsRunning();

        TestHelper.waitForDatabaseSnapshotToBeCompleted(TestHelper.TEST_DATABASE_1);
        final SourceRecords snapshotRecords = consumeRecordsByTopic(1);
        assertThat(snapshotRecords.allRecordsInOrder()).hasSize(1);

        connection.setAutoCommit(false);

        connection.execute("INSERT INTO tableb (id, colb) values (1,'1')");
        connection.execute("INSERT INTO tableb (id, colb) values (2,'2')");
        connection.execute("INSERT INTO tableb (id, colb) values (3,'3')");
        connection.execute("INSERT INTO tableb (id, colb) values (4,'4')");
        connection.execute("INSERT INTO tableb (id, colb) values (5,'5')");
        consumeRecordsByTopic(5);

        connection.execute("UPDATE tableb set id = colb + 300");
        connection.execute("UPDATE tableb set id = colb + 300");

        final SourceRecords records1 = consumeRecordsByTopic(14);

        stopConnector();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final int expectedRecords = 20;
        final SourceRecords records2 = consumeRecordsByTopic(expectedRecords - records1.allRecordsInOrder().size());

        final List<SourceRecord> tableB = records1.recordsForTopic("server1.testDB1.dbo.tableb");
        tableB.addAll(records2.recordsForTopic("server1.testDB1.dbo.tableb"));

        assertThat(tableB).hasSize(expectedRecords);

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
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        stopConnector();
        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_RESTART + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.tablea");
        List<SourceRecord> tableB = records.recordsForTopic("server1.testDB1.dbo.tableb");

        assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        assertThat(tableB).hasSize(RECORDS_PER_TABLE);

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
    @Ignore // the test is very flaky in CI environment
    public void verifyOffsets() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 10;
        final int ID_RESTART = 100;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        final List<Integer> expectedIds = new ArrayList<>();
        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
            expectedIds.add(id);
        }

        final String tableaCT = connection.getNameOfChangeTable("tablea");
        final String tablebCT = connection.getNameOfChangeTable("tableb");

        TestHelper.waitForCdcRecord(connection, "tableb", rs -> rs.getInt("id") == expectedIds.get(expectedIds.size() - 1));

        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> {
            // Wait for max lsn to be available
            if (!connection.getMaxLsn(TestHelper.TEST_DATABASE_1).isAvailable()) {
                return false;
            }

            // verify pre-snapshot inserts have succeeded
            Map<String, Boolean> resultMap = new HashMap<>();
            connection.getChangeTables(TestHelper.TEST_DATABASE_1).forEach(ct -> {
                final String tableName = ct.getChangeTableId().table();
                if (tableName.endsWith("dbo_" + tableaCT) || tableName.endsWith("dbo_" + tablebCT)) {
                    try {
                        final Lsn minLsn = connection.getMinLsn(TestHelper.TEST_DATABASE_1, tableName);
                        final Lsn maxLsn = connection.getMaxLsn(TestHelper.TEST_DATABASE_1);
                        SqlServerChangeTable[] tables = Collections.singletonList(ct).toArray(new SqlServerChangeTable[]{});
                        final List<Integer> ids = new ArrayList<>();
                        connection.getChangesForTables(TestHelper.TEST_DATABASE_1, tables, minLsn, maxLsn, resultsets -> {
                            final ResultSet rs = resultsets[0];
                            while (rs.next()) {
                                ids.add(rs.getInt("id"));
                            }
                        });
                        if (ids.equals(expectedIds)) {
                            resultMap.put(tableName, true);
                        }
                        else {
                            resultMap.put(tableName, false);
                        }
                    }
                    catch (Exception e) {
                        org.junit.Assert.fail("Failed to fetch changes for table " + tableName + ": " + e.getMessage());
                    }
                }
            });
            return resultMap.values().stream().filter(v -> !v).count() == 0;
        });

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
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final SourceRecords sourceRecords = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        final List<SourceRecord> tableA = sourceRecords.recordsForTopic("server1.testDB1.dbo.tablea");
        final List<SourceRecord> tableB = sourceRecords.recordsForTopic("server1.testDB1.dbo.tableb");

        assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        assertThat(tableB).hasSize(RECORDS_PER_TABLE);

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
    public void testIncludeTable() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 1;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.tableb")
                .build();
        connection.execute(
                "INSERT INTO tableb VALUES(1, 'b')");

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("server1.testDB1.dbo.tableb");
        assertThat(tableA == null || tableA.isEmpty()).isTrue();
        assertThat(tableB).hasSize(RECORDS_PER_TABLE);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-4346")
    public void shouldReportConfigurationErrorForUserNotHavingAccessToCDCTableInInitialMode() throws Exception {
        // First create a new user with only db_datareader role
        String testUserCreateSql = "IF EXISTS (select 1 from sys.server_principals where name = 'test_user')\n"
                + "DROP LOGIN test_user\n"
                + "CREATE LOGIN test_user WITH PASSWORD = 'Password!'\n"
                + "CREATE USER test_user FOR LOGIN test_user\n"
                + "ALTER ROLE db_denydatareader ADD MEMBER test_user";

        connection.execute(testUserCreateSql);

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "^dbo.tableb$")
                .with(SqlServerConnectorConfig.USER, "test_user")
                .build();

        SqlServerConnector connector = new SqlServerConnector();
        Config validatedConfig = connector.validate(config.asMap());

        assertConfigurationErrors(validatedConfig, SqlServerConnectorConfig.USER, 1);
    }

    @Test
    @FixFor("DBZ-4346")
    public void shouldNotReportConfigurationErrorForUserNotHavingAccessToCDCTableInInitialOnlyMode() throws Exception {
        // First create a new user with only db_datareader role
        String testUserCreateSql = "IF EXISTS (select 1 from sys.server_principals where name = 'test_user')\n"
                + "DROP LOGIN test_user\n"
                + "CREATE LOGIN test_user WITH PASSWORD = 'Password!'\n"
                + "CREATE USER test_user FOR LOGIN test_user\n"
                + "ALTER ROLE db_denydatareader ADD MEMBER test_user";

        connection.execute(testUserCreateSql);

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "^dbo.tableb$")
                .with(SqlServerConnectorConfig.USER, "test_user")
                .build();

        SqlServerConnector connector = new SqlServerConnector();
        Config validatedConfig = connector.validate(config.asMap());

        assertNoConfigurationErrors(validatedConfig, SqlServerConnectorConfig.USER);
    }

    @Test
    public void testTableIncludeList() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 1;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "^dbo.tableb$")
                .build();
        connection.execute(
                "INSERT INTO tableb VALUES(1, 'b')");

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        final SourceRecords snapshotRecords = consumeRecordsByTopic(1);
        assertThat(snapshotRecords.recordsForTopic("server1.testDB1.dbo.tableb")).isNotEmpty();

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("server1.testDB1.dbo.tableb");
        assertThat(tableA == null || tableA.isEmpty()).isTrue();
        assertThat(tableB).hasSize(RECORDS_PER_TABLE);

        stopConnector();
    }

    @Test
    public void testTableExcludeList() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 1;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_EXCLUDE_LIST, "dbo.tablea")
                .build();
        connection.execute(
                "INSERT INTO tableb VALUES(1, 'b')");

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("server1.testDB1.dbo.tableb");
        assertThat(tableA == null || tableA.isEmpty()).isTrue();
        assertThat(tableB).hasSize(RECORDS_PER_TABLE);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-1617")
    public void blacklistColumnWhenCdcColumnsDoNotMatchWithOriginalSnapshot() throws Exception {
        connection.execute("CREATE TABLE table_a (id int, name varchar(30), amount integer primary key(id))");
        TestHelper.enableTableCdc(connection, "table_a");

        connection.execute("ALTER TABLE table_a ADD blacklisted_column varchar(30)");

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(SqlServerConnectorConfig.COLUMN_EXCLUDE_LIST, "dbo.table_a.blacklisted_column")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        connection.execute("INSERT INTO table_a VALUES(10, 'some_name', 120, 'some_string')");

        final SourceRecords records = consumeRecordsByTopic(1);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.table_a");

        Schema expectedSchemaA = SchemaBuilder.struct()
                .optional()
                .name("server1.testDB1.dbo.table_a.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("amount", Schema.OPTIONAL_INT32_SCHEMA)
                .build();
        Struct expectedValueA = new Struct(expectedSchemaA)
                .put("id", 10)
                .put("name", "some_name")
                .put("amount", 120);

        assertThat(tableA).hasSize(1);
        SourceRecordAssert.assertThat(tableA.get(0))
                .valueAfterFieldIsEqualTo(expectedValueA)
                .valueAfterFieldSchemaIsEqualTo(expectedSchemaA);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-1067")
    public void testColumnExcludeList() throws Exception {
        connection.execute(
                "CREATE TABLE blacklist_column_table_a (id int, name varchar(30), amount integer primary key(id))",
                "CREATE TABLE blacklist_column_table_b (id int, name varchar(30), amount integer primary key(id))");
        TestHelper.enableTableCdc(connection, "blacklist_column_table_a");
        TestHelper.enableTableCdc(connection, "blacklist_column_table_b");

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(SqlServerConnectorConfig.COLUMN_EXCLUDE_LIST, "dbo.blacklist_column_table_a.amount")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        connection.execute("INSERT INTO blacklist_column_table_a VALUES(10, 'some_name', 120)");
        connection.execute("INSERT INTO blacklist_column_table_b VALUES(11, 'some_name', 447)");

        final SourceRecords records = consumeRecordsByTopic(2);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.blacklist_column_table_a");
        final List<SourceRecord> tableB = records.recordsForTopic("server1.testDB1.dbo.blacklist_column_table_b");

        Schema expectedSchemaA = SchemaBuilder.struct()
                .optional()
                .name("server1.testDB1.dbo.blacklist_column_table_a.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Struct expectedValueA = new Struct(expectedSchemaA)
                .put("id", 10)
                .put("name", "some_name");

        Schema expectedSchemaB = SchemaBuilder.struct()
                .optional()
                .name("server1.testDB1.dbo.blacklist_column_table_b.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("amount", Schema.OPTIONAL_INT32_SCHEMA)
                .build();
        Struct expectedValueB = new Struct(expectedSchemaB)
                .put("id", 11)
                .put("name", "some_name")
                .put("amount", 447);

        assertThat(tableA).hasSize(1);
        SourceRecordAssert.assertThat(tableA.get(0))
                .valueAfterFieldIsEqualTo(expectedValueA)
                .valueAfterFieldSchemaIsEqualTo(expectedSchemaA);

        assertThat(tableB).hasSize(1);
        SourceRecordAssert.assertThat(tableB.get(0))
                .valueAfterFieldIsEqualTo(expectedValueB)
                .valueAfterFieldSchemaIsEqualTo(expectedSchemaB);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-2522")
    public void testColumnIncludeList() throws Exception {
        connection.execute(
                "CREATE TABLE include_list_column_table_a (id int, name varchar(30), amount integer primary key(id))",
                "CREATE TABLE include_list_column_table_b (id int, name varchar(30), amount integer primary key(id))");
        TestHelper.enableTableCdc(connection, "include_list_column_table_a");
        TestHelper.enableTableCdc(connection, "include_list_column_table_b");

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(SqlServerConnectorConfig.COLUMN_INCLUDE_LIST, ".*id,.*name,dbo.include_list_column_table_b.amount")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        connection.execute("INSERT INTO include_list_column_table_a VALUES(10, 'some_name', 120)");
        connection.execute("INSERT INTO include_list_column_table_b VALUES(11, 'some_name', 447)");

        final SourceRecords records = consumeRecordsByTopic(2);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.include_list_column_table_a");
        final List<SourceRecord> tableB = records.recordsForTopic("server1.testDB1.dbo.include_list_column_table_b");

        Schema expectedSchemaA = SchemaBuilder.struct()
                .optional()
                .name("server1.testDB1.dbo.include_list_column_table_a.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Struct expectedValueA = new Struct(expectedSchemaA)
                .put("id", 10)
                .put("name", "some_name");

        Schema expectedSchemaB = SchemaBuilder.struct()
                .optional()
                .name("server1.testDB1.dbo.include_list_column_table_b.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("amount", Schema.OPTIONAL_INT32_SCHEMA)
                .build();
        Struct expectedValueB = new Struct(expectedSchemaB)
                .put("id", 11)
                .put("name", "some_name")
                .put("amount", 447);

        assertThat(tableA).hasSize(1);
        SourceRecordAssert.assertThat(tableA.get(0))
                .valueAfterFieldIsEqualTo(expectedValueA)
                .valueAfterFieldSchemaIsEqualTo(expectedSchemaA);

        assertThat(tableB).hasSize(1);
        SourceRecordAssert.assertThat(tableB.get(0))
                .valueAfterFieldIsEqualTo(expectedValueB)
                .valueAfterFieldSchemaIsEqualTo(expectedSchemaB);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-3505")
    public void shouldHandleInvalidColumnFilter() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.COLUMN_INCLUDE_LIST, ".^")
                .build();
        final LogInterceptor logInterceptor = new LogInterceptor(RelationalSnapshotChangeEventSource.class);

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        // should be no more records
        assertNoRecordsToConsume();

        final String message = "All columns in table testDB1.dbo.tablea were excluded due to include/exclude lists, defaulting to selecting all columns";
        stopConnector(value -> assertThat(logInterceptor.containsMessage(message)).isTrue());
    }

    @Test
    @FixFor("DBZ-1692")
    public void shouldConsumeEventsWithMaskedHashedColumns() throws Exception {
        connection.execute(
                "CREATE TABLE masked_hashed_column_table_a (id int, name varchar(255) primary key(id))",
                "CREATE TABLE masked_hashed_column_table_b (id int, name varchar(20), primary key(id))");
        TestHelper.enableTableCdc(connection, "masked_hashed_column_table_a");
        TestHelper.enableTableCdc(connection, "masked_hashed_column_table_b");

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with("column.mask.hash.SHA-256.with.salt.CzQMA0cB5K", "testDB1.dbo.masked_hashed_column_table_a.name, testDB1.dbo.masked_hashed_column_table_b.name")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        connection.execute("INSERT INTO masked_hashed_column_table_a VALUES(10, 'some_name')");
        connection.execute("INSERT INTO masked_hashed_column_table_b VALUES(11, 'some_name')");

        final SourceRecords records = consumeRecordsByTopic(2);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.masked_hashed_column_table_a");
        final List<SourceRecord> tableB = records.recordsForTopic("server1.testDB1.dbo.masked_hashed_column_table_b");

        assertThat(tableA).hasSize(1);
        SourceRecord record = tableA.get(0);
        VerifyRecord.isValidInsert(record, "id", 10);

        Struct value = (Struct) record.value();
        if (value.getStruct("after") != null) {
            assertThat(value.getStruct("after").getString("name")).isEqualTo("3b225d0696535d66f2c0fb2e36b012c520d396af3dd8f18330b9c9cd23ca714e");
        }

        assertThat(tableB).hasSize(1);
        record = tableB.get(0);
        VerifyRecord.isValidInsert(record, "id", 11);

        value = (Struct) record.value();
        if (value.getStruct("after") != null) {
            assertThat(value.getStruct("after").getString("name")).isEqualTo("3b225d0696535d66f2c0");
        }

        stopConnector();
    }

    @Test
    @FixFor("DBZ-1972")
    public void shouldConsumeEventsWithMaskedAndTruncatedColumns() throws Exception {
        connection.execute(
                "CREATE TABLE masked_hashed_column_table (id int, name varchar(255) primary key(id))",
                "CREATE TABLE truncated_column_table (id int, name varchar(20), primary key(id))");
        TestHelper.enableTableCdc(connection, "masked_hashed_column_table");
        TestHelper.enableTableCdc(connection, "truncated_column_table");

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with("column.mask.with.12.chars", "testDB1.dbo.masked_hashed_column_table.name")
                .with("column.truncate.to.4.chars", "testDB1.dbo.truncated_column_table.name")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        connection.execute("INSERT INTO masked_hashed_column_table VALUES(10, 'some_name')");
        connection.execute("INSERT INTO truncated_column_table VALUES(11, 'some_name')");

        final SourceRecords records = consumeRecordsByTopic(2);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.masked_hashed_column_table");
        final List<SourceRecord> tableB = records.recordsForTopic("server1.testDB1.dbo.truncated_column_table");

        assertThat(tableA).hasSize(1);
        SourceRecord record = tableA.get(0);
        VerifyRecord.isValidInsert(record, "id", 10);

        Struct value = (Struct) record.value();
        if (value.getStruct("after") != null) {
            assertThat(value.getStruct("after").getString("name")).isEqualTo("************");
        }

        assertThat(tableB).hasSize(1);
        record = tableB.get(0);
        VerifyRecord.isValidInsert(record, "id", 11);

        value = (Struct) record.value();
        if (value.getStruct("after") != null) {
            assertThat(value.getStruct("after").getString("name")).isEqualTo("some");
        }

        stopConnector();
    }

    @Test
    @FixFor("DBZ-2522")
    public void whenCaptureInstanceExcludesColumnsExpectSnapshotAndStreamingToExcludeColumns() throws Exception {
        connection.execute(
                "CREATE TABLE excluded_column_table_a (id int, name varchar(30), amount integer primary key(id))");
        connection.execute("INSERT INTO excluded_column_table_a VALUES(10, 'a name', 100)");

        TestHelper.enableTableCdc(connection, "excluded_column_table_a", "dbo_excluded_column_table_a",
                Arrays.asList("id", "name"));

        final Configuration config = TestHelper.defaultConfig()
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        connection.execute("INSERT INTO excluded_column_table_a VALUES(11, 'some_name', 120)");

        final SourceRecords records = consumeRecordsByTopic(3);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.excluded_column_table_a");

        Schema expectedSchemaA = SchemaBuilder.struct()
                .optional()
                .name("server1.testDB1.dbo.excluded_column_table_a.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Struct expectedValueSnapshot = new Struct(expectedSchemaA)
                .put("id", 10)
                .put("name", "a name");
        Struct expectedValueStreaming = new Struct(expectedSchemaA)
                .put("id", 11)
                .put("name", "some_name");

        assertThat(tableA).hasSize(2);
        SourceRecordAssert.assertThat(tableA.get(0))
                .valueAfterFieldSchemaIsEqualTo(expectedSchemaA)
                .valueAfterFieldIsEqualTo(expectedValueSnapshot);
        SourceRecordAssert.assertThat(tableA.get(1))
                .valueAfterFieldSchemaIsEqualTo(expectedSchemaA)
                .valueAfterFieldIsEqualTo(expectedValueStreaming);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-2522")
    public void whenMultipleCaptureInstancesExcludesColumnsExpectLatestCDCTableUtilized() throws Exception {
        connection.execute(
                "CREATE TABLE excluded_column_table_a (id int, name varchar(30), amount integer primary key(id))");
        connection.execute("INSERT INTO excluded_column_table_a VALUES(10, 'a name', 100)");

        TestHelper.enableTableCdc(connection, "excluded_column_table_a", "dbo_excluded_column_table_a",
                Arrays.asList("id", "name"));

        connection.execute("ALTER TABLE excluded_column_table_a ADD note varchar(30)");
        TestHelper.enableTableCdc(connection, "excluded_column_table_a", "dbo_excluded_column_table_a_2",
                Arrays.asList("id", "name", "note"));

        final Configuration config = TestHelper.defaultConfig()
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        connection.execute("INSERT INTO excluded_column_table_a VALUES(11, 'some_name', 120, 'a note')");

        final SourceRecords records = consumeRecordsByTopic(3);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.excluded_column_table_a");

        Schema expectedSchema = SchemaBuilder.struct()
                .optional()
                .name("server1.testDB1.dbo.excluded_column_table_a.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("note", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Struct expectedValueSnapshot = new Struct(expectedSchema)
                .put("id", 10)
                .put("name", "a name")
                .put("note", null);

        Struct expectedValueStreaming = new Struct(expectedSchema)
                .put("id", 11)
                .put("name", "some_name")
                .put("note", "a note");

        assertThat(tableA).hasSize(2);
        SourceRecordAssert.assertThat(tableA.get(0))
                .valueAfterFieldSchemaIsEqualTo(expectedSchema)
                .valueAfterFieldIsEqualTo(expectedValueSnapshot);
        SourceRecordAssert.assertThat(tableA.get(1))
                .valueAfterFieldSchemaIsEqualTo(expectedSchema)
                .valueAfterFieldIsEqualTo(expectedValueStreaming);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-2522")
    @Ignore // the test is very flaky in CI environment
    public void whenCaptureInstanceExcludesColumnsAndColumnsRenamedExpectNoErrors() throws Exception {
        connection.execute(
                "CREATE TABLE excluded_column_table_a (id int, name varchar(30), amount integer primary key(id))");
        connection.execute("INSERT INTO excluded_column_table_a VALUES(10, 'a name', 100)");

        TestHelper.enableTableCdc(connection, "excluded_column_table_a", "dbo_excluded_column_table_a",
                Arrays.asList("id", "name"));

        final Configuration config = TestHelper.defaultConfig()
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, ".*excluded_column_table_a")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForStreamingStarted();

        TestHelper.disableTableCdc(connection, "excluded_column_table_a");
        connection.execute("EXEC sp_RENAME 'excluded_column_table_a.name', 'first_name', 'COLUMN'");
        TestHelper.enableTableCdc(connection, "excluded_column_table_a", "dbo_excluded_column_table_a",
                Arrays.asList("id", "first_name"));

        connection.execute("INSERT INTO excluded_column_table_a VALUES(11, 'some_name', 120)");
        TestHelper.waitForCdcRecord(connection, "excluded_column_table_a", "dbo_excluded_column_table_a", rs -> rs.getInt("id") == 11);

        final SourceRecords records = consumeRecordsByTopic(2);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.excluded_column_table_a");

        Schema expectedSchema1 = SchemaBuilder.struct()
                .optional()
                .name("server1.testDB1.dbo.excluded_column_table_a.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Struct expectedValueSnapshot = new Struct(expectedSchema1)
                .put("id", 10)
                .put("name", "a name");
        Schema expectedSchema2 = SchemaBuilder.struct()
                .optional()
                .name("server1.testDB1.dbo.excluded_column_table_a.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("first_name", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Struct expectedValueStreaming = new Struct(expectedSchema2)
                .put("id", 11)
                .put("first_name", "some_name");

        assertThat(tableA).hasSize(2);
        SourceRecordAssert.assertThat(tableA.get(0))
                .valueAfterFieldSchemaIsEqualTo(expectedSchema1)
                .valueAfterFieldIsEqualTo(expectedValueSnapshot);
        SourceRecordAssert.assertThat(tableA.get(1))
                .valueAfterFieldSchemaIsEqualTo(expectedSchema2)
                .valueAfterFieldIsEqualTo(expectedValueStreaming);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-1068")
    public void excludeColumnWhenCaptureInstanceExcludesColumns() throws Exception {
        connection.execute(
                "CREATE TABLE excluded_column_table_a (id int, name varchar(30), amount integer primary key(id))");
        TestHelper.enableTableCdc(connection, "excluded_column_table_a", "dbo_excluded_column_table_a",
                Arrays.asList("id", "name"));

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        TestHelper.waitForStreamingStarted();
        connection.execute("INSERT INTO excluded_column_table_a VALUES(10, 'some_name', 120)");

        final SourceRecords records = consumeRecordsByTopic(1);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.excluded_column_table_a");

        Schema expectedSchemaA = SchemaBuilder.struct()
                .optional()
                .name("server1.testDB1.dbo.excluded_column_table_a.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Struct expectedValueA = new Struct(expectedSchemaA)
                .put("id", 10)
                .put("name", "some_name");

        assertThat(tableA).hasSize(1);
        SourceRecordAssert.assertThat(tableA.get(0))
                .valueAfterFieldSchemaIsEqualTo(expectedSchemaA)
                .valueAfterFieldIsEqualTo(expectedValueA);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-2522")
    public void excludeColumnWhenCaptureInstanceExcludesColumnInMiddleOfTable() throws Exception {
        connection.execute(
                "CREATE TABLE exclude_list_column_table_a (id int, amount integer, name varchar(30), primary key(id))");
        connection.execute("INSERT INTO exclude_list_column_table_a VALUES(10, 100, 'a name')");

        TestHelper.enableTableCdc(connection, "exclude_list_column_table_a", "dbo_exclude_list_column_table_a",
                Arrays.asList("id", "name"));

        final Configuration config = TestHelper.defaultConfig()
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, ".*exclude_list_column_table_a")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        connection.execute("INSERT INTO exclude_list_column_table_a VALUES(11, 120, 'some_name')");
        TestHelper.waitForCdcRecord(connection, "exclude_list_column_table_a", rs -> rs.getInt("id") == 11);

        final SourceRecords records = consumeRecordsByTopic(2);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.exclude_list_column_table_a");

        Schema expectedSchemaA = SchemaBuilder.struct()
                .optional()
                .name("server1.testDB1.dbo.exclude_list_column_table_a.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Struct expectedValue1 = new Struct(expectedSchemaA)
                .put("id", 10)
                .put("name", "a name");
        Struct expectedValue2 = new Struct(expectedSchemaA)
                .put("id", 11)
                .put("name", "some_name");

        assertThat(tableA).hasSize(2);
        SourceRecordAssert.assertThat(tableA.get(0))
                .valueAfterFieldSchemaIsEqualTo(expectedSchemaA)
                .valueAfterFieldIsEqualTo(expectedValue1);
        SourceRecordAssert.assertThat(tableA.get(1))
                .valueAfterFieldSchemaIsEqualTo(expectedSchemaA)
                .valueAfterFieldIsEqualTo(expectedValue2);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-2522")
    public void includeColumnsWhenCaptureInstanceExcludesColumnInMiddleOfTable() throws Exception {
        connection.execute(
                "CREATE TABLE include_list_column_table_a (id int, amount integer, name varchar(30), primary key(id))");
        TestHelper.enableTableCdc(connection, "include_list_column_table_a", "dbo_include_list_column_table_a",
                Arrays.asList("id", "name"));

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(SqlServerConnectorConfig.COLUMN_INCLUDE_LIST, "dbo.include_list_column_table_a.id,dbo.include_list_column_table_a.name")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        connection.execute("INSERT INTO include_list_column_table_a VALUES(10, 120, 'some_name')");
        TestHelper.waitForCdcRecord(connection, "include_list_column_table_a", rs -> rs.getInt("id") == 10);

        final SourceRecords records = consumeRecordsByTopic(1);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.include_list_column_table_a");

        Schema expectedSchemaA = SchemaBuilder.struct()
                .optional()
                .name("server1.testDB1.dbo.include_list_column_table_a.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Struct expectedValueA = new Struct(expectedSchemaA)
                .put("id", 10)
                .put("name", "some_name");

        assertThat(tableA).hasSize(1);
        SourceRecordAssert.assertThat(tableA.get(0))
                .valueAfterFieldSchemaIsEqualTo(expectedSchemaA)
                .valueAfterFieldIsEqualTo(expectedValueA);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-2522")
    public void excludeMultipleColumnsWhenCaptureInstanceExcludesSingleColumn() throws Exception {
        connection.execute(
                "CREATE TABLE exclude_list_column_table_a (id int, amount integer, note varchar(30), name varchar(30), primary key(id))");
        TestHelper.enableTableCdc(connection, "exclude_list_column_table_a", "dbo_exclude_list_column_table_a",
                Arrays.asList("id", "note", "name"));

        // Exclude the note column on top of the already excluded amount column
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(SqlServerConnectorConfig.COLUMN_EXCLUDE_LIST, "dbo.exclude_list_column_table_a.amount,dbo.exclude_list_column_table_a.note")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        connection.execute("INSERT INTO exclude_list_column_table_a VALUES(10, 120, 'a note', 'some_name')");
        TestHelper.waitForCdcRecord(connection, "exclude_list_column_table_a", rs -> rs.getInt("id") == 10);

        final SourceRecords records = consumeRecordsByTopic(1);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.exclude_list_column_table_a");

        Schema expectedSchemaA = SchemaBuilder.struct()
                .optional()
                .name("server1.testDB1.dbo.exclude_list_column_table_a.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Struct expectedValueA = new Struct(expectedSchemaA)
                .put("id", 10)
                .put("name", "some_name");

        assertThat(tableA).hasSize(1);
        SourceRecordAssert.assertThat(tableA.get(0))
                .valueAfterFieldSchemaIsEqualTo(expectedSchemaA)
                .valueAfterFieldIsEqualTo(expectedValueA);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-2522")
    public void includeMultipleColumnsWhenCaptureInstanceExcludesSingleColumn() throws Exception {
        connection.execute(
                "CREATE TABLE include_list_column_table_a (id int, amount integer, note varchar(30), name varchar(30), primary key(id))");
        TestHelper.enableTableCdc(connection, "include_list_column_table_a", "dbo_include_list_column_table_a",
                Arrays.asList("id", "note", "name"));

        // Exclude the note column on top of the already excluded amount column
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(SqlServerConnectorConfig.COLUMN_INCLUDE_LIST, "dbo.include_list_column_table_a.id,dbo.include_list_column_table_a.name")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        connection.execute("INSERT INTO include_list_column_table_a VALUES(10, 120, 'a note', 'some_name')");
        TestHelper.waitForCdcRecord(connection, "include_list_column_table_a", rs -> rs.getInt("id") == 10);

        final SourceRecords records = consumeRecordsByTopic(1);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.include_list_column_table_a");

        Schema expectedSchemaA = SchemaBuilder.struct()
                .optional()
                .name("server1.testDB1.dbo.include_list_column_table_a.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Struct expectedValueA = new Struct(expectedSchemaA)
                .put("id", 10)
                .put("name", "some_name");

        assertThat(tableA).hasSize(1);
        SourceRecordAssert.assertThat(tableA.get(0))
                .valueAfterFieldSchemaIsEqualTo(expectedSchemaA)
                .valueAfterFieldIsEqualTo(expectedValueA);

        stopConnector();
    }

    /**
     * Passing the "applicationName" property which can be asserted from the connected sessions".
     */
    @Test
    @FixFor("DBZ-964")
    public void shouldPropagateDatabaseDriverProperties() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
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
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.executeWithoutCommitting(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }
        connection.connection().commit();

        TestHelper.waitForCdcRecord(connection, "tablea", rs -> rs.getInt("id") == (ID_START + RECORDS_PER_TABLE - 1));
        TestHelper.waitForCdcRecord(connection, "tableb", rs -> rs.getInt("id") == (ID_START + RECORDS_PER_TABLE - 1));

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
        }

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_RESTART + i;
            connection.executeWithoutCommitting(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.executeWithoutCommitting(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
            connection.connection().commit();
        }

        TestHelper.waitForCdcRecord(connection, "tablea", rs -> rs.getInt("id") == (ID_RESTART + RECORDS_PER_TABLE - 1));
        TestHelper.waitForCdcRecord(connection, "tableb", rs -> rs.getInt("id") == (ID_RESTART + RECORDS_PER_TABLE - 1));

        sourceRecords = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        tableA = sourceRecords.recordsForTopic("server1.testDB1.dbo.tablea");
        tableB = sourceRecords.recordsForTopic("server1.testDB1.dbo.tableb");

        assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        assertThat(tableB).hasSize(RECORDS_PER_TABLE);

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
        final LogInterceptor logInterceptor = new LogInterceptor(RelationalDatabaseSchema.class);

        Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "my_products")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);

        stopConnector(value -> assertThat(logInterceptor.containsWarnMessage(DatabaseSchema.NO_CAPTURED_DATA_COLLECTIONS_WARNING)).isTrue());
    }

    @Test
    @FixFor("DBZ-1242")
    public void testNoEmptySchemaWarningAfterApplyingFilters() throws Exception {
        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor(RelationalDatabaseSchema.class);

        Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);

        stopConnector(value -> assertThat(logInterceptor.containsWarnMessage(DatabaseSchema.NO_CAPTURED_DATA_COLLECTIONS_WARNING)).isFalse());
    }

    @Test
    @FixFor("DBZ-916")
    public void keylessTable() throws Exception {
        connection.execute(
                "CREATE TABLE keyless (id int, name varchar(30))",
                "INSERT INTO keyless VALUES(1, 'k')");
        TestHelper.enableTableCdc(connection, "keyless");

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.keyless")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final List<SchemaAndValueField> key = Arrays.asList(
                new SchemaAndValueField("id", Schema.OPTIONAL_INT32_SCHEMA, 1),
                new SchemaAndValueField("name", Schema.OPTIONAL_STRING_SCHEMA, "k"));
        final List<SchemaAndValueField> key2 = Arrays.asList(
                new SchemaAndValueField("id", Schema.OPTIONAL_INT32_SCHEMA, 2),
                new SchemaAndValueField("name", Schema.OPTIONAL_STRING_SCHEMA, "k"));
        final List<SchemaAndValueField> key3 = Arrays.asList(
                new SchemaAndValueField("id", Schema.OPTIONAL_INT32_SCHEMA, 3),
                new SchemaAndValueField("name", Schema.OPTIONAL_STRING_SCHEMA, "k"));

        // Wait for snapshot completion
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.keyless").get(0).key()).isNull();
        assertThat(records.recordsForTopic("server1.testDB1.dbo.keyless").get(0).keySchema()).isNull();

        connection.execute(
                "INSERT INTO keyless VALUES(2, 'k')");
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.keyless").get(0).key()).isNull();
        assertThat(records.recordsForTopic("server1.testDB1.dbo.keyless").get(0).key()).isNull();

        connection.execute(
                "UPDATE keyless SET id=3 WHERE ID=2");
        records = consumeRecordsByTopic(3);
        final SourceRecord update1 = records.recordsForTopic("server1.testDB1.dbo.keyless").get(0);

        assertThat(update1.key()).isNull();
        assertThat(update1.keySchema()).isNull();
        assertRecord(((Struct) update1.value()).getStruct(Envelope.FieldName.BEFORE), key2);
        assertRecord(((Struct) update1.value()).getStruct(Envelope.FieldName.AFTER), key3);

        connection.execute(
                "DELETE FROM keyless WHERE id=3");
        records = consumeRecordsByTopic(2, false);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.keyless").get(0).key()).isNull();
        assertThat(records.recordsForTopic("server1.testDB1.dbo.keyless").get(0).keySchema()).isNull();
        assertNull(records.recordsForTopic("server1.testDB1.dbo.keyless").get(1).value());

        stopConnector();
    }

    @Test
    @FixFor("DBZ-1015")
    public void shouldRewriteIdentityKey() throws InterruptedException, SQLException {

        connection.execute(
                "CREATE TABLE keyless (id int, name varchar(30))",
                "INSERT INTO keyless VALUES(1, 'k')");
        TestHelper.enableTableCdc(connection, "keyless");

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.keyless")
                // rewrite key from table 'products': from {null} to {id}
                .with(SqlServerConnectorConfig.MSG_KEY_COLUMNS, "(.*).keyless:id")
                .build();

        start(SqlServerConnector.class, config);

        SourceRecords records = consumeRecordsByTopic(1);
        List<SourceRecord> recordsForTopic = records.recordsForTopic("server1.testDB1.dbo.keyless");
        assertThat(recordsForTopic.get(0).key()).isNotNull();
        Struct key = (Struct) recordsForTopic.get(0).key();
        assertThat(key.get("id")).isNotNull();

        stopConnector();
    }

    @Test
    @FixFor("DBZ-1491")
    public void shouldCaptureTableSchema() throws SQLException, InterruptedException {
        connection.execute(
                "CREATE TABLE table_schema_test (key_cola int not null,"
                        + "key_colb varchar(10) not null,"
                        + "cola int not null,"
                        + "colb datetimeoffset not null default ('2019-01-01 12:34:56.1234567+04:00'),"
                        + "colc varchar(20) default ('default_value'),"
                        + "cold float,"
                        + "primary key(key_cola, key_colb))");
        TestHelper.enableTableCdc(connection, "table_schema_test");

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        connection.execute(
                "INSERT INTO table_schema_test (key_cola, key_colb, cola, colb, colc, cold) VALUES(1, 'a', 100, '2019-01-01 10:20:39.1234567 +02:00', 'some_value', 100.20)");

        List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic("server1.testDB1.dbo.table_schema_test");
        assertThat(records).hasSize(1);
        SourceRecordAssert.assertThat(records.get(0))
                .keySchemaIsEqualTo(SchemaBuilder.struct()
                        .name("server1.testDB1.dbo.table_schema_test.Key")
                        .field("key_cola", Schema.INT32_SCHEMA)
                        .field("key_colb", Schema.STRING_SCHEMA)
                        .build())
                .valueAfterFieldSchemaIsEqualTo(SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB1.dbo.table_schema_test.Value")
                        .field("key_cola", Schema.INT32_SCHEMA)
                        .field("key_colb", Schema.STRING_SCHEMA)
                        .field("cola", Schema.INT32_SCHEMA)
                        .field("colb",
                                SchemaBuilder.string().name("io.debezium.time.ZonedTimestamp").required().defaultValue("2019-01-01T12:34:56.1234567+04:00").version(1)
                                        .build())
                        .field("colc", SchemaBuilder.string().optional().defaultValue("default_value").build())
                        .field("cold", Schema.OPTIONAL_FLOAT64_SCHEMA)
                        .build());

        stopConnector();
    }

    @Test
    @FixFor("DBZ-1923")
    public void shouldDetectPurgedHistory() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 10;
        final int ID_RESTART = 100;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.SCHEMA_HISTORY, PurgableFileSchemaHistory.class)
                .build();

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute("INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute("INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> {
            Testing.debug("Waiting for initial changes to be propagated to CDC structures");
            return connection.getMaxLsn(TestHelper.TEST_DATABASE_1).isAvailable();
        });

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
            connection.execute("INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute("INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        final LogInterceptor logInterceptor = new LogInterceptor(SqlServerConnectorIT.class);
        start(SqlServerConnector.class, config);
        waitForConnectorShutdown("sqlserver", TestHelper.TEST_SERVER_NAME);
        assertThat(logInterceptor.containsStacktraceElement(
                "The db history topic or its content is fully or partially missing. Please check database schema history topic configuration and re-execute the snapshot."))
                .isTrue();
    }

    @Test
    @FixFor("DBZ-1312")
    public void useShortTableNamesForColumnMapper() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with("column.mask.with.4.chars", "dbo.tablea.cola")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("server1.testDB1.dbo.tableb");
        assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        assertThat(tableB).hasSize(RECORDS_PER_TABLE);
        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final SourceRecord recordA = tableA.get(i);
            final SourceRecord recordB = tableB.get(i);
            final List<SchemaAndValueField> expectedRowB = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, i + ID_START),
                    new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));

            final Struct valueA = (Struct) recordA.value();
            assertThat(valueA.getStruct("after").getString("cola")).isEqualTo("****");

            final Struct valueB = (Struct) recordB.value();
            assertRecord((Struct) valueB.get("after"), expectedRowB);
            assertNull(valueB.get("before"));
        }

        stopConnector();
    }

    @Test
    @FixFor("DBZ-1312")
    public void useLongTableNamesForColumnMapper() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with("column.mask.with.4.chars", "testDB1.dbo.tablea.cola")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("server1.testDB1.dbo.tableb");
        assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        assertThat(tableB).hasSize(RECORDS_PER_TABLE);
        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final SourceRecord recordA = tableA.get(i);
            final SourceRecord recordB = tableB.get(i);
            final List<SchemaAndValueField> expectedRowB = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, i + ID_START),
                    new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));

            final Struct valueA = (Struct) recordA.value();
            assertThat(valueA.getStruct("after").getString("cola")).isEqualTo("****");

            final Struct valueB = (Struct) recordB.value();
            assertRecord((Struct) valueB.get("after"), expectedRowB);
            assertNull(valueB.get("before"));
        }

        stopConnector();
    }

    @Test
    @FixFor("DBZ-1312")
    public void useLongTableNamesForKeyMapper() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.MSG_KEY_COLUMNS, "testDB1.dbo.tablea:cola")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("server1.testDB1.dbo.tableb");
        assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        assertThat(tableB).hasSize(RECORDS_PER_TABLE);
        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final SourceRecord recordA = tableA.get(i);
            final SourceRecord recordB = tableB.get(i);
            final List<SchemaAndValueField> expectedRowB = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, i + ID_START),
                    new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));

            final Struct keyA = (Struct) recordA.key();
            assertThat(keyA.getString("cola")).isEqualTo("a");

            final Struct valueB = (Struct) recordB.value();
            assertRecord((Struct) valueB.get("after"), expectedRowB);
            assertNull(valueB.get("before"));
        }

        stopConnector();
    }

    @Test
    @FixFor("DBZ-1312")
    public void useShortTableNamesForKeyMapper() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.MSG_KEY_COLUMNS, "dbo.tablea:cola")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("server1.testDB1.dbo.tableb");
        assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        assertThat(tableB).hasSize(RECORDS_PER_TABLE);
        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final SourceRecord recordA = tableA.get(i);
            final SourceRecord recordB = tableB.get(i);
            final List<SchemaAndValueField> expectedRowB = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, i + ID_START),
                    new SchemaAndValueField("colb", Schema.OPTIONAL_STRING_SCHEMA, "b"));

            final Struct keyA = (Struct) recordA.key();
            assertThat(keyA.getString("cola")).isEqualTo("a");

            final Struct valueB = (Struct) recordB.value();
            assertRecord((Struct) valueB.get("after"), expectedRowB);
            assertNull(valueB.get("before"));
        }

        stopConnector();
    }

    @Test
    @FixFor({ "DBZ-1916", "DBZ-1830" })
    public void shouldPropagateSourceTypeByDatatype() throws Exception {
        connection.execute("CREATE TABLE dt_table (id int, c1 int, c2 int, c3a numeric(5,2), c3b varchar(128), f1 float(10), f2 decimal(8,4) primary key(id))");
        TestHelper.enableTableCdc(connection, "dt_table");

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.dt_table")
                .with("datatype.propagate.source.type", ".+\\.NUMERIC,.+\\.VARCHAR,.+\\.REAL,.+\\.DECIMAL")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();
        connection.execute("INSERT INTO dt_table (id,c1,c2,c3a,c3b,f1,f2) values (1, 123, 456, 789.01, 'test', 1.228, 234.56)");

        SourceRecords records = consumeRecordsByTopic(1);
        List<SourceRecord> recordsForTopic = records.recordsForTopic("server1.testDB1.dbo.dt_table");

        final SourceRecord record = recordsForTopic.get(0);
        final Field before = record.valueSchema().field("before");

        assertThat(before.schema().field("id").schema().parameters()).isNull();
        assertThat(before.schema().field("c1").schema().parameters()).isNull();
        assertThat(before.schema().field("c2").schema().parameters()).isNull();

        assertThat(before.schema().field("c3a").schema().parameters()).contains(
                entry(TYPE_NAME_PARAMETER_KEY, "NUMERIC"),
                entry(TYPE_LENGTH_PARAMETER_KEY, "5"),
                entry(TYPE_SCALE_PARAMETER_KEY, "2"));

        assertThat(before.schema().field("c3b").schema().parameters()).contains(
                entry(TYPE_NAME_PARAMETER_KEY, "VARCHAR"),
                entry(TYPE_LENGTH_PARAMETER_KEY, "128"));

        assertThat(before.schema().field("f2").schema().parameters()).contains(
                entry(TYPE_NAME_PARAMETER_KEY, "DECIMAL"),
                entry(TYPE_LENGTH_PARAMETER_KEY, "8"),
                entry(TYPE_SCALE_PARAMETER_KEY, "4"));

        assertThat(before.schema().field("f1").schema().parameters()).contains(
                entry(TYPE_NAME_PARAMETER_KEY, "REAL"),
                entry(TYPE_LENGTH_PARAMETER_KEY, "24"));

        stopConnector();
    }

    @Test
    @FixFor("DBZ-2379")
    public void shouldNotStreamWhenUsingSnapshotModeInitialOnly() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY)
                .build();

        final LogInterceptor logInterceptor = new LogInterceptor(SqlServerStreamingChangeEventSource.class);
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        // should be no more records
        assertNoRecordsToConsume();

        final String message = "Streaming is not enabled in current configuration";
        stopConnector(value -> assertThat(logInterceptor.containsMessage(message)).isTrue());
    }

    @Test
    @FixFor("DBZ-2582")
    public void testMaxLsnSelectStatementWithoutLimit() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 10;

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("server1.testDB1.dbo.tableb");
        assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        assertThat(tableB).hasSize(RECORDS_PER_TABLE);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-2582")
    public void testMaxLsnSelectStatementWithLimit() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 10;

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(SqlServerConnectorConfig.MAX_TRANSACTIONS_PER_ITERATION, 1)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.tablea");
        final List<SourceRecord> tableB = records.recordsForTopic("server1.testDB1.dbo.tableb");
        assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        assertThat(tableB).hasSize(RECORDS_PER_TABLE);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-2699")
    public void shouldEmitNoEventsForSkippedUpdateAndDeleteOperations() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.SKIPPED_OPERATIONS, "u,d")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        // Wait for snapshot completion
        TestHelper.waitForSnapshotToBeCompleted();
        consumeRecordsByTopic(1);

        connection.execute("INSERT INTO tablea VALUES(201, 'insert201')");
        connection.execute("UPDATE tablea SET cola='insert201-update' WHERE id=201");
        connection.execute("INSERT INTO tablea VALUES(202, 'insert202')");
        connection.execute("DELETE FROM tablea WHERE id=202");
        connection.execute("INSERT INTO tablea VALUES(203, 'insert203')");

        final SourceRecords records = consumeRecordsByTopic(3);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.tablea");
        assertThat(tableA).hasSize(3);
        tableA.forEach((SourceRecord record) -> {
            Struct value = (Struct) record.value();
            assertThat(value.get("op")).isEqualTo(Envelope.Operation.CREATE.code());
            assertThat(value.get("op")).isNotEqualTo(Envelope.Operation.UPDATE.code());
            assertThat(value.get("op")).isNotEqualTo(Envelope.Operation.DELETE.code());
        });

        assertInsert(tableA.get(0), "id", 201);
        assertInsert(tableA.get(1), "id", 202);
        assertInsert(tableA.get(2), "id", 203);

    }

    @Test
    public void shouldReturnSingleTaskConfig() {
        final Map<String, String> props = TestHelper.defaultConnectorConfig()
                .with(SqlServerConnectorConfig.DATABASE_NAMES, "mAsTeR,mOdEl")
                .build()
                .asMap();

        SqlServerConnector connector = new SqlServerConnector();
        connector.start(props);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertThat(taskConfigs).hasSize(1);
        assertThat(taskConfigs.get(0).get(SqlServerConnectorConfig.DATABASE_NAMES.name()))
                .isEqualTo("master,model");
    }

    @Test
    public void shouldHandleMaxTasksGreaterThanNumberOfDatabaseNames() {
        final Map<String, String> props = TestHelper.defaultConnectorConfig()
                .with(SqlServerConnectorConfig.DATABASE_NAMES, "mAsTeR,mOdEl")
                .build()
                .asMap();

        SqlServerConnector connector = new SqlServerConnector();
        connector.start(props);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(3);
        assertThat(taskConfigs).hasSize(2);
        assertThat(taskConfigs.get(0).get(SqlServerConnectorConfig.DATABASE_NAMES.name()))
                .isEqualTo("master");
        assertThat(taskConfigs.get(1).get(SqlServerConnectorConfig.DATABASE_NAMES.name()))
                .isEqualTo("model");
    }

    @Test
    public void shouldReturnTwoTaskConfigs() {
        final Map<String, String> props = TestHelper.defaultConnectorConfig()
                .with(SqlServerConnectorConfig.DATABASE_NAMES, "MaStEr,MoDeL")
                .build()
                .asMap();

        SqlServerConnector connector = new SqlServerConnector();
        connector.start(props);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(2);
        assertThat(taskConfigs).hasSize(2);
        assertThat(taskConfigs.get(0).get(SqlServerConnectorConfig.DATABASE_NAMES.name())).isEqualTo("master");
        assertThat(taskConfigs.get(1).get(SqlServerConnectorConfig.DATABASE_NAMES.name())).isEqualTo("model");
    }

    @Test
    @FixFor("DBZ-2975")
    public void shouldIncludeDatabaseNameIntoTopicAndSchemaNamesInMultiPartitionMode() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        TestHelper.waitForDatabaseSnapshotToBeCompleted(TestHelper.TEST_DATABASE_1);

        final SourceRecords records = consumeRecordsByTopic(1);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.tablea");
        assertThat(tableA).hasSize(1);

        final SourceRecord record = tableA.get(0);
        assertThat(record.keySchema().name()).isEqualTo("server1.testDB1.dbo.tablea.Key");
        assertThat(record.valueSchema().name()).isEqualTo("server1.testDB1.dbo.tablea.Envelope");
    }

    @Test
    @FixFor("DBZ-2793")
    public void shouldApplySchemaFilters() throws Exception {
        connection.setAutoCommit(false);
        String statements = "DROP TABLE IF EXISTS s1.tablea;" +
                "DROP TABLE IF EXISTS s1.tableb;" +
                "DROP TABLE IF EXISTS s2.tablea;" +
                "DROP TABLE IF EXISTS s2.tableb;";
        connection.execute(statements);
        connection.execute("DROP SCHEMA IF EXISTS s1");
        connection.execute("DROP SCHEMA IF EXISTS s2");
        connection.execute("CREATE SCHEMA s1");
        connection.execute("CREATE SCHEMA s2");
        statements = "CREATE TABLE s1.tablea (id int PRIMARY KEY, vala integer);" +
                "CREATE TABLE s1.tableb (id int PRIMARY KEY, valb integer);" +
                "CREATE TABLE s2.tablea (id int PRIMARY KEY, vala integer);" +
                "CREATE TABLE s2.tableb (id int PRIMARY KEY, valb integer);";
        connection.execute(statements);
        connection.setAutoCommit(true);
        TestHelper.enableSchemaTableCdc(connection, new TableId(null, "s1", "tablea"));
        TestHelper.enableSchemaTableCdc(connection, new TableId(null, "s1", "tableb"));
        TestHelper.enableSchemaTableCdc(connection, new TableId(null, "s2", "tablea"));
        TestHelper.enableSchemaTableCdc(connection, new TableId(null, "s2", "tableb"));

        // Test exclude filter, s2 schema and default dbo schema should be included.
        Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SCHEMA_EXCLUDE_LIST, "s1")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        TestHelper.waitForSnapshotToBeCompleted();
        consumeRecordsByTopic(1);

        connection.execute("INSERT INTO s1.tablea VALUES(1, 1)");
        connection.execute("INSERT INTO s1.tableb VALUES(1, 2)");
        connection.execute("INSERT INTO s2.tablea VALUES(1, 3)");
        connection.execute("INSERT INTO s2.tableb VALUES(1, 4)");
        connection.execute("INSERT INTO tablea VALUES(1001, 'a')");
        connection.execute("INSERT INTO tableb VALUES(1001, 'b')");

        SourceRecords records = consumeRecordsByTopic(4);
        List<SourceRecord> tableS1A = records.recordsForTopic("server1.testDB1.s1.tablea");
        List<SourceRecord> tableS1B = records.recordsForTopic("server1.testDB1.s1.tableb");
        List<SourceRecord> tableS2A = records.recordsForTopic("server1.testDB1.s2.tablea");
        List<SourceRecord> tableS2B = records.recordsForTopic("server1.testDB1.s2.tableb");
        List<SourceRecord> tableDboA = records.recordsForTopic("server1.testDB1.dbo.tablea");
        List<SourceRecord> tableDboB = records.recordsForTopic("server1.testDB1.dbo.tableb");

        assertNull(tableS1A);
        assertNull(tableS1B);
        assertThat(tableS2A).hasSize(1);
        assertThat(tableS2B).hasSize(1);
        assertThat(tableDboA).hasSize(1);
        assertThat(tableDboB).hasSize(1);

        stopConnector();

        // Test include filter, only s1 schema should be included.
        config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SCHEMA_INCLUDE_LIST, "s1")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        connection.execute("INSERT INTO s2.tablea VALUES(2, 3)");
        connection.execute("INSERT INTO s2.tableb VALUES(2, 4)");
        connection.execute("INSERT INTO tablea VALUES(1002, 'a')");
        connection.execute("INSERT INTO tableb VALUES(1002, 'a')");
        connection.execute("INSERT INTO s1.tablea VALUES(2, 1)");
        connection.execute("INSERT INTO s1.tableb VALUES(2, 2)");

        records = consumeRecordsByTopic(2);
        tableS2A = records.recordsForTopic("server1.testDB1.s2.tablea");
        tableS2B = records.recordsForTopic("server1.testDB1.s2.tableb");
        tableDboA = records.recordsForTopic("server1.testDB1.dbo.tablea");
        tableDboB = records.recordsForTopic("server1.testDB1.dbo.tableb");
        tableS1A = records.recordsForTopic("server1.testDB1.s1.tablea");
        tableS1B = records.recordsForTopic("server1.testDB1.s1.tableb");

        assertThat(tableS1A).hasSize(1);
        assertThat(tableS1B).hasSize(1);
        assertNull(tableS2A);
        assertNull(tableS2B);
        assertNull(tableDboA);
        assertNull(tableDboB);

        stopConnector();
    }

    @Test
    public void shouldFailWhenUserDoesNotHaveAccessToDatabase() {
        TestHelper.createTestDatabases(TestHelper.TEST_DATABASE_2);
        final Configuration config2 = TestHelper.defaultConfig(
                TestHelper.TEST_DATABASE_1, TestHelper.TEST_DATABASE_2)
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();
        Map<String, Object> result = new HashMap<>();
        start(SqlServerConnector.class, config2, (success, message, error) -> {
            result.put("success", success);
            result.put("message", message);
        });
        assertEquals(false, result.get("success"));
        assertEquals(
                "Connector configuration is not valid. User sa does not have access to CDC schema in the following databases: testDB2. This user can only be used in initial_only snapshot mode",
                result.get("message"));
    }

    @Test
    @FixFor("DBZ-5033")
    public void shouldIgnoreNullOffsetsWhenRecoveringHistory() {
        final Configuration config1 = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY)
                .build();
        start(SqlServerConnector.class, config1);
        assertConnectorIsRunning();
        TestHelper.waitForDatabaseSnapshotToBeCompleted(TestHelper.TEST_DATABASE_1);
        stopConnector();

        TestHelper.createTestDatabases(TestHelper.TEST_DATABASE_2);
        final Configuration config2 = TestHelper.defaultConfig(
                TestHelper.TEST_DATABASE_1, TestHelper.TEST_DATABASE_2)
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY)
                .build();
        start(SqlServerConnector.class, config2);
        assertConnectorIsRunning();
        TestHelper.waitForDatabaseSnapshotToBeCompleted(TestHelper.TEST_DATABASE_2);
        stopConnector();
    }

    @Test
    @FixFor("DBZ-5423")
    public void shouldStreamToOldTableAfterRename() throws Exception {
        connection.execute(
                "CREATE TABLE account (id int, name varchar(30), amount integer primary key(id))");
        TestHelper.enableTableCdc(connection, "account");

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        final Schema expectedSchema = SchemaBuilder.struct()
                .optional()
                .name("server1.testDB1.dbo.account.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("amount", Schema.OPTIONAL_INT32_SCHEMA)
                .build();

        connection.setAutoCommit(false);

        // Insert a record prior to rename
        connection.execute("INSERT INTO account VALUES(10, 'some_name', 120)");

        // Assert emitted record state
        SourceRecords records = consumeRecordsByTopic(1);
        List<SourceRecord> recordsForTopic = records.recordsForTopic("server1.testDB1.dbo.account");
        assertThat(recordsForTopic).hasSize(1);

        SourceRecordAssert.assertThat(recordsForTopic.get(0))
                .valueAfterFieldIsEqualTo(
                        new Struct(expectedSchema)
                                .put("id", 10)
                                .put("name", "some_name")
                                .put("amount", 120))
                .valueAfterFieldSchemaIsEqualTo(expectedSchema);

        // Rename table and test insertion post-insert
        // This is to verify that the emitted events still are emitted as "account" despite the table rename
        connection.execute("EXEC sp_rename 'account', 'account_new'");
        connection.execute("INSERT INTO account_new VALUES (11, 'some_value', 240)");

        records = consumeRecordsByTopic(1);
        recordsForTopic = records.recordsForTopic("server1.testDB1.dbo.account");
        List<SourceRecord> recordsForNewTableTopic = records.recordsForTopic("server1.testDB1.dbo.account_new");

        // Assert state
        assertThat(recordsForTopic).hasSize(1);
        assertThat(recordsForNewTableTopic).isNull();
        assertNoRecordsToConsume();

        SourceRecordAssert.assertThat(recordsForTopic.get(0))
                .valueAfterFieldIsEqualTo(
                        new Struct(expectedSchema)
                                .put("id", 11)
                                .put("name", "some_value")
                                .put("amount", 240))
                .valueAfterFieldSchemaIsEqualTo(expectedSchema);
    }

    @Test
    @FixFor("DBZ-5423")
    public void shouldStreamToNewTableAfterRestart() throws Exception {
        connection.execute(
                "CREATE TABLE account (id int, name varchar(30), amount integer primary key(id))");
        TestHelper.enableTableCdc(connection, "account");

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        final Schema expectedSchema = SchemaBuilder.struct()
                .optional()
                .name("server1.testDB1.dbo.account.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("amount", Schema.OPTIONAL_INT32_SCHEMA)
                .build();

        connection.setAutoCommit(false);

        // Insert a record prior to rename
        connection.execute("INSERT INTO account VALUES(10, 'some_name', 120)");

        // Assert emitted record state
        SourceRecords records = consumeRecordsByTopic(1);
        List<SourceRecord> recordsForTopic = records.recordsForTopic("server1.testDB1.dbo.account");
        assertThat(recordsForTopic).hasSize(1);

        SourceRecordAssert.assertThat(recordsForTopic.get(0))
                .valueAfterFieldIsEqualTo(
                        new Struct(expectedSchema)
                                .put("id", 10)
                                .put("name", "some_name")
                                .put("amount", 120))
                .valueAfterFieldSchemaIsEqualTo(expectedSchema);

        // Rename table and test insertion post-insert
        // This is to verify that the emitted events still are emitted as "account" despite the table rename
        connection.execute("EXEC sp_rename 'account', 'account_new'");
        connection.execute("INSERT INTO account_new VALUES (11, 'some_value', 240)");

        records = consumeRecordsByTopic(1);
        recordsForTopic = records.recordsForTopic("server1.testDB1.dbo.account");
        List<SourceRecord> recordsForNewTableTopic = records.recordsForTopic("server1.testDB1.dbo.account_new");

        // Assert state
        assertThat(recordsForTopic).hasSize(1);
        assertThat(recordsForNewTableTopic).isNull();
        assertNoRecordsToConsume();

        SourceRecordAssert.assertThat(recordsForTopic.get(0))
                .valueAfterFieldIsEqualTo(
                        new Struct(expectedSchema)
                                .put("id", 11)
                                .put("name", "some_value")
                                .put("amount", 240))
                .valueAfterFieldSchemaIsEqualTo(expectedSchema);

        stopConnector();

        final Schema newExpectedSchema = SchemaBuilder.struct()
                .optional()
                .name("server1.testDB1.dbo.account_new.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("amount", Schema.OPTIONAL_INT32_SCHEMA)
                .build();

        // Restart the connector
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingStarted();
        assertNoRecordsToConsume();

        connection.execute("INSERT INTO account_new VALUES (12, 'some_value2', 241)");

        records = consumeRecordsByTopic(1);
        recordsForTopic = records.recordsForTopic("server1.testDB1.dbo.account");
        recordsForNewTableTopic = records.recordsForTopic("server1.testDB1.dbo.account_new");

        final Schema expectedSchemaAfter = SchemaBuilder.struct()
                .optional()
                .name("server1.testDB1.dbo.account_new.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("amount", Schema.OPTIONAL_INT32_SCHEMA)
                .build();

        // Assert state
        assertThat(recordsForTopic).isNull();
        assertThat(recordsForNewTableTopic).hasSize(1);
        assertNoRecordsToConsume();

        SourceRecordAssert.assertThat(recordsForNewTableTopic.get(0))
                .valueAfterFieldIsEqualTo(
                        new Struct(newExpectedSchema)
                                .put("id", 12)
                                .put("name", "some_value2")
                                .put("amount", 241))
                .valueAfterFieldSchemaIsEqualTo(newExpectedSchema);

        stopConnector();
    }

    @Test
    @Ignore
    public void shouldStopRetriableRestartsAtConfiguredMaximumDuringSnapshot() throws Exception {
        shouldStopRetriableRestartsAtConfiguredMaximum(() -> {
            connection.execute("ALTER DATABASE " + TestHelper.TEST_DATABASE_2 + " SET OFFLINE");
            TestHelper.waitForDatabaseSnapshotToBeCompleted(TestHelper.TEST_DATABASE_1);
        });
    }

    @Test
    @Ignore
    public void shouldStopRetriableRestartsAtConfiguredMaximumDuringStreaming() throws Exception {
        shouldStopRetriableRestartsAtConfiguredMaximum(() -> {
            TestHelper.waitForStreamingStarted();
            connection.execute("ALTER DATABASE " + TestHelper.TEST_DATABASE_2
                    + " SET OFFLINE WITH ROLLBACK IMMEDIATE");
        });
    }

    private void shouldStopRetriableRestartsAtConfiguredMaximum(SqlRunnable scenario) throws Exception {
        TestHelper.createTestDatabase(TestHelper.TEST_DATABASE_2);
        connection = TestHelper.testConnection(TEST_DATABASE_2);
        connection.execute(
                "CREATE TABLE tablea (id int primary key, cola varchar(30))",
                "CREATE TABLE tableb (id int primary key, colb varchar(30))",
                "INSERT INTO tablea VALUES(1, 'a')");
        TestHelper.enableTableCdc(connection, "tablea");
        TestHelper.enableTableCdc(connection, "tableb");
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        final Configuration config1 = TestHelper.defaultConnectorConfig()
                .with(SqlServerConnectorConfig.DATABASE_NAMES.name(), TestHelper.TEST_DATABASE_1 + "," + TestHelper.TEST_DATABASE_2)
                .with("errors.max.retries", 1)
                .build();
        final LogInterceptor logInterceptor = new LogInterceptor(ErrorHandler.class);

        try {
            start(SqlServerConnector.class, config1);
            assertConnectorIsRunning();
            scenario.run();

            final String message1 = "1 of 1 retries will be attempted";
            final String message2 = "The maximum number of 1 retries has been attempted";
            Awaitility.await()
                    .alias("Checking for maximum restart messages")
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .atMost(5, TimeUnit.SECONDS)
                    .ignoreException(InstanceNotFoundException.class)
                    .until(() -> logInterceptor.containsMessage(message1) && logInterceptor.containsMessage(message2));
        }
        finally {
            // Set the database back online, since otherwise, it will be impossible to create it again
            // https://docs.microsoft.com/en-us/sql/t-sql/statements/drop-database-transact-sql?view=sql-server-ver15#general-remarks
            connection.execute("ALTER DATABASE " + TestHelper.TEST_DATABASE_2 + " SET ONLINE");
        }
    }

    private void assertRecord(Struct record, List<SchemaAndValueField> expected) {
        expected.forEach(schemaAndValueField -> schemaAndValueField.assertFor(record));
    }

    public static class PurgableFileSchemaHistory implements SchemaHistory {

        final SchemaHistory delegate = new FileSchemaHistory();

        @Override
        public boolean exists() {
            try {
                return storageExists() && java.nio.file.Files.size(TestHelper.SCHEMA_HISTORY_PATH) > 0;
            }
            catch (IOException e) {
                throw new SchemaHistoryException("File should exist");
            }
        }

        @Override
        public void configure(Configuration config, HistoryRecordComparator comparator,
                              SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
            delegate.configure(config, comparator, listener, useCatalogBeforeSchema);
        }

        @Override
        public void start() {
            delegate.start();
        }

        @Override
        public void record(Map<String, ?> source, Map<String, ?> position, String databaseName, String ddl)
                throws SchemaHistoryException {
            delegate.record(source, position, databaseName, ddl);
        }

        @Override
        public void record(Map<String, ?> source, Map<String, ?> position, String databaseName, String schemaName,
                           String ddl, TableChanges changes, Instant timestamp)
                throws SchemaHistoryException {
            delegate.record(source, position, databaseName, schemaName, ddl, changes, timestamp);
        }

        @Override
        public void recover(Offsets<?, ?> offsets, Tables schema, DdlParser ddlParser) {
            delegate.recover(offsets, schema, ddlParser);
        }

        @Override
        public void recover(Map<Map<String, ?>, Map<String, ?>> offsets, Tables schema, DdlParser ddlParser) {
            delegate.recover(offsets, schema, ddlParser);
        }

        @Override
        public void stop() {
            delegate.stop();
        }

        @Override
        public boolean storageExists() {
            return delegate.storageExists();
        }

        @Override
        public void initializeStorage() {
            delegate.initializeStorage();
        }
    }

    @FunctionalInterface
    interface SqlRunnable {
        void run() throws SQLException;
    }
}
