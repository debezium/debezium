/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Collect;
import io.debezium.util.Testing;

/**
 * Integration test for the user-facing history topic of the Debezium SQL Server connector.
 * <p>
 * The tests should verify the {@code CREATE} schema events from snapshot and the {@code CREATE} and
 * the {@code ALTER} schema events from streaming
 *
 * @author Jiri Pechanec
 */
public class SchemaHistoryTopicIT extends AbstractConnectorTest {

    private SqlServerConnection connection;

    @Before
    public void before() throws SQLException {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        connection.execute(
                "CREATE TABLE tablea (id int primary key, cola varchar(30))",
                "CREATE TABLE tableb (id int primary key, colb varchar(30))",
                "CREATE TABLE tablec (id int primary key, colc varchar(30))");
        TestHelper.enableTableCdc(connection, "tablea");
        TestHelper.enableTableCdc(connection, "tableb");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-1904")
    public void streamingSchemaChanges() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START_1 = 10;
        final int ID_START_2 = 100;
        final int ID_START_3 = 1000;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(RelationalDatabaseConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_1 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        Testing.Print.enable();

        // DDL for 3 tables
        SourceRecords records = consumeRecordsByTopic(3);
        final List<SourceRecord> schemaRecords = records.allRecordsInOrder();
        Assertions.assertThat(schemaRecords).hasSize(3);
        schemaRecords.forEach(record -> {
            Assertions.assertThat(record.topic()).isEqualTo("server1");
            Assertions.assertThat(((Struct) record.key()).getString("databaseName")).isEqualTo("testDB");
            Assertions.assertThat(record.sourceOffset().get("snapshot")).isEqualTo(true);
        });
        Assertions.assertThat(((Struct) schemaRecords.get(0).value()).getStruct("source").getString("snapshot")).isEqualTo("true");
        Assertions.assertThat(((Struct) schemaRecords.get(1).value()).getStruct("source").getString("snapshot")).isEqualTo("true");
        Assertions.assertThat(((Struct) schemaRecords.get(2).value()).getStruct("source").getString("snapshot")).isEqualTo("last");

        List<Struct> tableChanges = ((Struct) schemaRecords.get(0).value()).getArray("tableChanges");
        Assertions.assertThat(tableChanges).hasSize(1);
        Assertions.assertThat(tableChanges.get(0).get("type")).isEqualTo("CREATE");

        waitForAvailableRecords(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES, 24);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        final List<SourceRecord> tablebRecords = records.recordsForTopic("server1.dbo.tableb");
        // Additional schema change record was emitted
        if (tablebRecords.size() == RECORDS_PER_TABLE - 1) {
            tablebRecords.add(consumeRecord());
        }
        Assertions.assertThat(tablebRecords).hasSize(RECORDS_PER_TABLE);
        tablebRecords.forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct) ((Struct) record.value()).get("after"),
                    SchemaBuilder.struct()
                            .optional()
                            .name("server1.dbo.tableb.Value")
                            .field("id", Schema.INT32_SCHEMA)
                            .field("colb", Schema.OPTIONAL_STRING_SCHEMA)
                            .build());
        });

        final List<SourceRecord> updateBatch = records.allRecordsInOrder();
        final SourceRecord lastUpdate = updateBatch.get(updateBatch.size() - 1);

        // CDC must be disabled, otherwise rename fails
        TestHelper.disableTableCdc(connection, "tableb");
        // Enable a second capture instance
        connection.execute("exec sp_rename 'tableb.colb', 'newcolb';");
        TestHelper.enableTableCdc(connection, "tableb", "after_change");

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_2 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a2')");
            connection.execute(
                    "INSERT INTO tableb(id,newcolb) VALUES(" + id + ", 'b2')");
        }

        // DDL for 1 table
        records = consumeRecordsByTopic(1);
        Assertions.assertThat(records.allRecordsInOrder()).hasSize(1);
        final SourceRecord schemaRecord = records.allRecordsInOrder().get(0);
        Assertions.assertThat(schemaRecord.topic()).isEqualTo("server1");
        Assertions.assertThat(((Struct) schemaRecord.key()).getString("databaseName")).isEqualTo("testDB");
        Assertions.assertThat(schemaRecord.sourceOffset().get("snapshot")).isNull();

        Assertions.assertThat(((Struct) schemaRecord.value()).getStruct("source").getString("snapshot")).isNull();

        tableChanges = ((Struct) schemaRecord.value()).getArray("tableChanges");
        Assertions.assertThat(tableChanges).hasSize(1);
        Assertions.assertThat(tableChanges.get(0).get("type")).isEqualTo("ALTER");
        Assertions.assertThat(lastUpdate.sourceOffset()).isEqualTo(schemaRecord.sourceOffset());

        records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tableb")).hasSize(RECORDS_PER_TABLE);

        records.recordsForTopic("server1.dbo.tableb").forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct) ((Struct) record.value()).get("after"),
                    SchemaBuilder.struct()
                            .optional()
                            .name("server1.dbo.tableb.Value")
                            .field("id", Schema.INT32_SCHEMA)
                            .field("newcolb", Schema.OPTIONAL_STRING_SCHEMA)
                            .build());
        });

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_3 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a3')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b3')");
        }
        records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tableb")).hasSize(RECORDS_PER_TABLE);
        records.recordsForTopic("server1.dbo.tableb").forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct) ((Struct) record.value()).get("after"),
                    SchemaBuilder.struct()
                            .optional()
                            .name("server1.dbo.tableb.Value")
                            .field("id", Schema.INT32_SCHEMA)
                            .field("newcolb", Schema.OPTIONAL_STRING_SCHEMA)
                            .build());
        });
    }

    @Test
    @FixFor("DBZ-1904")
    public void snapshotSchemaChanges() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START_1 = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(RelationalDatabaseConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_1 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        // Testing.Print.enable();

        // DDL for 3 tables
        SourceRecords records = consumeRecordsByTopic(3);
        final List<SourceRecord> schemaRecords = records.allRecordsInOrder();
        Assertions.assertThat(schemaRecords).hasSize(3);
        schemaRecords.forEach(record -> {
            Assertions.assertThat(record.topic()).isEqualTo("server1");
            Assertions.assertThat(((Struct) record.key()).getString("databaseName")).isEqualTo("testDB");
            Assertions.assertThat(record.sourceOffset().get("snapshot")).isEqualTo(true);
        });
        Assertions.assertThat(((Struct) schemaRecords.get(0).value()).getStruct("source").getString("snapshot")).isEqualTo("true");
        Assertions.assertThat(((Struct) schemaRecords.get(1).value()).getStruct("source").getString("snapshot")).isEqualTo("true");
        Assertions.assertThat(((Struct) schemaRecords.get(2).value()).getStruct("source").getString("snapshot")).isEqualTo("true");

        final List<Struct> tableChanges = ((Struct) schemaRecords.get(0).value()).getArray("tableChanges");
        Assertions.assertThat(tableChanges).hasSize(1);
        Assertions.assertThat(tableChanges.get(0).get("type")).isEqualTo("CREATE");

        records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tableb")).hasSize(RECORDS_PER_TABLE);
        records.recordsForTopic("server1.dbo.tableb").forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct) ((Struct) record.value()).get("after"),
                    SchemaBuilder.struct()
                            .optional()
                            .name("server1.dbo.tableb.Value")
                            .field("id", Schema.INT32_SCHEMA)
                            .field("colb", Schema.OPTIONAL_STRING_SCHEMA)
                            .build());
        });
    }

    @Test
    @FixFor("DBZ-2303")
    public void schemaChangeAfterSnapshot() throws Exception {
        final int RECORDS_PER_TABLE = 1;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.tablec")
                .build();

        connection.execute("CREATE TABLE tabled (id int primary key, cold varchar(30))");

        connection.execute("INSERT INTO tablec VALUES(1, 'c')");
        // Enable CDC for already existing table
        TestHelper.enableTableCdc(connection, "tablec");
        // Make sure table's capture instance exists first; avoids unexpected ALTER
        TestHelper.waitForEnabledCdc(connection, "tablec");

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        // 1 schema event + 1 data event
        Testing.Print.enable();
        SourceRecords records = consumeRecordsByTopic(1 + 1);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tablec")).hasSize(1);

        stopConnector();
        assertConnectorNotRunning();

        final Configuration config2 = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.tablec,dbo.tabled")
                .build();
        start(SqlServerConnector.class, config2);
        assertConnectorIsRunning();

        // Guarantee we've started streaming and not still in bootstrap steps
        TestHelper.waitForStreamingStarted();

        // CDC for newly added table
        TestHelper.enableTableCdc(connection, "tabled");
        // Make sure table's capture instance exists first
        TestHelper.waitForEnabledCdc(connection, "tabled");

        connection.execute("INSERT INTO tabled VALUES(1, 'd')");

        // 1-2 schema events + 1 data event
        records = consumeRecordsByTopic(2 + 1);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tabled")).hasSize(1);

        final List<SourceRecord> schemaEvents = records.recordsForTopic("server1");

        // TODO DBZ-4082: schemaEvents is null occasionally when running this test on CI;
        // still we got the right number of records, so I'm logging all received records here
        if (schemaEvents == null) {
            Testing.print("Received records: " + records.allRecordsInOrder());
        }

        final SourceRecord schemaEventD = schemaEvents.get(schemaEvents.size() - 1);
        Assertions.assertThat(((Struct) schemaEventD.value()).getStruct("source").getString("schema")).isEqualTo("dbo");
        Assertions.assertThat(((Struct) schemaEventD.value()).getStruct("source").getString("table")).isEqualTo("tabled");
    }

    @Test
    @FixFor("DBZ-3347")
    public void shouldContainPartitionInSchemaChangeEventInSinglePartitionMode() throws Exception {
        shouldContainPartitionInSchemaChangeEvent(TestHelper.defaultConfig(),
                Collections.singletonMap("server", "server1"));
    }

    @Test
    @FixFor({ "DBZ-3347", "DBZ-2975" })
    public void shouldContainPartitionInSchemaChangeEventInMultiPartitionMode() throws Exception {
        shouldContainPartitionInSchemaChangeEvent(TestHelper.defaultMultiPartitionConfig(),
                Collect.hashMapOf("server", "server1", "database", "testDB"));
    }

    private void shouldContainPartitionInSchemaChangeEvent(Configuration.Builder configBuilder,
                                                           Map<String, String> expectedPartition)
            throws Exception {
        connection.execute("create table dbz3347 (id int primary key, data varchar(50))");
        TestHelper.enableTableCdc(connection, "dbz3347");

        Configuration config = configBuilder
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo\\.dbz3347")
                .with(SqlServerConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        TestHelper.waitForStreamingStarted();

        SourceRecords schemaChanges = consumeRecordsByTopic(1);
        SourceRecord change = schemaChanges.recordsForTopic("server1").get(0);
        assertThat(change.sourcePartition()).isEqualTo(expectedPartition);
    }
}
