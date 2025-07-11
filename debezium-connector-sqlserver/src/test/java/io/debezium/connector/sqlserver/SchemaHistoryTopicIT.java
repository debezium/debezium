/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.SnapshotType;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
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
public class SchemaHistoryTopicIT extends AbstractAsyncEngineConnectorTest {

    private SqlServerConnection connection;

    @Before
    public void before() throws SQLException, InterruptedException {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        connection.execute(
                "CREATE TABLE tablea (id int primary key, cola varchar(30))",
                "CREATE TABLE tableb (id int primary key, colb varchar(30))",
                "CREATE TABLE tablec (id int primary key, colc varchar(30))");
        TestHelper.enableTableCdc(connection, "tablea");
        TestHelper.enableTableCdc(connection, "tableb");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        // In some cases the max lsn from lsn_time_mapping table was coming out to be null, since
        // the operations done above needed some time to be captured by the capture process.
        Thread.sleep(1000);
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
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
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
        assertThat(schemaRecords).hasSize(3);
        schemaRecords.forEach(record -> {
            assertThat(record.topic()).isEqualTo("server1");
            assertThat(((Struct) record.key()).getString("databaseName")).isEqualTo("testDB1");
            assertThat(record.sourceOffset().get("snapshot")).isEqualTo(SnapshotType.INITIAL.toString());
        });
        assertThat(((Struct) schemaRecords.get(0).value()).getStruct("source").getString("snapshot")).isEqualTo("true");
        assertThat(((Struct) schemaRecords.get(1).value()).getStruct("source").getString("snapshot")).isEqualTo("true");
        assertThat(((Struct) schemaRecords.get(2).value()).getStruct("source").getString("snapshot")).isEqualTo("last");

        List<Struct> tableChanges = ((Struct) schemaRecords.get(0).value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertThat(tableChanges.get(0).get("type")).isEqualTo("CREATE");

        waitForAvailableRecords(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES, 24);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        final List<SourceRecord> tablebRecords = records.recordsForTopic("server1.testDB1.dbo.tableb");
        // Additional schema change record was emitted
        if (tablebRecords.size() == RECORDS_PER_TABLE - 1) {
            tablebRecords.add(consumeRecord());
        }
        assertThat(tablebRecords).hasSize(RECORDS_PER_TABLE);
        tablebRecords.forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct) ((Struct) record.value()).get("after"),
                    SchemaBuilder.struct()
                            .optional()
                            .name("server1.testDB1.dbo.tableb.Value")
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
        assertThat(records.allRecordsInOrder()).hasSize(1);
        final SourceRecord schemaRecord = records.allRecordsInOrder().get(0);
        assertThat(schemaRecord.topic()).isEqualTo("server1");
        assertThat(((Struct) schemaRecord.key()).getString("databaseName")).isEqualTo("testDB1");
        assertThat(schemaRecord.sourceOffset().get("snapshot")).isNull();

        assertThat(((Struct) schemaRecord.value()).getStruct("source").getString("snapshot")).isNull();

        tableChanges = ((Struct) schemaRecord.value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertThat(tableChanges.get(0).get("type")).isEqualTo("ALTER");
        assertThat(lastUpdate.sourceOffset()).isEqualTo(schemaRecord.sourceOffset());

        records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tableb")).hasSize(RECORDS_PER_TABLE);

        records.recordsForTopic("server1.testDB1.dbo.tableb").forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct) ((Struct) record.value()).get("after"),
                    SchemaBuilder.struct()
                            .optional()
                            .name("server1.testDB1.dbo.tableb.Value")
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
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tableb")).hasSize(RECORDS_PER_TABLE);
        records.recordsForTopic("server1.testDB1.dbo.tableb").forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct) ((Struct) record.value()).get("after"),
                    SchemaBuilder.struct()
                            .optional()
                            .name("server1.testDB1.dbo.tableb.Value")
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
        assertThat(schemaRecords).hasSize(3);
        schemaRecords.forEach(record -> {
            assertThat(record.topic()).isEqualTo("server1");
            assertThat(((Struct) record.key()).getString("databaseName")).isEqualTo("testDB1");
            assertThat(record.sourceOffset().get("snapshot")).isEqualTo(SnapshotType.INITIAL.toString());
        });
        assertThat(((Struct) schemaRecords.get(0).value()).getStruct("source").getString("snapshot")).isEqualTo("true");
        assertThat(((Struct) schemaRecords.get(1).value()).getStruct("source").getString("snapshot")).isEqualTo("true");
        assertThat(((Struct) schemaRecords.get(2).value()).getStruct("source").getString("snapshot")).isEqualTo("true");

        final List<Struct> tableChanges = ((Struct) schemaRecords.get(0).value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertThat(tableChanges.get(0).get("type")).isEqualTo("CREATE");

        records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tableb")).hasSize(RECORDS_PER_TABLE);
        records.recordsForTopic("server1.testDB1.dbo.tableb").forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct) ((Struct) record.value()).get("after"),
                    SchemaBuilder.struct()
                            .optional()
                            .name("server1.testDB1.dbo.tableb.Value")
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
                .with(SqlServerConnectorConfig.STORE_ONLY_CAPTURED_TABLES_DDL, "true")
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
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tablec")).hasSize(1);

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
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tabled")).hasSize(1);

        final List<SourceRecord> schemaEvents = records.recordsForTopic("server1");

        // TODO DBZ-4082: schemaEvents is null occasionally when running this test on CI;
        // still we got the right number of records, so I'm logging all received records here
        if (schemaEvents == null) {
            Testing.print("Received records: " + records.allRecordsInOrder());
        }

        final SourceRecord schemaEventD = schemaEvents.get(schemaEvents.size() - 1);
        assertThat(((Struct) schemaEventD.value()).getStruct("source").getString("schema")).isEqualTo("dbo");
        assertThat(((Struct) schemaEventD.value()).getStruct("source").getString("table")).isEqualTo("tabled");
    }

    @Test
    @FixFor({ "DBZ-3347", "DBZ-2975" })
    public void shouldContainPartitionInSchemaChangeEvent() throws Exception {
        connection.execute("create table dbz3347 (id int primary key, data varchar(50))");
        TestHelper.enableTableCdc(connection, "dbz3347");

        Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo\\.dbz3347")
                .with(SqlServerConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        TestHelper.waitForStreamingStarted();

        SourceRecords schemaChanges = consumeRecordsByTopic(1);
        SourceRecord change = schemaChanges.recordsForTopic("server1").get(0);
        assertThat(change.sourcePartition()).isEqualTo(Collect.hashMapOf("server", "server1", "database", "testDB1"));
    }
}
