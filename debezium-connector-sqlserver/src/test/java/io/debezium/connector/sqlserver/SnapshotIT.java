/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static io.debezium.connector.sqlserver.SqlServerConnectorConfig.SNAPSHOT_ISOLATION_MODE;
import static io.debezium.relational.RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST;
import static junit.framework.TestCase.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNull;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotIsolationMode;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.converters.CloudEventsConverterTest;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.data.SchemaAndValueField;
import io.debezium.data.SourceRecordAssert;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.time.Timestamp;
import io.debezium.util.Testing;

/**
 * Integration test for the Debezium SQL Server connector.
 *
 * @author Jiri Pechanec
 */
public class SnapshotIT extends AbstractConnectorTest {

    private static final int INITIAL_RECORDS_PER_TABLE = 500;
    private static final int STREAMING_RECORDS_PER_TABLE = 500;

    private SqlServerConnection connection;

    @Before
    public void before() throws SQLException {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        connection.execute(
                "CREATE TABLE table1 (id int, name varchar(30), price decimal(8,2), ts datetime2(0), primary key(id))");

        // Populate database
        for (int i = 0; i < INITIAL_RECORDS_PER_TABLE; i++) {
            connection.execute(
                    String.format("INSERT INTO table1 VALUES(%s, '%s', %s, '%s')", i, "name" + i, new BigDecimal(i + ".23"), "2018-07-18 13:28:56"));
        }

        TestHelper.enableTableCdc(connection, "table1");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
        // TestHelper.dropTestDatabase();
    }

    @Test
    public void takeSnapshotInExclusiveMode() throws Exception {
        takeSnapshot(SnapshotIsolationMode.EXCLUSIVE);
    }

    @Test
    public void takeSnapshotInSnapshotMode() throws Exception {
        Testing.Print.enable();
        takeSnapshot(SnapshotIsolationMode.SNAPSHOT);
    }

    @Test
    public void takeSnapshotInRepeatableReadMode() throws Exception {
        takeSnapshot(SnapshotIsolationMode.REPEATABLE_READ);
    }

    @Test
    public void takeSnapshotInReadCommittedMode() throws Exception {
        takeSnapshot(SnapshotIsolationMode.READ_COMMITTED);
    }

    @Test
    public void takeSnapshotInReadUncommittedMode() throws Exception {
        takeSnapshot(SnapshotIsolationMode.READ_UNCOMMITTED);
    }

    private void takeSnapshot(SnapshotIsolationMode lockingMode) throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(SNAPSHOT_ISOLATION_MODE.name(), lockingMode.getValue())
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final SourceRecords records = consumeRecordsByTopic(INITIAL_RECORDS_PER_TABLE);
        final List<SourceRecord> table1 = records.recordsForTopic("server1.testDB1.dbo.table1");

        assertThat(table1).hasSize(INITIAL_RECORDS_PER_TABLE);

        for (int i = 0; i < INITIAL_RECORDS_PER_TABLE; i++) {
            final SourceRecord record1 = table1.get(i);
            final List<SchemaAndValueField> expectedKey1 = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, i));
            final List<SchemaAndValueField> expectedRow1 = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, i),
                    new SchemaAndValueField("name", Schema.OPTIONAL_STRING_SCHEMA, "name" + i),
                    new SchemaAndValueField("price", Decimal.builder(2).parameter("connect.decimal.precision", "8").optional().build(), new BigDecimal(i + ".23")),
                    new SchemaAndValueField("ts", Timestamp.builder().optional().schema(), 1_531_920_536_000l));

            final Struct key1 = (Struct) record1.key();
            final Struct value1 = (Struct) record1.value();
            assertRecord(key1, expectedKey1);
            assertRecord((Struct) value1.get("after"), expectedRow1);
            assertThat(record1.sourceOffset())
                    .extracting("snapshot").containsExactly(true);
            assertThat(record1.sourceOffset())
                    .extracting("snapshot_completed").containsExactly(i == INITIAL_RECORDS_PER_TABLE - 1);
            assertNull(value1.get("before"));
        }
    }

    @Test
    public void takeSnapshotAndStartStreaming() throws Exception {
        final Configuration config = TestHelper.defaultConfig().build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Ignore initial records
        final SourceRecords records = consumeRecordsByTopic(INITIAL_RECORDS_PER_TABLE);
        final List<SourceRecord> table1 = records.recordsForTopic("server1.testDB1.dbo.table1");
        assertThat(((Struct) table1.get(0).value()).getStruct("source").getString("snapshot")).isEqualTo("first");
        table1.subList(1, INITIAL_RECORDS_PER_TABLE - 1).forEach(record -> {
            assertThat(((Struct) record.value()).getStruct("source").getString("snapshot")).isEqualTo("true");
        });
        assertThat(((Struct) table1.get(INITIAL_RECORDS_PER_TABLE - 1).value()).getStruct("source").getString("snapshot")).isEqualTo("last");
        testStreaming();
    }

    @Test
    @FixFor("DBZ-1280")
    public void testDeadlockDetection() throws Exception {
        final LogInterceptor logInterceptorErrorHandler = new LogInterceptor(ErrorHandler.class);
        final LogInterceptor logInterceptorTask = new LogInterceptor(BaseSourceTask.class);
        final Configuration config = TestHelper.defaultConfig()
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_LOCK_TIMEOUT_MS, 1_000)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        connection.setAutoCommit(false).executeWithoutCommitting(
                "SELECT TOP(0) * FROM dbo.table1 WITH (TABLOCKX)");
        consumeRecordsByTopic(INITIAL_RECORDS_PER_TABLE);
        Awaitility.await().atMost(TestHelper.waitTimeForLogEntries(), TimeUnit.SECONDS).until(
                () -> logInterceptorTask.containsWarnMessage("Going to restart connector after "));
        assertThat(logInterceptorErrorHandler.containsStacktraceElement("Lock request time out period exceeded.")).as("Log contains error related to lock timeout")
                .isTrue();
        connection.rollback();
    }

    private void testStreaming() throws SQLException, InterruptedException {
        for (int i = 0; i < STREAMING_RECORDS_PER_TABLE; i++) {
            final int id = i + INITIAL_RECORDS_PER_TABLE;
            connection.execute(
                    String.format("INSERT INTO table1 VALUES(%s, '%s', %s, '%s')", id, "name" + id, new BigDecimal(id + ".23"), "2018-07-18 13:28:56"));
        }

        // Wait for last written CDC entry
        final int lastId = INITIAL_RECORDS_PER_TABLE + (STREAMING_RECORDS_PER_TABLE - 1);
        TestHelper.waitForCdcRecord(connection, "table1", rs -> rs.getInt("id") == lastId);

        final SourceRecords records = consumeRecordsByTopic(STREAMING_RECORDS_PER_TABLE);
        final List<SourceRecord> table1 = records.recordsForTopic("server1.testDB1.dbo.table1");

        assertThat(table1).hasSize(INITIAL_RECORDS_PER_TABLE);

        for (int i = 0; i < INITIAL_RECORDS_PER_TABLE; i++) {
            final int id = i + INITIAL_RECORDS_PER_TABLE;
            final SourceRecord record1 = table1.get(i);
            final List<SchemaAndValueField> expectedKey1 = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, id));
            final List<SchemaAndValueField> expectedRow1 = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, id),
                    new SchemaAndValueField("name", Schema.OPTIONAL_STRING_SCHEMA, "name" + id),
                    new SchemaAndValueField("price", Decimal.builder(2).parameter("connect.decimal.precision", "8").optional().build(), new BigDecimal(id + ".23")),
                    new SchemaAndValueField("ts", Timestamp.builder().optional().schema(), 1_531_920_536_000l));

            final Struct key1 = (Struct) record1.key();
            final Struct value1 = (Struct) record1.value();
            assertRecord(key1, expectedKey1);
            assertRecord((Struct) value1.get("after"), expectedRow1);
            assertThat(record1.sourceOffset()).hasSize(3);

            Assert.assertTrue(record1.sourceOffset().containsKey("change_lsn"));
            Assert.assertTrue(record1.sourceOffset().containsKey("commit_lsn"));
            Assert.assertTrue(record1.sourceOffset().containsKey("event_serial_no"));
            assertNull(value1.get("before"));
        }
    }

    @Test
    public void takeSchemaOnlySnapshotAndStartStreaming() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        testStreaming();
    }

    @Test
    @FixFor("DBZ-1031")
    public void takeSnapshotFromTableWithReservedName() throws Exception {
        connection.execute(
                "CREATE TABLE [User] (id int, name varchar(30), primary key(id))");

        for (int i = 0; i < INITIAL_RECORDS_PER_TABLE; i++) {
            connection.execute(
                    String.format("INSERT INTO [User] VALUES(%s, '%s')", i, "name" + i));
        }

        TestHelper.enableTableCdc(connection, "User");
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        final Configuration config = TestHelper.defaultConfig()
                .with(TABLE_INCLUDE_LIST, "dbo.User")
                .build();
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final SourceRecords records = consumeRecordsByTopic(INITIAL_RECORDS_PER_TABLE);
        final List<SourceRecord> user = records.recordsForTopic("server1.testDB1.dbo.User");

        assertThat(user).hasSize(INITIAL_RECORDS_PER_TABLE);

        for (int i = 0; i < INITIAL_RECORDS_PER_TABLE; i++) {
            final SourceRecord record1 = user.get(i);
            final List<SchemaAndValueField> expectedKey1 = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, i));
            final List<SchemaAndValueField> expectedRow1 = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, i),
                    new SchemaAndValueField("name", Schema.OPTIONAL_STRING_SCHEMA, "name" + i));

            final Struct key1 = (Struct) record1.key();
            final Struct value1 = (Struct) record1.value();
            assertRecord(key1, expectedKey1);
            assertRecord((Struct) value1.get("after"), expectedRow1);
            assertThat(record1.sourceOffset())
                    .extracting("snapshot").containsExactly(true);
            assertThat(record1.sourceOffset())
                    .extracting("snapshot_completed").containsExactly(i == INITIAL_RECORDS_PER_TABLE - 1);
            assertNull(value1.get("before"));
        }
    }

    @Test
    public void takeSchemaOnlySnapshotAndSendHeartbeat() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(Heartbeat.HEARTBEAT_INTERVAL, 300_000)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        TestHelper.waitForSnapshotToBeCompleted();
        final SourceRecord record = consumeRecord();
        assertThat(record).isNotNull();
        assertThat(record.topic()).startsWith("__debezium-heartbeat");
    }

    @Test
    @FixFor("DBZ-2456")
    public void shouldSelectivelySnapshotTables() throws SQLException, InterruptedException {
        connection.execute(
                "CREATE TABLE table_a (id int, name varchar(30), amount integer primary key(id))",
                "CREATE TABLE table_b (id int, name varchar(30), amount integer primary key(id))");
        connection.execute("INSERT INTO table_a VALUES(10, 'some_name', 120)");
        connection.execute("INSERT INTO table_b VALUES(11, 'some_name', 447)");
        TestHelper.enableTableCdc(connection, "table_a");
        TestHelper.enableTableCdc(connection, "table_b");

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.COLUMN_EXCLUDE_LIST, "dbo.table_a.amount")
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.table_a,dbo.table_b")
                .with(CommonConnectorConfig.SNAPSHOT_MODE_TABLES, "[A-z].*dbo.table_a")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(1);
        List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.table_a");
        List<SourceRecord> tableB = records.recordsForTopic("server1.testDB1.dbo.table_b");

        assertThat(tableA).hasSize(1);
        assertThat(tableB).isNull();
        TestHelper.waitForSnapshotToBeCompleted();
        connection.execute("INSERT INTO table_a VALUES(22, 'some_name', 556)");
        connection.execute("INSERT INTO table_b VALUES(24, 'some_name', 558)");

        records = consumeRecordsByTopic(2);
        tableA = records.recordsForTopic("server1.testDB1.dbo.table_a");
        tableB = records.recordsForTopic("server1.testDB1.dbo.table_b");

        assertThat(tableA).hasSize(1);
        assertThat(tableB).hasSize(1);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-1067")
    public void testColumnExcludeList() throws Exception {
        connection.execute(
                "CREATE TABLE blacklist_column_table_a (id int, name varchar(30), amount integer primary key(id))",
                "CREATE TABLE blacklist_column_table_b (id int, name varchar(30), amount integer primary key(id))");
        connection.execute("INSERT INTO blacklist_column_table_a VALUES(10, 'some_name', 120)");
        connection.execute("INSERT INTO blacklist_column_table_b VALUES(11, 'some_name', 447)");
        TestHelper.enableTableCdc(connection, "blacklist_column_table_a");
        TestHelper.enableTableCdc(connection, "blacklist_column_table_b");

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.COLUMN_EXCLUDE_LIST, "dbo.blacklist_column_table_a.amount")
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.blacklist_column_table_a,dbo.blacklist_column_table_b")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

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
    public void reoderCapturedTables() throws Exception {
        connection.execute(
                "CREATE TABLE table_a (id int, name varchar(30), amount integer primary key(id))",
                "CREATE TABLE table_b (id int, name varchar(30), amount integer primary key(id))");
        connection.execute("INSERT INTO table_a VALUES(10, 'some_name', 120)");
        connection.execute("INSERT INTO table_b VALUES(11, 'some_name', 447)");
        TestHelper.enableTableCdc(connection, "table_a");
        TestHelper.enableTableCdc(connection, "table_b");

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.table_b,dbo.table_a")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(1);
        List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.table_a");
        List<SourceRecord> tableB = records.recordsForTopic("server1.testDB1.dbo.table_b");

        assertThat(tableB).hasSize(1);
        assertThat(tableA).isNull();

        records = consumeRecordsByTopic(1);
        tableA = records.recordsForTopic("server1.testDB1.dbo.table_a");
        assertThat(tableA).hasSize(1);

        stopConnector();
    }

    @Test
    public void reoderCapturedTablesWithOverlappingTableWhitelist() throws Exception {
        connection.execute(
                "CREATE TABLE table_a (id int, name varchar(30), amount integer primary key(id))",
                "CREATE TABLE table_ac (id int, name varchar(30), amount integer primary key(id))",
                "CREATE TABLE table_ab (id int, name varchar(30), amount integer primary key(id))");
        connection.execute("INSERT INTO table_a VALUES(10, 'some_name', 120)");
        connection.execute("INSERT INTO table_ab VALUES(11, 'some_name', 447)");
        connection.execute("INSERT INTO table_ac VALUES(12, 'some_name', 885)");
        TestHelper.enableTableCdc(connection, "table_a");
        TestHelper.enableTableCdc(connection, "table_ab");
        TestHelper.enableTableCdc(connection, "table_ac");

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.table_ab,dbo.table_(.*)")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(1);
        List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.table_a");
        List<SourceRecord> tableB = records.recordsForTopic("server1.testDB1.dbo.table_ab");
        List<SourceRecord> tableC = records.recordsForTopic("server1.testDB1.dbo.table_ac");

        assertThat(tableB).hasSize(1);
        assertThat(tableA).isNull();
        assertThat(tableC).isNull();

        records = consumeRecordsByTopic(1);
        tableA = records.recordsForTopic("server1.testDB1.dbo.table_a");
        assertThat(tableA).hasSize(1);
        assertThat(tableC).isNull();

        records = consumeRecordsByTopic(1);
        tableC = records.recordsForTopic("server1.testDB1.dbo.table_ac");
        assertThat(tableC).hasSize(1);

        stopConnector();
    }

    @Test
    public void reoderCapturedTablesWithoutTableWhitelist() throws Exception {
        connection.execute(
                "CREATE TABLE table_ac (id int, name varchar(30), amount integer primary key(id))",
                "CREATE TABLE table_a (id int, name varchar(30), amount integer primary key(id))",
                "CREATE TABLE table_ab (id int, name varchar(30), amount integer primary key(id))");
        connection.execute("INSERT INTO table_ac VALUES(12, 'some_name', 885)");
        connection.execute("INSERT INTO table_a VALUES(10, 'some_name', 120)");
        connection.execute("INSERT INTO table_ab VALUES(11, 'some_name', 447)");
        TestHelper.enableTableCdc(connection, "table_a");
        TestHelper.enableTableCdc(connection, "table_ab");
        TestHelper.enableTableCdc(connection, "table_ac");

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_EXCLUDE_LIST, "dbo.table1")

                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(1);
        List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.table_a");
        List<SourceRecord> tableB = records.recordsForTopic("server1.testDB1.dbo.table_ab");
        List<SourceRecord> tableC = records.recordsForTopic("server1.testDB1.dbo.table_ac");

        assertThat(tableA).hasSize(1);
        assertThat(tableB).isNull();
        assertThat(tableC).isNull();

        records = consumeRecordsByTopic(1);
        tableB = records.recordsForTopic("server1.testDB1.dbo.table_ab");
        assertThat(tableB).hasSize(1);
        assertThat(tableC).isNull();

        records = consumeRecordsByTopic(1);
        tableC = records.recordsForTopic("server1.testDB1.dbo.table_ac");
        assertThat(tableC).hasSize(1);

        stopConnector();
    }

    @Test
    @FixFor({ "DBZ-1292", "DBZ-3157" })
    public void shouldOutputRecordsInCloudEventsFormat() throws Exception {
        final Configuration config = TestHelper.defaultConfig().build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final SourceRecords snapshotRecords = consumeRecordsByTopic(INITIAL_RECORDS_PER_TABLE);
        final List<SourceRecord> snapshotTable1 = snapshotRecords.recordsForTopic("server1.testDB1.dbo.table1");

        assertThat(snapshotTable1).hasSize(INITIAL_RECORDS_PER_TABLE);

        // test snapshot
        for (SourceRecord sourceRecord : snapshotTable1) {
            CloudEventsConverterTest.shouldConvertToCloudEventsInJson(sourceRecord, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(sourceRecord, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInAvro(sourceRecord, "sqlserver", "server1", false);
        }

        for (int i = 0; i < STREAMING_RECORDS_PER_TABLE; i++) {
            final int id = i + INITIAL_RECORDS_PER_TABLE;
            connection.execute(
                    String.format("INSERT INTO table1 VALUES(%s, '%s', %s, '%s')", id, "name" + id, new BigDecimal(id + ".23"), "2018-07-18 13:28:56"));
        }

        final SourceRecords streamingRecords = consumeRecordsByTopic(STREAMING_RECORDS_PER_TABLE);
        final List<SourceRecord> streamingTable1 = streamingRecords.recordsForTopic("server1.testDB1.dbo.table1");

        assertThat(streamingTable1).hasSize(INITIAL_RECORDS_PER_TABLE);

        // test streaming
        for (SourceRecord sourceRecord : streamingTable1) {
            CloudEventsConverterTest.shouldConvertToCloudEventsInJson(sourceRecord, false,
                    jsonNode -> {
                        assertThat(jsonNode.get(CloudEventsMaker.FieldName.ID).asText()).contains("event_serial_no:1");
                    });
            CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(sourceRecord, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInAvro(sourceRecord, "sqlserver", "server1", false);
        }
    }

    @Test
    @FixFor("DBZ-5198")
    public void shouldHandleBracketsInSnapshotSelect() throws InterruptedException, SQLException {
        connection.execute(
                "CREATE TABLE [user detail] (id int PRIMARY KEY, name varchar(30))",
                "INSERT INTO [user detail] VALUES(1, 'k')");
        TestHelper.enableTableCdc(connection, "user detail");

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.user detail")
                .with(SqlServerConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE, "[dbo].[user detail]")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(1);
        List<SourceRecord> recordsForTopic = records.recordsForTopic("server1.testDB1.dbo.user_detail");
        assertThat(recordsForTopic.get(0).key()).isNotNull();
        Struct value = (Struct) ((Struct) recordsForTopic.get(0).value()).get("after");
        System.out.println("DATA: " + value);
        assertThat(value.get("id")).isEqualTo(1);
        assertThat(value.get("name")).isEqualTo("k");

        stopConnector();
    }

    @Test
    @FixFor("DBZ-6811")
    public void shouldSendHeartbeatsWhenNoRecordsAreSent() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(Heartbeat.HEARTBEAT_INTERVAL, 100)
                .build();

        start(SqlServerConnector.class, config);
        TestHelper.waitForSnapshotToBeCompleted();

        final AtomicInteger heartbeatCount = new AtomicInteger();
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> {
            final SourceRecord record = consumeRecord();
            if (record != null) {
                if (record.topic().startsWith("__debezium-heartbeat")) {
                    assertHeartBeatRecord(record);
                    heartbeatCount.incrementAndGet();
                }
            }
            return heartbeatCount.get() > 10;
        });
    }

    private void assertRecord(Struct record, List<SchemaAndValueField> expected) {
        expected.forEach(schemaAndValueField -> schemaAndValueField.assertFor(record));
    }

    private void assertHeartBeatRecord(SourceRecord heartbeat) {
        assertEquals("__debezium-heartbeat." + TestHelper.TEST_SERVER_NAME, heartbeat.topic());

        Struct key = (Struct) heartbeat.key();
        assertThat(key.get("serverName")).isEqualTo(TestHelper.TEST_SERVER_NAME);

        Struct value = (Struct) heartbeat.value();
        assertThat(value.getInt64("ts_ms")).isLessThanOrEqualTo(Instant.now().toEpochMilli());
    }
}
