/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static io.debezium.connector.sqlserver.SqlServerConnectorConfig.SNAPSHOT_ISOLATION_MODE;
import static io.debezium.relational.RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST;
import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertNull;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;
import org.fest.assertions.MapAssert;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.CommonConnectorConfig.Version;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotIsolationMode;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.converters.CloudEventsConverterTest;
import io.debezium.converters.CloudEventsMaker;
import io.debezium.data.SchemaAndValueField;
import io.debezium.data.SourceRecordAssert;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.junit.logging.LogInterceptor;
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
    private static final int TOTAL_INITIAL_RECORDS_PER_TABLE = INITIAL_RECORDS_PER_TABLE * TestHelper.TEST_DATABASES.size();

    private SqlServerConnection connection;

    @Before
    public void before() throws SQLException {
        TestHelper.createMultipleTestDatabases();
        connection = TestHelper.testConnection();
        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            connection.execute(
                    "CREATE TABLE table1 (id int, name varchar(30), price decimal(8,2), ts datetime2(0), primary key(id))");

            // Populate database
            for (int i = 0; i < INITIAL_RECORDS_PER_TABLE; i++) {
                connection.execute(
                        String.format("INSERT INTO table1 VALUES(%s, '%s', %s, '%s')", i, "name" + i, new BigDecimal(i + ".23"), "2018-07-18 13:28:56"));
            }

            TestHelper.enableTableCdc(connection, databaseName, "table1");
        });

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
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
        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SNAPSHOT_ISOLATION_MODE.name(), lockingMode.getValue())
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final SourceRecords snapshotRecords = consumeRecordsByTopic(TOTAL_INITIAL_RECORDS_PER_TABLE);

        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);

            final List<SourceRecord> table1 = snapshotRecords.recordsForTopic(TestHelper.topicName(databaseName, "table1"));
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
                assertThat(record1.sourceOffset()).includes(
                        MapAssert.entry("snapshot", true),
                        MapAssert.entry("snapshot_completed", i == INITIAL_RECORDS_PER_TABLE - 1));
                assertNull(value1.get("before"));
            }
        });
    }

    @Test
    public void takeSnapshotAndStartStreaming() throws Exception {
        final Configuration config = TestHelper.defaultMultiDatabaseConfig().build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final SourceRecords snapshotRecords = consumeRecordsByTopic(TOTAL_INITIAL_RECORDS_PER_TABLE);
        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            // Ignore initial records
            final List<SourceRecord> table1 = snapshotRecords.recordsForTopic(TestHelper.topicName(databaseName, "table1"));
            table1.subList(0, INITIAL_RECORDS_PER_TABLE - 1).forEach(record -> {
                assertThat(((Struct) record.value()).getStruct("source").getString("snapshot")).isEqualTo("true");
            });
            assertThat(((Struct) table1.get(INITIAL_RECORDS_PER_TABLE - 1).value()).getStruct("source").getString("snapshot")).isEqualTo("last");
            testStreaming(databaseName);
        });

    }

    @Test
    @FixFor("DBZ-1280")
    public void testDeadlockDetection() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor();
        final Configuration config = TestHelper.defaultSingleDatabaseConfig()
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_LOCK_TIMEOUT_MS, 1_000)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        String databaseName = TestHelper.TEST_FIRST_DATABASE;
        connection.execute("USE " + databaseName);
        connection.setAutoCommit(false).executeWithoutCommitting(
                "SELECT TOP(0) * FROM dbo.table1 WITH (TABLOCKX)");
        consumeRecordsByTopic(INITIAL_RECORDS_PER_TABLE);
        assertConnectorNotRunning();
        assertThat(logInterceptor.containsStacktraceElement("Lock request time out period exceeded.")).as("Log contains error related to lock timeout").isTrue();
        connection.rollback();
    }

    @Test
    public void takeSnapshotWithOldStructAndStartStreaming() throws Exception {
        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SOURCE_STRUCT_MAKER_VERSION, Version.V1)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final SourceRecords snapshotRecords = consumeRecordsByTopic(TOTAL_INITIAL_RECORDS_PER_TABLE);
        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            // Ignore initial records
            final List<SourceRecord> table1 = snapshotRecords.recordsForTopic(TestHelper.topicName(databaseName, "table1"));
            table1.forEach(record -> {
                assertThat(((Struct) record.value()).getStruct("source").getBoolean("snapshot")).isTrue();
            });
            testStreaming(databaseName);
        });
    }

    private void testStreaming(String databaseName) throws SQLException, InterruptedException {
        for (int i = 0; i < STREAMING_RECORDS_PER_TABLE; i++) {
            final int id = i + INITIAL_RECORDS_PER_TABLE;
            connection.execute(
                    String.format("INSERT INTO table1 VALUES(%s, '%s', %s, '%s')", id, "name" + id, new BigDecimal(id + ".23"), "2018-07-18 13:28:56"));
        }

        // Wait for last written CDC entry
        final int lastId = INITIAL_RECORDS_PER_TABLE + (STREAMING_RECORDS_PER_TABLE - 1);
        TestHelper.waitForCdcRecord(connection, databaseName, "table1", rs -> rs.getInt("id") == lastId);

        final SourceRecords records = consumeRecordsByTopic(STREAMING_RECORDS_PER_TABLE);
        final List<SourceRecord> table1 = records.recordsForTopic(TestHelper.topicName(databaseName, "table1"));

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
            assertThat(record1.sourceOffset()).hasSize(4);

            Assert.assertTrue(record1.sourceOffset().containsKey("change_lsn"));
            Assert.assertTrue(record1.sourceOffset().containsKey("commit_lsn"));
            Assert.assertTrue(record1.sourceOffset().containsKey("event_serial_no"));
            assertNull(value1.get("before"));
        }
    }

    @Test
    public void takeSchemaOnlySnapshotAndStartStreaming() throws Exception {
        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            testStreaming(databaseName);
        });
    }

    @Test
    @FixFor("DBZ-1031")
    public void takeSnapshotFromTableWithReservedName() throws Exception {
        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            connection.execute(
                    "CREATE TABLE [User] (id int, name varchar(30), primary key(id))");

            for (int i = 0; i < INITIAL_RECORDS_PER_TABLE; i++) {
                connection.execute(
                        String.format("INSERT INTO [User] VALUES(%s, '%s')", i, "name" + i));
            }

            TestHelper.enableTableCdc(connection, databaseName, "User");
        });

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);

        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(TABLE_INCLUDE_LIST, "dbo.User")
                .build();
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final SourceRecords snapshotRecords = consumeRecordsByTopic(TOTAL_INITIAL_RECORDS_PER_TABLE);
        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            final List<SourceRecord> user = snapshotRecords.recordsForTopic(TestHelper.topicName(databaseName, "User"));

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
                assertThat(record1.sourceOffset()).includes(
                        MapAssert.entry("snapshot", true),
                        MapAssert.entry("snapshot_completed", i == INITIAL_RECORDS_PER_TABLE - 1));
                assertNull(value1.get("before"));
            }
        });
    }

    @Test
    public void takeSchemaOnlySnapshotAndSendHeartbeat() throws Exception {
        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(Heartbeat.HEARTBEAT_INTERVAL, 300_000)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        TestHelper.waitForSnapshotToBeCompleted();
        final SourceRecord record = consumeRecord();
        Assertions.assertThat(record).isNotNull();
        Assertions.assertThat(record.topic()).startsWith("__debezium-heartbeat");
    }

    @Test
    @FixFor("DBZ-1067")
    public void testBlacklistColumn() throws Exception {
        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            connection.execute(
                    "CREATE TABLE blacklist_column_table_a (id int, name varchar(30), amount integer primary key(id))",
                    "CREATE TABLE blacklist_column_table_b (id int, name varchar(30), amount integer primary key(id))");
            connection.execute("INSERT INTO blacklist_column_table_a VALUES(10, 'some_name', 120)");
            connection.execute("INSERT INTO blacklist_column_table_b VALUES(11, 'some_name', 447)");
            TestHelper.enableTableCdc(connection, databaseName, "blacklist_column_table_a");
            TestHelper.enableTableCdc(connection, databaseName, "blacklist_column_table_b");
        });

        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.COLUMN_BLACKLIST, "dbo.blacklist_column_table_a.amount")
                .with(SqlServerConnectorConfig.TABLE_WHITELIST, "dbo.blacklist_column_table_a,dbo.blacklist_column_table_b")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final SourceRecords snapshotRecords = consumeRecordsByTopic(2 * TestHelper.TEST_DATABASES.size());
        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            final List<SourceRecord> tableA = snapshotRecords.recordsForTopic(TestHelper.topicName(databaseName, "blacklist_column_table_a"));
            final List<SourceRecord> tableB = snapshotRecords.recordsForTopic(TestHelper.topicName(databaseName, "blacklist_column_table_b"));

            Schema expectedSchemaA = SchemaBuilder.struct()
                    .optional()
                    .name(TestHelper.schemaName(databaseName, "blacklist_column_table_a", "Value"))
                    .field("id", Schema.INT32_SCHEMA)
                    .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                    .build();
            Struct expectedValueA = new Struct(expectedSchemaA)
                    .put("id", 10)
                    .put("name", "some_name");

            Schema expectedSchemaB = SchemaBuilder.struct()
                    .optional()
                    .name(TestHelper.schemaName(databaseName, "blacklist_column_table_b", "Value"))
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
        });

        stopConnector();
    }

    @Test
    @FixFor("DBZ-2456")
    public void shouldSelectivelySnapshotTables() throws SQLException, InterruptedException {
        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            connection.execute(
                    "CREATE TABLE table_a (id int, name varchar(30), amount integer primary key(id))",
                    "CREATE TABLE table_b (id int, name varchar(30), amount integer primary key(id))");
            connection.execute("INSERT INTO table_a VALUES(10, 'some_name', 120)");
            connection.execute("INSERT INTO table_b VALUES(11, 'some_name', 447)");
            TestHelper.enableTableCdc(connection, databaseName, "table_a");
            TestHelper.enableTableCdc(connection, databaseName, "table_b");
        });

        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.COLUMN_EXCLUDE_LIST, "dbo.table_a.amount")
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.table_a,dbo.table_b")
                .with(CommonConnectorConfig.SNAPSHOT_MODE_TABLES, "[A-z].*dbo.table_a")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords snapshotRecords = consumeRecordsByTopic(TestHelper.TEST_DATABASES.size());
        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            List<SourceRecord> tableA = snapshotRecords.recordsForTopic(TestHelper.topicName(databaseName, "table_a"));
            List<SourceRecord> tableB = snapshotRecords.recordsForTopic(TestHelper.topicName(databaseName, "table_b"));

            Assertions.assertThat(tableA).hasSize(1);
            Assertions.assertThat(tableB).isNull();
        });

        TestHelper.waitForSnapshotToBeCompleted();

        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            connection.execute("INSERT INTO table_a VALUES(22, 'some_name', 556)");
            connection.execute("INSERT INTO table_b VALUES(24, 'some_name', 558)");

            SourceRecords records = consumeRecordsByTopic(2);
            List<SourceRecord> tableA = records.recordsForTopic(TestHelper.topicName(databaseName, "table_a"));
            List<SourceRecord> tableB = records.recordsForTopic(TestHelper.topicName(databaseName, "table_b"));

            Assertions.assertThat(tableA).hasSize(1);
            Assertions.assertThat(tableB).hasSize(1);
        });

        stopConnector();
    }

    @Test
    @FixFor("DBZ-1067")
    public void testColumnExcludeList() throws Exception {
        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            connection.execute(
                    "CREATE TABLE blacklist_column_table_a (id int, name varchar(30), amount integer primary key(id))",
                    "CREATE TABLE blacklist_column_table_b (id int, name varchar(30), amount integer primary key(id))");
            connection.execute("INSERT INTO blacklist_column_table_a VALUES(10, 'some_name', 120)");
            connection.execute("INSERT INTO blacklist_column_table_b VALUES(11, 'some_name', 447)");
            TestHelper.enableTableCdc(connection, databaseName, "blacklist_column_table_a");
            TestHelper.enableTableCdc(connection, databaseName, "blacklist_column_table_b");
        });

        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.COLUMN_EXCLUDE_LIST, "dbo.blacklist_column_table_a.amount")
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.blacklist_column_table_a,dbo.blacklist_column_table_b")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final SourceRecords snapshotRecords = consumeRecordsByTopic(2 * TestHelper.TEST_DATABASES.size());
        TestHelper.forEachDatabase(databaseName -> {
            final List<SourceRecord> tableA = snapshotRecords.recordsForTopic(TestHelper.topicName(databaseName, "blacklist_column_table_a"));
            final List<SourceRecord> tableB = snapshotRecords.recordsForTopic(TestHelper.topicName(databaseName, "blacklist_column_table_b"));

            Schema expectedSchemaA = SchemaBuilder.struct()
                    .optional()
                    .name(TestHelper.schemaName(databaseName, "blacklist_column_table_a", "Value"))
                    .field("id", Schema.INT32_SCHEMA)
                    .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                    .build();
            Struct expectedValueA = new Struct(expectedSchemaA)
                    .put("id", 10)
                    .put("name", "some_name");

            Schema expectedSchemaB = SchemaBuilder.struct()
                    .optional()
                    .name(TestHelper.schemaName(databaseName, "blacklist_column_table_b", "Value"))
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
        });

        stopConnector();
    }

    @Test
    public void reoderCapturedTables() throws Exception {
        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            connection.execute(
                    "CREATE TABLE table_a (id int, name varchar(30), amount integer primary key(id))",
                    "CREATE TABLE table_b (id int, name varchar(30), amount integer primary key(id))");
            connection.execute("INSERT INTO table_a VALUES(10, 'some_name', 120)");
            connection.execute("INSERT INTO table_b VALUES(11, 'some_name', 447)");
            TestHelper.enableTableCdc(connection, databaseName, "table_a");
            TestHelper.enableTableCdc(connection, databaseName, "table_b");
        });

        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.table_b,dbo.table_a")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final SourceRecords records = consumeRecordsByTopic(2 * TestHelper.TEST_DATABASES.size());
        Map<String, List<SourceRecord>> recordsByDatabase = TestHelper.recordsByDatabase(records.allRecordsInOrder());
        TestHelper.forEachDatabase(databaseName -> {
            List<SourceRecord> databaseRecords = recordsByDatabase.get(databaseName);
            Assertions.assertThat(databaseRecords).hasSize(2);
            Assertions.assertThat(databaseRecords.get(0).topic()).isEqualTo(TestHelper.topicName(databaseName, "table_b"));
            Assertions.assertThat(databaseRecords.get(1).topic()).isEqualTo(TestHelper.topicName(databaseName, "table_a"));
        });

        stopConnector();
    }

    @Test
    public void reoderCapturedTablesWithOverlappingTableWhitelist() throws Exception {
        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            connection.execute(
                    "CREATE TABLE table_a (id int, name varchar(30), amount integer primary key(id))",
                    "CREATE TABLE table_ac (id int, name varchar(30), amount integer primary key(id))",
                    "CREATE TABLE table_ab (id int, name varchar(30), amount integer primary key(id))");
            connection.execute("INSERT INTO table_a VALUES(10, 'some_name', 120)");
            connection.execute("INSERT INTO table_ab VALUES(11, 'some_name', 447)");
            connection.execute("INSERT INTO table_ac VALUES(12, 'some_name', 885)");
            TestHelper.enableTableCdc(connection, databaseName, "table_a");
            TestHelper.enableTableCdc(connection, databaseName, "table_ab");
            TestHelper.enableTableCdc(connection, databaseName, "table_ac");
        });

        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.table_ab,dbo.table_(.*)")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final SourceRecords records = consumeRecordsByTopic(3 * TestHelper.TEST_DATABASES.size());
        Map<String, List<SourceRecord>> recordsByDatabase = TestHelper.recordsByDatabase(records.allRecordsInOrder());
        TestHelper.forEachDatabase(databaseName -> {
            List<SourceRecord> databaseRecords = recordsByDatabase.get(databaseName);
            Assertions.assertThat(databaseRecords).hasSize(3);
            Assertions.assertThat(databaseRecords.get(0).topic()).isEqualTo(TestHelper.topicName(databaseName, "table_ab"));
            Assertions.assertThat(databaseRecords.get(1).topic()).isEqualTo(TestHelper.topicName(databaseName, "table_a"));
            Assertions.assertThat(databaseRecords.get(2).topic()).isEqualTo(TestHelper.topicName(databaseName, "table_ac"));
        });

        stopConnector();
    }

    @Test
    public void reoderCapturedTablesWithoutTableWhitelist() throws Exception {
        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            connection.execute(
                    "CREATE TABLE table_ac (id int, name varchar(30), amount integer primary key(id))",
                    "CREATE TABLE table_a (id int, name varchar(30), amount integer primary key(id))",
                    "CREATE TABLE table_ab (id int, name varchar(30), amount integer primary key(id))");
            connection.execute("INSERT INTO table_ac VALUES(12, 'some_name', 885)");
            connection.execute("INSERT INTO table_a VALUES(10, 'some_name', 120)");
            connection.execute("INSERT INTO table_ab VALUES(11, 'some_name', 447)");
            TestHelper.enableTableCdc(connection, databaseName, "table_a");
            TestHelper.enableTableCdc(connection, databaseName, "table_ab");
            TestHelper.enableTableCdc(connection, databaseName, "table_ac");
        });

        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_EXCLUDE_LIST, "dbo.table1")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final SourceRecords records = consumeRecordsByTopic(3 * TestHelper.TEST_DATABASES.size());
        Map<String, List<SourceRecord>> recordsByDatabase = TestHelper.recordsByDatabase(records.allRecordsInOrder());
        TestHelper.forEachDatabase(databaseName -> {
            List<SourceRecord> databaseRecords = recordsByDatabase.get(databaseName);
            Assertions.assertThat(databaseRecords).hasSize(3);
            Assertions.assertThat(databaseRecords.get(0).topic()).isEqualTo(TestHelper.topicName(databaseName, "table_a"));
            Assertions.assertThat(databaseRecords.get(1).topic()).isEqualTo(TestHelper.topicName(databaseName, "table_ab"));
            Assertions.assertThat(databaseRecords.get(2).topic()).isEqualTo(TestHelper.topicName(databaseName, "table_ac"));
        });

        stopConnector();
    }

    @Test
    @FixFor({ "DBZ-1292", "DBZ-3157" })
    public void shouldOutputRecordsInCloudEventsFormat() throws Exception {
        final Configuration config = TestHelper.defaultMultiDatabaseConfig().build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final SourceRecords snapshotRecords = consumeRecordsByTopic(TOTAL_INITIAL_RECORDS_PER_TABLE);
        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);

            final List<SourceRecord> snapshotTable1 = snapshotRecords.recordsForTopic(TestHelper.topicName(databaseName, "table1"));
            assertThat(snapshotTable1).hasSize(INITIAL_RECORDS_PER_TABLE);

            // test snapshot
            for (SourceRecord sourceRecord : snapshotTable1) {
                CloudEventsConverterTest.shouldConvertToCloudEventsInJson(sourceRecord, false);
                CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(sourceRecord, false);
                CloudEventsConverterTest.shouldConvertToCloudEventsInAvro(sourceRecord, "sqlserver", TestHelper.TEST_SERVER_NAME, false);
            }

            for (int i = 0; i < STREAMING_RECORDS_PER_TABLE; i++) {
                final int id = i + INITIAL_RECORDS_PER_TABLE;
                connection.execute(
                        String.format("INSERT INTO table1 VALUES(%s, '%s', %s, '%s')", id, "name" + id, new BigDecimal(id + ".23"), "2018-07-18 13:28:56"));
            }

            final SourceRecords streamingRecords = consumeRecordsByTopic(STREAMING_RECORDS_PER_TABLE);
            final List<SourceRecord> streamingTable1 = streamingRecords.recordsForTopic(TestHelper.topicName(databaseName, "table1"));

            assertThat(streamingTable1).hasSize(STREAMING_RECORDS_PER_TABLE);

            // test streaming
            for (SourceRecord sourceRecord : streamingTable1) {
                CloudEventsConverterTest.shouldConvertToCloudEventsInJson(sourceRecord, false,
                        jsonNode -> {
                            assertThat(jsonNode.get(CloudEventsMaker.FieldName.ID).asText()).contains("event_serial_no:1");
                        });
                CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(sourceRecord, false);
                CloudEventsConverterTest.shouldConvertToCloudEventsInAvro(sourceRecord, "sqlserver", TestHelper.TEST_SERVER_NAME, false);
            }
        });
    }

    private void assertRecord(Struct record, List<SchemaAndValueField> expected) {
        expected.forEach(schemaAndValueField -> schemaAndValueField.assertFor(record));
    }
}
