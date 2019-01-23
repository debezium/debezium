/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static io.debezium.connector.sqlserver.SqlServerConnectorConfig.SNAPSHOT_LOCKING_MODE;
import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertNull;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

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

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotLockingMode;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.data.SchemaAndValueField;
import io.debezium.data.SourceRecordAssert;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.heartbeat.Heartbeat;
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
                "CREATE TABLE table1 (id int, name varchar(30), price decimal(8,2), ts datetime2(0), primary key(id))"
        );

        // Populate database
        for (int i = 0; i < INITIAL_RECORDS_PER_TABLE; i++) {
            connection.execute(
                    String.format("INSERT INTO table1 VALUES(%s, '%s', %s, '%s')", i, "name" + i, new BigDecimal(i + ".23"), "2018-07-18 13:28:56")
            );
        }

        TestHelper.enableTableCdc(connection, "table1");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
//        TestHelper.dropTestDatabase();
    }

    @Test
    public void takeSnapshotInExclusiveMode() throws Exception {
        takeSnapshot(SnapshotLockingMode.EXCLUSIVE);
    }

    @Test
    public void takeSnapshotInSnapshotMode() throws Exception {
        takeSnapshot(SnapshotLockingMode.SNAPSHOT);
    }

    @Test
    public void takeSnapshotInNoneMode() throws Exception {
        takeSnapshot(SnapshotLockingMode.NONE);
    }

    private void takeSnapshot(SnapshotLockingMode lockingMode) throws Exception {
        final Configuration config = TestHelper.defaultConfig()
            .with(SNAPSHOT_LOCKING_MODE.name(), lockingMode.getValue())
            .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final SourceRecords records = consumeRecordsByTopic(INITIAL_RECORDS_PER_TABLE);
        final List<SourceRecord> table1 = records.recordsForTopic("server1.dbo.table1");

        assertThat(table1).hasSize(INITIAL_RECORDS_PER_TABLE);

        for (int i = 0; i < INITIAL_RECORDS_PER_TABLE; i++) {
            final SourceRecord record1 = table1.get(i);
            final List<SchemaAndValueField> expectedKey1 = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, i)
            );
            final List<SchemaAndValueField> expectedRow1 = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, i),
                    new SchemaAndValueField("name", Schema.OPTIONAL_STRING_SCHEMA, "name" + i),
                    new SchemaAndValueField("price", Decimal.builder(2).parameter("connect.decimal.precision", "8").optional().build(), new BigDecimal(i + ".23")),
                    new SchemaAndValueField("ts", Timestamp.builder().optional().schema(), 1_531_920_536_000l)
            );

            final Struct key1 = (Struct)record1.key();
            final Struct value1 = (Struct)record1.value();
            assertRecord(key1, expectedKey1);
            assertRecord((Struct)value1.get("after"), expectedRow1);
            assertThat(record1.sourceOffset()).includes(
                    MapAssert.entry("snapshot", true),
                    MapAssert.entry("snapshot_completed", i == INITIAL_RECORDS_PER_TABLE - 1));
            assertNull(value1.get("before"));
        }
    }

    @Test
    public void takeSnapshotAndStartStreaming() throws Exception {
        final Configuration config = TestHelper.defaultConfig().build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Ignore initial records
        consumeRecordsByTopic(INITIAL_RECORDS_PER_TABLE);

        testStreaming();
    }

    private void testStreaming() throws SQLException, InterruptedException {
        for (int i = 0; i < STREAMING_RECORDS_PER_TABLE; i++) {
            final int id = i + INITIAL_RECORDS_PER_TABLE;
            connection.execute(
                    String.format("INSERT INTO table1 VALUES(%s, '%s', %s, '%s')", id, "name" + id, new BigDecimal(id + ".23"), "2018-07-18 13:28:56")
            );
        }

        final SourceRecords records = consumeRecordsByTopic(STREAMING_RECORDS_PER_TABLE);
        final List<SourceRecord> table1 = records.recordsForTopic("server1.dbo.table1");

        assertThat(table1).hasSize(INITIAL_RECORDS_PER_TABLE);

        for (int i = 0; i < INITIAL_RECORDS_PER_TABLE; i++) {
            final int id = i + INITIAL_RECORDS_PER_TABLE;
            final SourceRecord record1 = table1.get(i);
            final List<SchemaAndValueField> expectedKey1 = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, id)
            );
            final List<SchemaAndValueField> expectedRow1 = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, id),
                    new SchemaAndValueField("name", Schema.OPTIONAL_STRING_SCHEMA, "name" + id),
                    new SchemaAndValueField("price", Decimal.builder(2).parameter("connect.decimal.precision", "8").optional().build(), new BigDecimal(id + ".23")),
                    new SchemaAndValueField("ts", Timestamp.builder().optional().schema(), 1_531_920_536_000l)
            );

            final Struct key1 = (Struct)record1.key();
            final Struct value1 = (Struct)record1.value();
            assertRecord(key1, expectedKey1);
            assertRecord((Struct)value1.get("after"), expectedRow1);
            assertThat(record1.sourceOffset()).hasSize(1);

            Assert.assertTrue(record1.sourceOffset().containsKey("change_lsn"));
            assertNull(value1.get("before"));
        }
    }

    @Test
    public void takeSchemaOnlySnapshotAndStartStreaming() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_SCHEMA_ONLY)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        testStreaming();
    }

    @Test
    @FixFor("DBZ-1031")
    public void takeSnapshotFromTableWithReservedName() throws Exception {
        connection.execute(
                "CREATE TABLE [User] (id int, name varchar(30), primary key(id))"
        );

        for (int i = 0; i < INITIAL_RECORDS_PER_TABLE; i++) {
            connection.execute(
                    String.format("INSERT INTO [User] VALUES(%s, '%s')", i, "name" + i)
            );
        }

        TestHelper.enableTableCdc(connection, "User");
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);

        final Configuration config = TestHelper.defaultConfig()
                .with("table.whitelist", "dbo.User")
                .build();
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final SourceRecords records = consumeRecordsByTopic(INITIAL_RECORDS_PER_TABLE);
        final List<SourceRecord> user = records.recordsForTopic("server1.dbo.User");

        assertThat(user).hasSize(INITIAL_RECORDS_PER_TABLE);

        for (int i = 0; i < INITIAL_RECORDS_PER_TABLE; i++) {
            final SourceRecord record1 = user.get(i);
            final List<SchemaAndValueField> expectedKey1 = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, i)
            );
            final List<SchemaAndValueField> expectedRow1 = Arrays.asList(
                    new SchemaAndValueField("id", Schema.INT32_SCHEMA, i),
                    new SchemaAndValueField("name", Schema.OPTIONAL_STRING_SCHEMA, "name" + i)
            );

            final Struct key1 = (Struct)record1.key();
            final Struct value1 = (Struct)record1.value();
            assertRecord(key1, expectedKey1);
            assertRecord((Struct)value1.get("after"), expectedRow1);
            assertThat(record1.sourceOffset()).includes(
                    MapAssert.entry("snapshot", true),
                    MapAssert.entry("snapshot_completed", i == INITIAL_RECORDS_PER_TABLE - 1));
            assertNull(value1.get("before"));
        }
    }

    @Test
    public void takeSchemaOnlySnapshotAndSendHeartbeat() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_SCHEMA_ONLY)
                .with(Heartbeat.HEARTBEAT_INTERVAL, 300_000)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final SourceRecord record = consumeRecord();

        Assertions.assertThat(record.topic()).startsWith("__debezium-heartbeat");
    }

    @Test
    @FixFor("DBZ-1067")
    public void blacklistColumn() throws Exception {
        connection.execute(
                "CREATE TABLE blacklist_column_table_a (id int, name varchar(30), amount integer primary key(id))",
                "CREATE TABLE blacklist_column_table_b (id int, name varchar(30), amount integer primary key(id))"
        );
        connection.execute("INSERT INTO blacklist_column_table_a VALUES(10, 'some_name', 120)");
        connection.execute("INSERT INTO blacklist_column_table_b VALUES(11, 'some_name', 447)");
        TestHelper.enableTableCdc(connection, "blacklist_column_table_a");
        TestHelper.enableTableCdc(connection, "blacklist_column_table_b");

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.COLUMN_BLACKLIST, "dbo.blacklist_column_table_a.amount")
                .with(SqlServerConnectorConfig.TABLE_WHITELIST, "dbo.blacklist_column_table_a,dbo.blacklist_column_table_b")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

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

    private void assertRecord(Struct record, List<SchemaAndValueField> expected) {
        expected.forEach(schemaAndValueField -> schemaAndValueField.assertFor(record));
    }
 }
