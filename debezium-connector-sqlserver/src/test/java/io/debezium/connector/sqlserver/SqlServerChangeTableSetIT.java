/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.document.Array;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.IoUtil;
import io.debezium.util.Testing;

/**
 * Integration test for the Debezium SQL Server connector.
 *
 * @author Jiri Pechanec
 */
public class SqlServerChangeTableSetIT extends AbstractAsyncEngineConnectorTest {

    private static final String ORDERS_TOPIC = "server1.testDB1.dbo.orders";
    private static final String AGENT_JOB_ALREADY_RUNNING = "refused because the job is already running";
    private static final String AGENT_JOB_NOT_RUNNING = "refused because the job is not currently running";
    private static final String CAPTURE_JOB_NAME = "cdc." + TestHelper.TEST_DATABASE_1 + "_capture";

    private SqlServerConnection connection;

    @BeforeEach
    void before() throws SQLException {
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
        // Testing.Debug.enable();
    }

    @AfterEach
    void after() throws SQLException {
        if (connection != null) {
            // The capture job has to be stopped, or the drop of the database in the next test waits for the
            // transaction of the capture job. The next test creates a new database, whose capture job is started
            // by sp_cdc_enable_db.
            stopCaptureJob();
            connection.close();
        }
    }

    @Test
    void addTable() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tableb")).hasSize(RECORDS_PER_TABLE);

        // Enable CDC for already existing table
        TestHelper.enableTableCdc(connection, "tablec");

        // CDC for newly added table
        connection.execute(
                "CREATE TABLE tabled (id int primary key, cold varchar(30))");
        TestHelper.enableTableCdc(connection, "tabled");

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablec VALUES(" + id + ", 'c')");
            connection.execute(
                    "INSERT INTO tabled VALUES(" + id + ", 'd')");
        }
        records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tablec")).hasSize(RECORDS_PER_TABLE);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tabled")).hasSize(RECORDS_PER_TABLE);
        records.recordsForTopic("server1.testDB1.dbo.tablec").forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct) ((Struct) record.value()).get("after"),
                    SchemaBuilder.struct()
                            .optional()
                            .name("server1.testDB1.dbo.tablec.Value")
                            .field("id", Schema.INT32_SCHEMA)
                            .field("colc", Schema.OPTIONAL_STRING_SCHEMA)
                            .build());
        });
        records.recordsForTopic("server1.testDB1.dbo.tabled").forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct) ((Struct) record.value()).get("after"),
                    SchemaBuilder.struct()
                            .optional()
                            .name("server1.testDB1.dbo.tabled.Value")
                            .field("id", Schema.INT32_SCHEMA)
                            .field("cold", Schema.OPTIONAL_STRING_SCHEMA)
                            .build());
        });
    }

    @Test
    void removeTable() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START_1 = 10;
        final int ID_START_2 = 100;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
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

        SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tableb")).hasSize(RECORDS_PER_TABLE);

        // Disable CDC for a table
        TestHelper.disableTableCdc(connection, "tableb");

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_2 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a2')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b2')");
        }
        records = consumeRecordsByTopic(RECORDS_PER_TABLE);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tableb")).isNullOrEmpty();
    }

    @Test
    void addColumnToTableEndOfBatchWithoutLsnLimit() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .build();
        addColumnToTable(config, true);
    }

    @Test
    @FixFor("DBZ-3992")
    public void addColumnToTableEndOfBatchWithLsnLimit() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(SqlServerConnectorConfig.MAX_TRANSACTIONS_PER_ITERATION, 1)
                .build();
        addColumnToTable(config, true);
    }

    @Test
    void addColumnToTableMiddleOfBatchWithoutLsnLimit() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .build();
        addColumnToTable(config, false);
    }

    @Test
    @FixFor("DBZ-3992")
    public void addColumnToTableMiddleOfBatchWithLsnLimit() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(SqlServerConnectorConfig.MAX_TRANSACTIONS_PER_ITERATION, 1)
                .build();
        addColumnToTable(config, true);
    }

    private void addColumnToTable(Configuration config, boolean pauseAfterCaptureChange) throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START_1 = 10;
        final int ID_START_2 = 100;
        final int ID_START_3 = 1000;
        final int ID_START_4 = 10000;

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

        SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
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

        // Enable a second capture instance
        connection.execute("ALTER TABLE dbo.tableb ADD newcol INT NOT NULL DEFAULT 0");

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_2 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a2')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b2', 2)");
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
                            .field("colb", Schema.OPTIONAL_STRING_SCHEMA)
                            .build());
        });

        TestHelper.enableTableCdc(connection, "tableb", "after_change");
        if (pauseAfterCaptureChange) {
            Thread.sleep(5_000);
        }

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_3 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a3')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b3', 3)");
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
                            .field("colb", Schema.OPTIONAL_STRING_SCHEMA)
                            .field("newcol", SchemaBuilder.int32().defaultValue(0).build())
                            .build());
        });

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_4 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a4')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b4', 4)");
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
                            .field("colb", Schema.OPTIONAL_STRING_SCHEMA)
                            .field("newcol", SchemaBuilder.int32().defaultValue(0).build())
                            .build());
        });
    }

    @Test
    void removeColumnFromTable() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START_1 = 10;
        final int ID_START_2 = 100;
        final int ID_START_3 = 1000;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
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

        SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
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

        // Enable a second capture instance
        connection.execute("ALTER TABLE dbo.tableb DROP COLUMN colb");
        TestHelper.enableTableCdc(connection, "tableb", "after_change");

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_2 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a2')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ")");
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
                            .build());
        });

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_3 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a3')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ")");
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
                            .build());
        });
    }

    @Test
    @FixFor("DBZ-2716")
    public void removeColumnFromTableWithoutChangingCapture() throws Exception {
        connection.execute("CREATE TABLE tableb2 (colb1 varchar(30), id int primary key, colb2 varchar(30))");
        TestHelper.enableTableCdc(connection, "tableb2");
        connection.execute("ALTER TABLE dbo.tableb2 DROP COLUMN colb1");

        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 1;
        final int ID_START_1 = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.tableb2")
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(SqlServerConnectorConfig.COLUMN_INCLUDE_LIST, ".*id")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_1 + i;
            connection.execute(
                    "INSERT INTO tableb2 VALUES(" + id + ", 'b2')");
        }

        SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tableb2")).hasSize(RECORDS_PER_TABLE);
        records.recordsForTopic("server1.testDB1.dbo.tableb2").forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct) ((Struct) record.value()).get("after"),
                    SchemaBuilder.struct()
                            .optional()
                            .name("server1.testDB1.dbo.tableb2.Value")
                            .field("id", Schema.INT32_SCHEMA)
                            .build());
        });
    }

    @Test
    void addColumnToTableWithParallelWrites() throws Exception {
        final int RECORDS_PER_TABLE = 20;
        final int TABLES = 2;
        final int ID_START_1 = 10;
        final int ID_START_2 = 100;
        final int ID_START_3 = 1000;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
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

        SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
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

        Executors.newSingleThreadExecutor().submit(() -> {
            try (JdbcConnection connection = TestHelper.testConnection()) {
                for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                    final int id = ID_START_2 + i;
                    connection.execute(
                            "INSERT INTO tablea VALUES(" + id + ", 'a2')");
                    connection.execute(
                            "INSERT INTO tableb(id,colb) VALUES(" + id + ",'b')");
                    Thread.sleep(1000);
                }
            }
            catch (Exception e) {
                e.printStackTrace();
                throw new IllegalArgumentException(e);
            }
        });

        // Enable a second capture instance
        connection.execute("ALTER TABLE dbo.tableb ADD colb2 VARCHAR(32)");
        TestHelper.enableTableCdc(connection, "tableb", "after_change");

        records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tableb")).hasSize(RECORDS_PER_TABLE);

        final AtomicInteger beforeChangeCount = new AtomicInteger();
        final AtomicInteger afterChangeCount = new AtomicInteger();
        records.recordsForTopic("server1.testDB1.dbo.tableb").forEach(record -> {
            if (((Struct) record.value()).getStruct("after").schema().field("colb2") != null) {
                afterChangeCount.incrementAndGet();
            }
            else {
                beforeChangeCount.incrementAndGet();
                assertThat(afterChangeCount.intValue()).isZero();
            }
        });
        assertThat(beforeChangeCount.intValue()).isPositive();
        assertThat(afterChangeCount.intValue()).isPositive();

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_3 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a3')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b1', 'b2')");
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
                            .field("colb", Schema.OPTIONAL_STRING_SCHEMA)
                            .field("colb2", Schema.OPTIONAL_STRING_SCHEMA)
                            .build());
        });
    }

    @Test
    void readHistoryAfterRestart() throws Exception {
        final int RECORDS_PER_TABLE = 1;
        final int TABLES = 2;
        final int ID_START_1 = 10;
        final int ID_START_2 = 100;
        final int ID_START_3 = 1000;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForStreamingStarted();

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_1 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tableb")).hasSize(RECORDS_PER_TABLE);

        // Enable a second capture instance
        connection.execute("ALTER TABLE dbo.tableb DROP COLUMN colb");
        TestHelper.enableTableCdc(connection, "tableb", "after_change");

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_2 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a2')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ")");
        }
        records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tableb")).hasSize(RECORDS_PER_TABLE);

        stopConnector();
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_3 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a3')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ")");
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
                            .build());
        });

        // Validate history change types
        final DocumentReader reader = DocumentReader.defaultReader();
        final List<Document> changes = new ArrayList<>();
        IoUtil.readLines(TestHelper.SCHEMA_HISTORY_PATH, line -> {
            try {
                changes.add(reader.read(line));
            }
            catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
        // 3 tables from snapshot + 1 ALTER
        assertThat(changes).hasSize(3 + 1);
        changes.subList(0, 3).forEach(change -> {
            final Array changeArray = change.getArray("tableChanges");
            assertThat(changeArray.size()).isEqualTo(1);
            final String type = changeArray.get(0).asDocument().getString("type");
            assertThat(type).isEqualTo("CREATE");
        });
        final Array changeArray = changes.get(3).getArray("tableChanges");
        assertThat(changeArray.size()).isEqualTo(1);
        final String type = changeArray.get(0).asDocument().getString("type");
        final String tableIid = changeArray.get(0).asDocument().getString("id");
        assertThat(type).isEqualTo("ALTER");
        assertThat(tableIid).isEqualTo("\"testDB1\".\"dbo\".\"tableb\"");
    }

    @Test
    void renameColumn() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START_1 = 10;
        final int ID_START_2 = 100;
        final int ID_START_3 = 1000;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
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

        SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
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
    @FixFor("debezium/dbz#2940")
    void captureInstanceReplacedBeforeTheFirstPollIsMigrated() throws Exception {
        // The connector detects a new capture instance through the LSN window, which is empty when the connector's
        // offset carries no commit LSN. A capture job that has published nothing gives the connector such an offset,
        // and the capture instance is replaced before the connector's first successful poll.
        startConnectorWithoutCommitLsn();

        // The capture instance is replaced while the connector is running but before its first successful poll
        connection.execute("ALTER TABLE orders ADD amount int");
        TestHelper.disableTableCdc(connection, "orders");
        TestHelper.enableTableCdc(connection, "orders", "orders_amount");
        connection.execute("INSERT INTO orders VALUES (2, 'paid', 20)");
        startCaptureJob();

        SourceRecords records = consumeRecordsByTopic(1);
        assertSingleOrderRecord(records, ordersValueSchema("amount"));
        // The snapshot creates the table and the replaced capture instance migrates it once
        assertThat(countSchemaChanges("CREATE")).isEqualTo(1);
        assertThat(countSchemaChanges("ALTER")).isEqualTo(1);

        // The offset carries a commit LSN now, so the LSN window nominates the capture instance as well
        connection.execute("ALTER TABLE orders ADD quantity int");
        TestHelper.disableTableCdc(connection, "orders");
        TestHelper.enableTableCdc(connection, "orders", "orders_quantity");
        connection.execute("INSERT INTO orders VALUES (3, 'shipped', 30, 5)");

        records = consumeRecordsByTopic(1);
        assertSingleOrderRecord(records, ordersValueSchema("amount", "quantity"));
        // The capture instance is nominated twice, but the table is migrated only once
        assertThat(countSchemaChanges("ALTER")).isEqualTo(2);

        // The capture instance is replaced again, but the schema of the table did not change
        TestHelper.disableTableCdc(connection, "orders");
        TestHelper.enableTableCdc(connection, "orders", "orders_unchanged");
        connection.execute("INSERT INTO orders VALUES (4, 'delivered', 40, 6)");

        records = consumeRecordsByTopic(1);
        assertSingleOrderRecord(records, ordersValueSchema("amount", "quantity"));
        assertThat(countSchemaChanges("ALTER")).isEqualTo(2);
    }

    @Test
    @FixFor("debezium/dbz#2940")
    void droppedColumnIsNotMigrated() throws Exception {
        // A column is dropped from the captured table while the connector streams and CDC is not re-enabled on the
        // table. The capture instance keeps the dropped column, so the change table still carries it and the connector
        // must keep decoding it with the schema it has, instead of migrating the table to a schema that no longer
        // describes the change table.
        startConnectorWithoutCommitLsn();

        // The column is dropped before the connector's first successful poll, so the capture instance keeps it
        connection.execute("ALTER TABLE orders DROP COLUMN status");
        connection.execute("INSERT INTO orders (id) VALUES (1)");
        startCaptureJob();

        final SourceRecords records = consumeRecordsByTopic(1);
        // The connector keeps the column in the schema, so it keeps decoding the change table with it
        assertSingleOrderRecord(records, ordersValueSchema());
        // The table is not migrated, so the schema keeps the dropped column and no ALTER is emitted
        assertThat(countSchemaChanges("ALTER")).isZero();
    }

    /**
     * Creates the {@code orders} table and starts the connector with an offset that carries no commit LSN, so that the
     * connector cannot detect a replaced capture instance through the LSN window.
     */
    private void startConnectorWithoutCommitLsn() throws Exception {
        // The capture job has to be stopped before the database is dropped, or the drop waits for the transaction of
        // the capture job
        stopCaptureJob();
        connection.close();
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        connection.execute("CREATE TABLE orders (id int primary key, status varchar(30))");
        TestHelper.enableTableCdc(connection, "orders");
        // The capture job of the new database is started by sp_cdc_enable_db, so it has to be stopped again, or it
        // publishes changes before the test makes them
        stopCaptureJob();
        clearLsnTimeMapping();

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();
        // The snapshot stores the maximum LSN as the commit LSN of the connector's offset, and the capture job is
        // stopped, so the maximum LSN cannot have changed since the snapshot read it
        assertThat(maxLsn()).as("The maximum LSN must not be available, so that the offset carries no commit LSN").isEqualTo(Lsn.NULL);
    }

    /**
     * Stops the CDC capture job, so that it publishes nothing and can be started again.
     */
    private void stopCaptureJob() {
        Awaitility.await("capture job stopped")
                .atMost(Duration.ofSeconds(60))
                .pollDelay(Duration.ZERO)
                .pollInterval(Duration.ofMillis(100))
                .until(() -> {
                    try {
                        connection.execute("EXEC sys.sp_cdc_stop_job");
                        return true;
                    }
                    catch (SQLException e) {
                        // The SQL Server Agent refuses to stop the capture job while it is idle between its scans.
                        // An idle capture job publishes nothing, which is the state this method wants, but the Agent
                        // starts a capture job that has not run yet, so the refusal is a success only once the
                        // capture job has run.
                        return isAgentRefusal(e, AGENT_JOB_NOT_RUNNING) && captureJobHasRun();
                    }
                });
    }

    /**
     * Starts the CDC capture job, so that it publishes the changes the test makes. The Agent refuses the start with a
     * stale "already running" answer, while it is still finishing the start request of the capture job, so the capture
     * job is stopped and started again in that case.
     */
    private void startCaptureJob() {
        Awaitility.await("capture job started")
                .atMost(Duration.ofSeconds(60))
                .pollDelay(Duration.ZERO)
                .pollInterval(Duration.ofMillis(100))
                .until(() -> {
                    try {
                        connection.execute("EXEC sys.sp_cdc_start_job");
                        return true;
                    }
                    catch (SQLException e) {
                        if (!isAgentRefusal(e, AGENT_JOB_ALREADY_RUNNING)) {
                            return false;
                        }
                        // The capture job publishes nothing until it runs, so the stale refusal is answered by stopping
                        // it, which makes the state of the Agent consistent. The next attempt starts it for real.
                        stopCaptureJob();
                        return false;
                    }
                });
    }

    private static boolean isAgentRefusal(SQLException e, String reason) {
        return e.getMessage() != null && e.getMessage().contains(reason);
    }

    /**
     * @return {@code true} if the capture job of the test database has run, which means that the SQL Server Agent has
     *         started it and will not start it again
     */
    private boolean captureJobHasRun() throws SQLException {
        return connection.prepareQueryAndMap(
                "SELECT COUNT(*) FROM msdb.dbo.sysjobactivity WHERE job_id = (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = ?) AND start_execution_date IS NOT NULL",
                preparer -> preparer.setString(1, CAPTURE_JOB_NAME),
                connection.singleResultMapper(resultSet -> resultSet.getLong(1) > 0, "Cannot read the state of the capture job"));
    }

    /**
     * Removes the rows of {@code cdc.lsn_time_mapping}, so that the maximum LSN is not available and the connector's
     * offset carries no commit LSN. The rows with {@code tran_id = 0x00} have to be removed as well: they carry the
     * start LSN of the capture instance, and the maximum LSN is taken over all the rows of the table.
     */
    private void clearLsnTimeMapping() throws SQLException {
        Awaitility.await("The capture job published nothing")
                .atMost(Duration.ofSeconds(30))
                .pollDelay(Duration.ZERO)
                .pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    connection.execute("DELETE FROM cdc.lsn_time_mapping");
                    return Lsn.NULL.equals(maxLsn());
                });
    }

    /**
     * @return the maximum LSN the capture job has published, or {@link Lsn#NULL} if it has published nothing
     */
    private Lsn maxLsn() throws SQLException {
        return connection.getMaxLsn(TestHelper.TEST_DATABASE_1);
    }

    private void assertSingleOrderRecord(SourceRecords records, Schema valueSchema) {
        final List<SourceRecord> orderRecords = records.recordsForTopic(ORDERS_TOPIC);
        assertThat(orderRecords).hasSize(1);
        orderRecords.forEach(record -> assertSchemaMatchesStruct((Struct) ((Struct) record.value()).get("after"), valueSchema));
    }

    private static Schema ordersValueSchema(String... optionalIntColumns) {
        final SchemaBuilder builder = SchemaBuilder.struct()
                .optional()
                .name(ORDERS_TOPIC + ".Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("status", Schema.OPTIONAL_STRING_SCHEMA);
        for (String column : optionalIntColumns) {
            builder.field(column, Schema.OPTIONAL_INT32_SCHEMA);
        }
        return builder.build();
    }

    private static int countSchemaChanges(String type) throws IOException {
        final DocumentReader reader = DocumentReader.defaultReader();
        final List<Document> changes = new ArrayList<>();
        IoUtil.readLines(TestHelper.SCHEMA_HISTORY_PATH, line -> {
            try {
                changes.add(reader.read(line));
            }
            catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
        return (int) changes.stream()
                .flatMap(change -> change.getArray("tableChanges").streamValues())
                .filter(tableChange -> type.equals(tableChange.asDocument().getString("type")))
                .count();
    }

    @Test
    void changeColumn() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START_1 = 10;
        final int ID_START_2 = 100;
        final int ID_START_3 = 1000;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_1 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", '" + id + "')");
        }

        SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
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
            final Struct value = ((Struct) record.value()).getStruct("after");
            final int id = value.getInt32("id");
            final String colb = value.getString("colb");
            assertThat(Integer.toString(id)).isEqualTo(colb);
        });

        // Enable a second capture instance
        connection.execute("ALTER TABLE dbo.tableb ALTER COLUMN colb INT");
        TestHelper.enableTableCdc(connection, "tableb", "after_change");

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_2 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a2')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", '" + id + " ')");
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
                            .field("colb", Schema.OPTIONAL_INT32_SCHEMA)
                            .build());
            final Struct value = ((Struct) record.value()).getStruct("after");
            final int id = value.getInt32("id");
            final int colb = value.getInt32("colb");
            assertThat(id).isEqualTo(colb);
        });

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_3 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a3')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", '" + id + " ')");
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
                            .field("colb", Schema.OPTIONAL_INT32_SCHEMA)
                            .build());
            final Struct value = ((Struct) record.value()).getStruct("after");
            final int id = value.getInt32("id");
            final int colb = value.getInt32("colb");
            assertThat(id).isEqualTo(colb);
        });
    }

    @Test
    @FixFor("DBZ-1491")
    public void addDefaultValue() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        TestHelper.waitForStreamingStarted();
        TestHelper.waitForMaxLsnAvailable(connection);

        connection.execute("ALTER TABLE dbo.tableb ADD DEFAULT ('default_value') FOR colb");
        TestHelper.enableTableCdc(connection, "tableb", "after_change");

        connection.execute("INSERT INTO tableb VALUES('1', 'some_value')");
        TestHelper.waitForCdcRecord(connection, "tableb", "after_change", rs -> rs.getInt("id") == 1);

        List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic("server1.testDB1.dbo.tableb");
        assertThat(records).hasSize(1);
        Testing.debug("Records: " + records);
        Testing.debug("Value Schema: " + records.get(0).valueSchema());
        Testing.debug("Fields: " + records.get(0).valueSchema().fields());
        Testing.debug("After Schema: " + records.get(0).valueSchema().field("after").schema());
        Testing.debug("After Columns: " + records.get(0).valueSchema().field("after").schema().fields());

        Schema colbSchema = records.get(0).valueSchema().field("after").schema().field("colb").schema();
        Testing.debug("ColumnB Schema: " + colbSchema);
        Testing.debug("ColumnB Schema Default Value: " + colbSchema.defaultValue());
        assertThat(colbSchema.defaultValue()).isNotNull();
        assertThat(colbSchema.defaultValue()).isEqualTo("default_value");
    }

    @Test
    @FixFor("DBZ-1491")
    public void alterDefaultValue() throws Exception {
        connection.execute("CREATE TABLE table_dv (id int primary key, colb varchar(30))");
        connection.execute("ALTER TABLE dbo.table_dv ADD CONSTRAINT DV_colb DEFAULT ('default_value') FOR colb");
        TestHelper.enableTableCdc(connection, "table_dv");

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        connection.execute("INSERT INTO table_dv VALUES('1', 'some_value')");
        consumeRecordsByTopic(1);

        // Default value constraint cannot be modified. Drop existing and create a new one instead.
        connection.execute("ALTER TABLE dbo.table_dv DROP CONSTRAINT DV_colb");
        connection.execute("ALTER TABLE dbo.table_dv ADD DEFAULT ('new_default_value') FOR colb");
        TestHelper.enableTableCdc(connection, "table_dv", "after_change");

        connection.execute("INSERT INTO table_dv VALUES('2', 'some_value2')");
        List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic("server1.testDB1.dbo.table_dv");
        assertThat(records).hasSize(1);

        Schema colbSchema = records.get(0).valueSchema().field("after").schema().field("colb").schema();
        assertThat(colbSchema.defaultValue()).isNotNull();
        assertThat(colbSchema.defaultValue()).isEqualTo("new_default_value");
    }

    @Test
    @FixFor("debezium/dbz#960")
    void excludedCaptureInstanceIsNotEnumerated() throws Exception {
        // tableb already has its default capture instance "dbo_tableb"; add a second one for the same source table.
        TestHelper.enableTableCdc(connection, "tableb", "tableb_v2");

        // Without a filter, both capture instances of tableb are enumerated.
        assertThat(captureInstancesFor(connection, "dbo", "tableb"))
                .containsExactlyInAnyOrder("dbo_tableb", "tableb_v2");

        // With the second instance excluded, only the default one is enumerated (its function is never queried).
        try (SqlServerConnection filtered = TestHelper.testConnection(TestHelper.TEST_DATABASE_1,
                builder -> builder.with(SqlServerConnectorConfig.CAPTURE_INSTANCE_EXCLUDE_LIST, "tableb_v2"))) {
            filtered.connect();
            assertThat(captureInstancesFor(filtered, "dbo", "tableb"))
                    .containsExactly("dbo_tableb");
            // Capture instances of other tables are unaffected.
            assertThat(captureInstancesFor(filtered, "dbo", "tablea"))
                    .containsExactly("dbo_tablea");
        }
    }

    @Test
    @FixFor("debezium/dbz#960")
    void includeListLimitsEnumerationToMatchingCaptureInstances() throws Exception {
        TestHelper.enableTableCdc(connection, "tableb", "tableb_v2");

        try (SqlServerConnection filtered = TestHelper.testConnection(TestHelper.TEST_DATABASE_1,
                builder -> builder.with(SqlServerConnectorConfig.CAPTURE_INSTANCE_INCLUDE_LIST, "dbo_table.*"))) {
            filtered.connect();
            // Only capture instances whose names match the include pattern survive; "tableb_v2" does not.
            assertThat(captureInstancesFor(filtered, "dbo", "tableb"))
                    .containsExactly("dbo_tableb");
            assertThat(captureInstancesFor(filtered, "dbo", "tablea"))
                    .containsExactly("dbo_tablea");
        }
    }

    @Test
    @FixFor("debezium/dbz#2800")
    void excludedCaptureInstanceWithSameStartLsnDoesNotShadowIncludedOne() throws Exception {
        // tableb already has "dbo_tableb"; add a newer capture instance for the same source table. The CDC cleanup job
        // moves start_lsn of every capture instance to the same low water mark once the retention period has passed,
        // so in production both instances of a table end up with the same start LSN. Reproduce that with the cleanup
        // procedure the job uses.
        TestHelper.enableTableCdc(connection, "tableb", "tableb_v2");
        // The low water mark has to be an LSN the capture job has processed, so produce one first.
        connection.execute("INSERT INTO tableb VALUES(100, 'b')");
        TestHelper.waitForCdcRecord(connection, "tableb", rs -> rs.getInt("id") == 100);
        connection.execute("DECLARE @lwm binary(10) = sys.fn_cdc_get_max_lsn();"
                + " EXEC sys.sp_cdc_cleanup_change_table @capture_instance = 'dbo_tableb', @low_water_mark = @lwm, @threshold = 5000;"
                + " EXEC sys.sp_cdc_cleanup_change_table @capture_instance = 'tableb_v2', @low_water_mark = @lwm, @threshold = 5000");

        // Without a filter the newest instance wins, as before.
        assertThat(captureInstancesFor(connection, "dbo", "tableb")).containsExactly("tableb_v2");

        // Excluding the newest instance must fall back to the older one instead of dropping the table.
        try (SqlServerConnection filtered = TestHelper.testConnection(TestHelper.TEST_DATABASE_1,
                builder -> builder.with(SqlServerConnectorConfig.CAPTURE_INSTANCE_EXCLUDE_LIST, "tableb_v2"))) {
            filtered.connect();
            assertThat(captureInstancesFor(filtered, "dbo", "tableb")).containsExactly("dbo_tableb");
        }
        // The include list has the same blind spot.
        try (SqlServerConnection filtered = TestHelper.testConnection(TestHelper.TEST_DATABASE_1,
                builder -> builder.with(SqlServerConnectorConfig.CAPTURE_INSTANCE_INCLUDE_LIST, "dbo_table.*"))) {
            filtered.connect();
            assertThat(captureInstancesFor(filtered, "dbo", "tableb")).containsExactly("dbo_tableb");
        }

        // Streaming with the exclusion in place must still deliver the changes of tableb.
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(SqlServerConnectorConfig.CAPTURE_INSTANCE_EXCLUDE_LIST, "tableb_v2")
                .build();
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        connection.execute("INSERT INTO tablea VALUES(1, 'a')", "INSERT INTO tableb VALUES(1, 'b')");

        final SourceRecords records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tablea")).hasSize(1);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tableb")).hasSize(1);
    }

    private static List<String> captureInstancesFor(SqlServerConnection connection, String schema, String table) throws SQLException {
        return connection.getChangeTables(TestHelper.TEST_DATABASE_1).stream()
                .filter(ct -> schema.equals(ct.getSourceTableId().schema()) && table.equals(ct.getSourceTableId().table()))
                .map(SqlServerChangeTable::getCaptureInstance)
                .collect(Collectors.toList());
    }
}
