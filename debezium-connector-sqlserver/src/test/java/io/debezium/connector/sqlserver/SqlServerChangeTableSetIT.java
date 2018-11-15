/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
 * Integration test for the Debezium SQL Server connector.
 *
 * @author Jiri Pechanec
 */
public class SqlServerChangeTableSetIT extends AbstractConnectorTest {

    private SqlServerConnection connection;

    @Before
    public void before() throws SQLException {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        connection.execute(
                "CREATE TABLE tablea (id int primary key, cola varchar(30))",
                "CREATE TABLE tableb (id int primary key, colb varchar(30))",
                "CREATE TABLE tablec (id int primary key, colc varchar(30))"
        );
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
    public void addTable() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_SCHEMA_ONLY)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')"
            );
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')"
            );
        }

        SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tableb")).hasSize(RECORDS_PER_TABLE);

        // Enable CDC for already existing table
        TestHelper.enableTableCdc(connection, "tablec");

        // CDC for newly added table
        connection.execute(
                "CREATE TABLE tabled (id int primary key, cold varchar(30))"
        );
        TestHelper.enableTableCdc(connection, "tabled");

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablec VALUES(" + id + ", 'c')"
            );
            connection.execute(
                    "INSERT INTO tabled VALUES(" + id + ", 'd')"
            );
        }
        records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tablec")).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tabled")).hasSize(RECORDS_PER_TABLE);
        records.recordsForTopic("server1.dbo.tablec").forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct)((Struct)record.value()).get("after"),
                    SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB.dbo.tablec.Value")
                        .field("id", Schema.INT32_SCHEMA)
                        .field("colc", Schema.OPTIONAL_STRING_SCHEMA)
                        .build()
            );
        });
        records.recordsForTopic("server1.dbo.tabled").forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct)((Struct)record.value()).get("after"),
                    SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB.dbo.tabled.Value")
                        .field("id", Schema.INT32_SCHEMA)
                        .field("cold", Schema.OPTIONAL_STRING_SCHEMA)
                        .build()
            );
        });
    }

    @Test
    public void removeTable() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START_1 = 10;
        final int ID_START_2 = 100;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_SCHEMA_ONLY)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_1 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')"
            );
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')"
            );
        }

        SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tableb")).hasSize(RECORDS_PER_TABLE);

        // Disable CDC for a table
        TestHelper.disableTableCdc(connection, "tableb");

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_2 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a2')"
            );
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b2')"
            );
        }
        records = consumeRecordsByTopic(RECORDS_PER_TABLE);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tableb")).isNullOrEmpty();
    }

    @Test
    public void addColumnToTableEndOfBatch() throws Exception {
        addColumnToTable(true);
    }

    @Test
    public void addColumnToTableMiddleOfBatch() throws Exception {
        addColumnToTable(false);
    }

    private void addColumnToTable(boolean pauseAfterCaptureChange) throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START_1 = 10;
        final int ID_START_2 = 100;
        final int ID_START_3 = 1000;
        final int ID_START_4 = 10000;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_SCHEMA_ONLY)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_1 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')"
            );
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')"
            );
        }

        SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tableb")).hasSize(RECORDS_PER_TABLE);
        records.recordsForTopic("server1.dbo.tableb").forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct)((Struct)record.value()).get("after"),
                    SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB.dbo.tableb.Value")
                        .field("id", Schema.INT32_SCHEMA)
                        .field("colb", Schema.OPTIONAL_STRING_SCHEMA)
                        .build()
            );
        });

        // Enable a second capture instance
        connection.execute("ALTER TABLE dbo.tableb ADD newcol INT NOT NULL DEFAULT 0");

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_2 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a2')"
            );
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b2', 2)"
            );
        }
        records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tableb")).hasSize(RECORDS_PER_TABLE);

        records.recordsForTopic("server1.dbo.tableb").forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct)((Struct)record.value()).get("after"),
                    SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB.dbo.tableb.Value")
                        .field("id", Schema.INT32_SCHEMA)
                        .field("colb", Schema.OPTIONAL_STRING_SCHEMA)
                        .build()
            );
        });

        TestHelper.enableTableCdc(connection, "tableb", "after_change");
        if (pauseAfterCaptureChange) {
            Thread.sleep(5_000);
        }

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_3 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a3')"
            );
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b3', 3)"
            );
        }
        records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tableb")).hasSize(RECORDS_PER_TABLE);

        records.recordsForTopic("server1.dbo.tableb").forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct)((Struct)record.value()).get("after"),
                    SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB.dbo.tableb.Value")
                        .field("id", Schema.INT32_SCHEMA)
                        .field("colb", Schema.OPTIONAL_STRING_SCHEMA)
                        .field("newcol", Schema.INT32_SCHEMA)
                        .build()
            );
        });

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_4 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a4')"
            );
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b4', 4)"
            );
        }
        records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tableb")).hasSize(RECORDS_PER_TABLE);
        records.recordsForTopic("server1.dbo.tableb").forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct)((Struct)record.value()).get("after"),
                    SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB.dbo.tableb.Value")
                        .field("id", Schema.INT32_SCHEMA)
                        .field("colb", Schema.OPTIONAL_STRING_SCHEMA)
                        .field("newcol", Schema.INT32_SCHEMA)
                        .build()
            );
        });
    }

    @Test
    public void removeColumnFromTable() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START_1 = 10;
        final int ID_START_2 = 100;
        final int ID_START_3 = 1000;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_SCHEMA_ONLY)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_1 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')"
            );
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')"
            );
        }

        SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tableb")).hasSize(RECORDS_PER_TABLE);
        records.recordsForTopic("server1.dbo.tableb").forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct)((Struct)record.value()).get("after"),
                    SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB.dbo.tableb.Value")
                        .field("id", Schema.INT32_SCHEMA)
                        .field("colb", Schema.OPTIONAL_STRING_SCHEMA)
                        .build()
            );
        });

        // Enable a second capture instance
        connection.execute("ALTER TABLE dbo.tableb DROP COLUMN colb");
        TestHelper.enableTableCdc(connection, "tableb", "after_change");

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_2 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a2')"
            );
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ")"
            );
        }
        records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tableb")).hasSize(RECORDS_PER_TABLE);

        records.recordsForTopic("server1.dbo.tableb").forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct)((Struct)record.value()).get("after"),
                    SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB.dbo.tableb.Value")
                        .field("id", Schema.INT32_SCHEMA)
                        .build()
            );
        });

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_3 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a3')"
            );
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ")"
            );
        }
        records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tableb")).hasSize(RECORDS_PER_TABLE);
        records.recordsForTopic("server1.dbo.tableb").forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct)((Struct)record.value()).get("after"),
                    SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB.dbo.tableb.Value")
                        .field("id", Schema.INT32_SCHEMA)
                        .build()
            );
        });
    }

    @Test
    public void renameColumn() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START_1 = 10;
        final int ID_START_2 = 100;
        final int ID_START_3 = 1000;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_SCHEMA_ONLY)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_1 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')"
            );
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')"
            );
        }

        SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tableb")).hasSize(RECORDS_PER_TABLE);
        records.recordsForTopic("server1.dbo.tableb").forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct)((Struct)record.value()).get("after"),
                    SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB.dbo.tableb.Value")
                        .field("id", Schema.INT32_SCHEMA)
                        .field("colb", Schema.OPTIONAL_STRING_SCHEMA)
                        .build()
            );
        });

        // CDC must be disabled, otherwise rename fails
        TestHelper.disableTableCdc(connection, "tableb");
        // Enable a second capture instance
        connection.execute("exec sp_rename 'tableb.colb', 'newcolb';");
        TestHelper.enableTableCdc(connection, "tableb", "after_change");

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_2 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a2')"
            );
            connection.execute(
                    "INSERT INTO tableb(id,newcolb) VALUES(" + id + ", 'b2')"
            );
        }
        records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tableb")).hasSize(RECORDS_PER_TABLE);

        records.recordsForTopic("server1.dbo.tableb").forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct)((Struct)record.value()).get("after"),
                    SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB.dbo.tableb.Value")
                        .field("id", Schema.INT32_SCHEMA)
                        .field("newcolb", Schema.OPTIONAL_STRING_SCHEMA)
                        .build()
            );
        });

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_3 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a3')"
            );
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b3')"
            );
        }
        records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tableb")).hasSize(RECORDS_PER_TABLE);
        records.recordsForTopic("server1.dbo.tableb").forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct)((Struct)record.value()).get("after"),
                    SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB.dbo.tableb.Value")
                        .field("id", Schema.INT32_SCHEMA)
                        .field("newcolb", Schema.OPTIONAL_STRING_SCHEMA)
                        .build()
            );
        });
    }

    @Test
    public void changeColumn() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START_1 = 10;
        final int ID_START_2 = 100;
        final int ID_START_3 = 1000;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_SCHEMA_ONLY)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_1 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')"
            );
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", '" + id + "')"
            );
        }

        SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tableb")).hasSize(RECORDS_PER_TABLE);
        records.recordsForTopic("server1.dbo.tableb").forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct)((Struct)record.value()).get("after"),
                    SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB.dbo.tableb.Value")
                        .field("id", Schema.INT32_SCHEMA)
                        .field("colb", Schema.OPTIONAL_STRING_SCHEMA)
                        .build()
            );
            final Struct value = ((Struct)record.value()).getStruct("after");
            final int id = value.getInt32("id");
            final String colb = value.getString("colb");
            Assertions.assertThat(Integer.toString(id)).isEqualTo(colb);
        });

        // Enable a second capture instance
        connection.execute("ALTER TABLE dbo.tableb ALTER COLUMN colb INT");
        TestHelper.enableTableCdc(connection, "tableb", "after_change");

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_2 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a2')"
            );
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", '" + id + " ')"
            );
        }
        records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tableb")).hasSize(RECORDS_PER_TABLE);

        records.recordsForTopic("server1.dbo.tableb").forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct)((Struct)record.value()).get("after"),
                    SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB.dbo.tableb.Value")
                        .field("id", Schema.INT32_SCHEMA)
                        .field("colb", Schema.OPTIONAL_INT32_SCHEMA)
                        .build()
            );
            final Struct value = ((Struct)record.value()).getStruct("after");
            final int id = value.getInt32("id");
            final int colb = value.getInt32("colb");
            Assertions.assertThat(id).isEqualTo(colb);
        });

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_3 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a3')"
            );
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", '" + id + " ')"
            );
        }
        records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.tableb")).hasSize(RECORDS_PER_TABLE);
        records.recordsForTopic("server1.dbo.tableb").forEach(record -> {
            assertSchemaMatchesStruct(
                    (Struct)((Struct)record.value()).get("after"),
                    SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB.dbo.tableb.Value")
                        .field("id", Schema.INT32_SCHEMA)
                        .field("colb", Schema.OPTIONAL_INT32_SCHEMA)
                        .build()
            );
            final Struct value = ((Struct)record.value()).getStruct("after");
            final int id = value.getInt32("id");
            final int colb = value.getInt32("colb");
            Assertions.assertThat(id).isEqualTo(colb);
        });
    }
}
