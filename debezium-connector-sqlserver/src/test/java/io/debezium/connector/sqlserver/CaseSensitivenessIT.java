/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

/**
 * Integration test to verify behaviour of database with and without case sensitive names.
 *
 * @author Jiri Pechanec
 */
public class CaseSensitivenessIT extends AbstractAsyncEngineConnectorTest {

    private SqlServerConnection connection;

    @Before
    public void before() throws SQLException {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-1051")
    public void caseInsensitiveDatabase() throws Exception {
        connection.execute(
                "CREATE TABLE MyTableOne (Id int primary key, ColA varchar(30))",
                "INSERT INTO MyTableOne VALUES(1, 'a')");
        TestHelper.enableTableCdc(connection, "MyTableOne");
        testDatabase();
    }

    @Test
    @FixFor("DBZ-1051")
    public void caseSensitiveDatabase() throws Exception {
        connection.execute(
                "ALTER DATABASE testDB1 COLLATE Latin1_General_BIN",
                "CREATE TABLE MyTableOne (Id int primary key, ColA varchar(30))",
                "INSERT INTO MyTableOne VALUES(1, 'a')");
        TestHelper.enableTableCdc(connection, "MyTableOne");
        testDatabase();
    }

    private void testDatabase() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.MyTableOne")).hasSize(1);
        SourceRecord record = records.recordsForTopic("server1.testDB1.dbo.MyTableOne").get(0);
        assertSchemaMatchesStruct(
                (Struct) ((Struct) record.value()).get("after"),
                SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB1.dbo.MyTableOne.Value")
                        .field("Id", Schema.INT32_SCHEMA)
                        .field("ColA", Schema.OPTIONAL_STRING_SCHEMA)
                        .build());
        assertSchemaMatchesStruct(
                (Struct) record.key(),
                SchemaBuilder.struct()
                        .name("server1.testDB1.dbo.MyTableOne.Key")
                        .field("Id", Schema.INT32_SCHEMA)
                        .build());
        assertThat(((Struct) ((Struct) record.value()).get("after")).getInt32("Id")).isEqualTo(1);

        connection.execute("INSERT INTO MyTableOne VALUES(2, 'b')");
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.MyTableOne")).hasSize(1);
        record = records.recordsForTopic("server1.testDB1.dbo.MyTableOne").get(0);
        assertSchemaMatchesStruct(
                (Struct) ((Struct) record.value()).get("after"),
                SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB1.dbo.MyTableOne.Value")
                        .field("Id", Schema.INT32_SCHEMA)
                        .field("ColA", Schema.OPTIONAL_STRING_SCHEMA)
                        .build());
        assertSchemaMatchesStruct(
                (Struct) record.key(),
                SchemaBuilder.struct()
                        .name("server1.testDB1.dbo.MyTableOne.Key")
                        .field("Id", Schema.INT32_SCHEMA)
                        .build());
        assertThat(((Struct) ((Struct) record.value()).get("after")).getInt32("Id")).isEqualTo(2);

        connection.execute(
                "CREATE TABLE MyTableTwo (Id int primary key, ColB varchar(30))");
        TestHelper.enableTableCdc(connection, "MyTableTwo");
        connection.execute("INSERT INTO MyTableTwo VALUES(3, 'b')");
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.MyTableTwo")).hasSize(1);
        record = records.recordsForTopic("server1.testDB1.dbo.MyTableTwo").get(0);
        assertSchemaMatchesStruct(
                (Struct) ((Struct) record.value()).get("after"),
                SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB1.dbo.MyTableTwo.Value")
                        .field("Id", Schema.INT32_SCHEMA)
                        .field("ColB", Schema.OPTIONAL_STRING_SCHEMA)
                        .build());
        assertSchemaMatchesStruct(
                (Struct) record.key(),
                SchemaBuilder.struct()
                        .name("server1.testDB1.dbo.MyTableTwo.Key")
                        .field("Id", Schema.INT32_SCHEMA)
                        .build());
        assertThat(((Struct) ((Struct) record.value()).get("after")).getInt32("Id")).isEqualTo(3);
    }
}
