/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.CommonConnectorConfig.SchemaNameAdjustmentMode;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

/**
 * Integration test to verify behaviour of database with special chars table names.
 *
 * @author Jiri Pechanec
 */
public class SpecialCharsInNamesIT extends AbstractAsyncEngineConnectorTest {

    private SqlServerConnection connection;

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-1546")
    public void shouldParseWhitespaceChars() throws Exception {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo\\.UAT WAG CZ\\$Fixed Asset.*, dbo\\.UAT WAG CZ\\$Fixed Prop.*")
                .build();

        connection.execute(
                "CREATE TABLE [UAT WAG CZ$Fixed Asset ['1']]] (id int primary key, [my col$a] varchar(30))",
                "CREATE TABLE [UAT WAG CZ$Fixed Prop_\\%/] (id int primary key, [my col$a] varchar(30))",
                "INSERT INTO [UAT WAG CZ$Fixed Asset ['1']]] VALUES(1, 'asset')",
                "INSERT INTO [UAT WAG CZ$Fixed Prop_\\%/] VALUES(1, 'prop')");
        TestHelper.enableTableCdc(connection, "UAT WAG CZ$Fixed Asset ['1']");
        TestHelper.enableTableCdc(connection, "person");

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(2, false);
        assertThat(actualRecords.recordsForTopic("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Asset___1__")).hasSize(1);
        assertThat(actualRecords.recordsForTopic("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Prop____")).hasSize(1);

        List<SourceRecord> carRecords = actualRecords.recordsForTopic("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Asset___1__");
        assertThat(carRecords.size()).isEqualTo(1);
        SourceRecord carRecord = carRecords.get(0);

        assertSchemaMatchesStruct(
                (Struct) ((Struct) carRecord.value()).get("after"),
                SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Asset___1__.Value")
                        .field("id", Schema.INT32_SCHEMA)
                        .field("my col$a", Schema.OPTIONAL_STRING_SCHEMA)
                        .build());
        assertSchemaMatchesStruct(
                (Struct) carRecord.key(),
                SchemaBuilder.struct()
                        .name("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Asset___1__.Key")
                        .field("id", Schema.INT32_SCHEMA)
                        .build());
        assertThat(((Struct) carRecord.value()).getStruct("after").getString("my col$a")).isEqualTo("asset");

        List<SourceRecord> personRecords = actualRecords.recordsForTopic("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Prop____");
        assertThat(personRecords.size()).isEqualTo(1);
        SourceRecord personRecord = personRecords.get(0);

        assertSchemaMatchesStruct(
                (Struct) ((Struct) personRecord.value()).get("after"),
                SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Prop____.Value")
                        .field("id", Schema.INT32_SCHEMA)
                        .field("my col$a", Schema.OPTIONAL_STRING_SCHEMA)
                        .build());
        assertSchemaMatchesStruct(
                (Struct) personRecord.key(),
                SchemaBuilder.struct()
                        .name("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Prop____.Key")
                        .field("id", Schema.INT32_SCHEMA)
                        .build());
        assertThat(((Struct) personRecord.value()).getStruct("after").getString("my col$a")).isEqualTo("prop");
    }

    @Test
    @FixFor("DBZ-1153")
    public void shouldParseSpecialChars() throws Exception {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo\\.UAT WAG CZ\\$Fixed Asset.*")
                .with(SqlServerConnectorConfig.FIELD_NAME_ADJUSTMENT_MODE, SchemaNameAdjustmentMode.AVRO)
                .build();

        connection.execute(
                "CREATE TABLE [UAT WAG CZ$Fixed Asset ['1']]] (id int primary key, [my col$a] varchar(30), [my col#b] varchar(30))",
                "INSERT INTO [UAT WAG CZ$Fixed Asset ['1']]] VALUES(1, 'a', 'b')");
        TestHelper.enableTableCdc(connection, "UAT WAG CZ$Fixed Asset ['1']");
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Asset___1__")).hasSize(1);

        SourceRecord record = records.recordsForTopic("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Asset___1__").get(0);
        assertSchemaMatchesStruct(
                (Struct) ((Struct) record.value()).get("after"),
                SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Asset___1__.Value")
                        .field("id", Schema.INT32_SCHEMA)
                        .field("my_col_a", Schema.OPTIONAL_STRING_SCHEMA)
                        .field("my_col_b", Schema.OPTIONAL_STRING_SCHEMA)
                        .build());
        assertSchemaMatchesStruct(
                (Struct) record.key(),
                SchemaBuilder.struct()
                        .name("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Asset___1__.Key")
                        .field("id", Schema.INT32_SCHEMA)
                        .build());
        assertThat(((Struct) record.value()).getStruct("after").getInt32("id")).isEqualTo(1);

        connection.execute("INSERT INTO [UAT WAG CZ$Fixed Asset ['1']]] VALUES(2, 'b', 'c')");
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Asset___1__")).hasSize(1);
        record = records.recordsForTopic("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Asset___1__").get(0);
        assertSchemaMatchesStruct(
                (Struct) ((Struct) record.value()).get("after"),
                SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Asset___1__.Value")
                        .field("id", Schema.INT32_SCHEMA)
                        .field("my_col_a", Schema.OPTIONAL_STRING_SCHEMA)
                        .field("my_col_b", Schema.OPTIONAL_STRING_SCHEMA)
                        .build());
        assertSchemaMatchesStruct(
                (Struct) record.key(),
                SchemaBuilder.struct()
                        .name("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Asset___1__.Key")
                        .field("id", Schema.INT32_SCHEMA)
                        .build());
        assertThat(((Struct) record.value()).getStruct("after").getInt32("id")).isEqualTo(2);

        connection.execute(
                "CREATE TABLE [UAT WAG CZ$Fixed Asset Two] (id int primary key, [my col$] varchar(30), Description varchar(30) NOT NULL)");
        TestHelper.enableTableCdc(connection, "UAT WAG CZ$Fixed Asset Two");
        connection.execute("INSERT INTO [UAT WAG CZ$Fixed Asset Two] VALUES(3, 'b', 'empty')");
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Asset_Two")).hasSize(1);
        record = records.recordsForTopic("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Asset_Two").get(0);
        assertSchemaMatchesStruct(
                (Struct) ((Struct) record.value()).get("after"),
                SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Asset_Two.Value")
                        .field("id", Schema.INT32_SCHEMA)
                        .field("my_col_", Schema.OPTIONAL_STRING_SCHEMA)
                        .field("Description", Schema.STRING_SCHEMA)
                        .build());
        assertSchemaMatchesStruct(
                (Struct) record.key(),
                SchemaBuilder.struct()
                        .name("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Asset_Two.Key")
                        .field("id", Schema.INT32_SCHEMA)
                        .build());
        assertThat(((Struct) record.value()).getStruct("after").getInt32("id")).isEqualTo(3);

        connection.execute("UPDATE [UAT WAG CZ$Fixed Asset Two] SET Description='c1' WHERE id=3");
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Asset_Two")).hasSize(1);
        record = records.recordsForTopic("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Asset_Two").get(0);
        assertSchemaMatchesStruct(
                (Struct) ((Struct) record.value()).get("after"),
                SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Asset_Two.Value")
                        .field("id", Schema.INT32_SCHEMA)
                        .field("my_col_", Schema.OPTIONAL_STRING_SCHEMA)
                        .field("Description", Schema.STRING_SCHEMA)
                        .build());
        assertSchemaMatchesStruct(
                (Struct) ((Struct) record.value()).get("before"),
                SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Asset_Two.Value")
                        .field("id", Schema.INT32_SCHEMA)
                        .field("my_col_", Schema.OPTIONAL_STRING_SCHEMA)
                        .field("Description", Schema.STRING_SCHEMA)
                        .build());
        assertThat(((Struct) record.value()).getStruct("after").getString("Description")).isEqualTo("c1");
        assertThat(((Struct) record.value()).getStruct("before").getString("Description")).isEqualTo("empty");

        stopConnector();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        connection.execute("INSERT INTO [UAT WAG CZ$Fixed Asset ['1']]] VALUES(4, 'b', 'c')");
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Asset___1__")).hasSize(1);
        record = records.recordsForTopic("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Asset___1__").get(0);
        assertSchemaMatchesStruct(
                (Struct) ((Struct) record.value()).get("after"),
                SchemaBuilder.struct()
                        .optional()
                        .name("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Asset___1__.Value")
                        .field("id", Schema.INT32_SCHEMA)
                        .field("my_col_a", Schema.OPTIONAL_STRING_SCHEMA)
                        .field("my_col_b", Schema.OPTIONAL_STRING_SCHEMA)
                        .build());
        assertSchemaMatchesStruct(
                (Struct) record.key(),
                SchemaBuilder.struct()
                        .name("server1.testDB1.dbo.UAT_WAG_CZ_Fixed_Asset___1__.Key")
                        .field("id", Schema.INT32_SCHEMA)
                        .build());
        assertThat(((Struct) record.value()).getStruct("after").getInt32("id")).isEqualTo(4);
    }

    @Test
    @FixFor("DBZ-4125")
    public void shouldHandleSpecialCharactersInDatabaseNames() throws Exception {
        final String databaseName = "test-db";
        TestHelper.createTestDatabase(databaseName);
        connection = TestHelper.testConnection(databaseName);

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        final Configuration config = TestHelper.defaultConfig(databaseName)
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.FIELD_NAME_ADJUSTMENT_MODE, SchemaNameAdjustmentMode.NONE)
                .with(CommonConnectorConfig.SCHEMA_NAME_ADJUSTMENT_MODE, SchemaNameAdjustmentMode.AVRO)
                .build();
        connection.execute(
                "CREATE TABLE tablea (id int primary key, cola varchar(30))",
                "INSERT INTO tablea VALUES(1, 'a')");
        TestHelper.enableTableCdc(connection, "tablea");
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        SourceRecords recordsByTopic = consumeRecordsByTopic(1);
        List<SourceRecord> records = recordsByTopic.recordsForTopic("server1.test-db.dbo.tablea");
        assertThat(records).hasSize(1);
        Struct source = (Struct) ((Struct) records.get(0).value()).get("source");
        assertThat(source.get("db")).isEqualTo("test-db");

        TestHelper.waitForMaxLsnAvailable(connection, databaseName);
    }
}
