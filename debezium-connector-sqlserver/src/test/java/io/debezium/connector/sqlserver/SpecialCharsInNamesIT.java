/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;
import java.util.List;

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
import io.debezium.util.Testing;

/**
 * Integration test to verify behaviour of database with special chars table names.
 *
 * @author Jiri Pechanec
 */
public class SpecialCharsInNamesIT extends AbstractConnectorTest {

    private SqlServerConnection connection;

    @Before
    public void before() throws SQLException {
        TestHelper.createMultipleTestDatabases();
        connection = TestHelper.testConnection();

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
    @FixFor("DBZ-1546")
    public void shouldParseWhitespaceChars() throws Exception {
        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo\\.UAT WAG CZ\\$Fixed Asset.*, dbo\\.UAT WAG CZ\\$Fixed Prop.*")
                .build();

        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            connection.execute(
                    "CREATE TABLE [UAT WAG CZ$Fixed Asset] (id int primary key, [my col$a] varchar(30))",
                    "CREATE TABLE [UAT WAG CZ$Fixed Prop] (id int primary key, [my col$a] varchar(30))",
                    "INSERT INTO [UAT WAG CZ$Fixed Asset] VALUES(1, 'asset')",
                    "INSERT INTO [UAT WAG CZ$Fixed Prop] VALUES(1, 'prop')");
            TestHelper.enableTableCdc(connection, databaseName, "UAT WAG CZ$Fixed Asset");
            TestHelper.enableTableCdc(connection, databaseName, "person");
        });

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(2 * TestHelper.TEST_DATABASES.size(), false);
        TestHelper.forEachDatabase(databaseName -> {
            Assertions.assertThat(actualRecords.recordsForTopic(TestHelper.topicName(databaseName, "UAT_WAG_CZ_Fixed_Asset"))).hasSize(1);
            Assertions.assertThat(actualRecords.recordsForTopic(TestHelper.topicName(databaseName, "UAT_WAG_CZ_Fixed_Prop"))).hasSize(1);

            List<SourceRecord> carRecords = actualRecords.recordsForTopic(TestHelper.topicName(databaseName, "UAT_WAG_CZ_Fixed_Asset"));
            Assertions.assertThat(carRecords.size()).isEqualTo(1);
            SourceRecord carRecord = carRecords.get(0);

            assertSchemaMatchesStruct(
                    (Struct) ((Struct) carRecord.value()).get("after"),
                    SchemaBuilder.struct()
                            .optional()
                            .name(TestHelper.schemaName(databaseName, "UAT_WAG_CZ_Fixed_Asset", "Value"))
                            .field("id", Schema.INT32_SCHEMA)
                            .field("my col$a", Schema.OPTIONAL_STRING_SCHEMA)
                            .build());
            assertSchemaMatchesStruct(
                    (Struct) carRecord.key(),
                    SchemaBuilder.struct()
                            .name(TestHelper.schemaName(databaseName, "UAT_WAG_CZ_Fixed_Asset", "Key"))
                            .field("id", Schema.INT32_SCHEMA)
                            .build());
            Assertions.assertThat(((Struct) carRecord.value()).getStruct("after").getString("my col$a")).isEqualTo("asset");

            List<SourceRecord> personRecords = actualRecords.recordsForTopic(TestHelper.topicName(databaseName, "UAT_WAG_CZ_Fixed_Prop"));
            Assertions.assertThat(personRecords.size()).isEqualTo(1);
            SourceRecord personRecord = personRecords.get(0);

            assertSchemaMatchesStruct(
                    (Struct) ((Struct) personRecord.value()).get("after"),
                    SchemaBuilder.struct()
                            .optional()
                            .name(TestHelper.schemaName(databaseName, "UAT_WAG_CZ_Fixed_Prop", "Value"))
                            .field("id", Schema.INT32_SCHEMA)
                            .field("my col$a", Schema.OPTIONAL_STRING_SCHEMA)
                            .build());
            assertSchemaMatchesStruct(
                    (Struct) personRecord.key(),
                    SchemaBuilder.struct()
                            .name(TestHelper.schemaName(databaseName, "UAT_WAG_CZ_Fixed_Prop", "Key"))
                            .field("id", Schema.INT32_SCHEMA)
                            .build());
            Assertions.assertThat(((Struct) personRecord.value()).getStruct("after").getString("my col$a")).isEqualTo("prop");
        });
    }

    @Test
    @FixFor("DBZ-1153")
    public void shouldParseSpecialChars() throws Exception {
        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo\\.UAT WAG CZ\\$Fixed Asset.*")
                .with(SqlServerConnectorConfig.SANITIZE_FIELD_NAMES, true)
                .build();

        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            connection.execute(
                    "CREATE TABLE [UAT WAG CZ$Fixed Asset] (id int primary key, [my col$a] varchar(30))",
                    "INSERT INTO [UAT WAG CZ$Fixed Asset] VALUES(1, 'a')");
            TestHelper.enableTableCdc(connection, databaseName, "UAT WAG CZ$Fixed Asset");
        });

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords snapshotRecords = consumeRecordsByTopic(TestHelper.TEST_DATABASES.size());
        TestHelper.forEachDatabase(databaseName -> {
            Assertions.assertThat(snapshotRecords.recordsForTopic(TestHelper.topicName(databaseName, "UAT_WAG_CZ_Fixed_Asset"))).hasSize(1);

            SourceRecord record = snapshotRecords.recordsForTopic(TestHelper.topicName(databaseName, "UAT_WAG_CZ_Fixed_Asset")).get(0);
            assertSchemaMatchesStruct(
                    (Struct) ((Struct) record.value()).get("after"),
                    SchemaBuilder.struct()
                            .optional()
                            .name(TestHelper.schemaName(databaseName, "UAT_WAG_CZ_Fixed_Asset", "Value"))
                            .field("id", Schema.INT32_SCHEMA)
                            .field("my_col_a", Schema.OPTIONAL_STRING_SCHEMA)
                            .build());
            assertSchemaMatchesStruct(
                    (Struct) record.key(),
                    SchemaBuilder.struct()
                            .name(TestHelper.schemaName(databaseName, "UAT_WAG_CZ_Fixed_Asset", "Key"))
                            .field("id", Schema.INT32_SCHEMA)
                            .build());
            Assertions.assertThat(((Struct) record.value()).getStruct("after").getInt32("id")).isEqualTo(1);

            connection.execute("USE " + databaseName);
            connection.execute("INSERT INTO [UAT WAG CZ$Fixed Asset] VALUES(2, 'b')");
        });

        SourceRecords streamingRecords = consumeRecordsByTopic(TestHelper.TEST_DATABASES.size());
        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            Assertions.assertThat(streamingRecords.recordsForTopic(TestHelper.topicName(databaseName, "UAT_WAG_CZ_Fixed_Asset"))).hasSize(1);
            SourceRecord record = streamingRecords.recordsForTopic(TestHelper.topicName(databaseName, "UAT_WAG_CZ_Fixed_Asset")).get(0);
            assertSchemaMatchesStruct(
                    (Struct) ((Struct) record.value()).get("after"),
                    SchemaBuilder.struct()
                            .optional()
                            .name(TestHelper.schemaName(databaseName, "UAT_WAG_CZ_Fixed_Asset", "Value"))
                            .field("id", Schema.INT32_SCHEMA)
                            .field("my_col_a", Schema.OPTIONAL_STRING_SCHEMA)
                            .build());
            assertSchemaMatchesStruct(
                    (Struct) record.key(),
                    SchemaBuilder.struct()
                            .name(TestHelper.schemaName(databaseName, "UAT_WAG_CZ_Fixed_Asset", "Key"))
                            .field("id", Schema.INT32_SCHEMA)
                            .build());
            Assertions.assertThat(((Struct) record.value()).getStruct("after").getInt32("id")).isEqualTo(2);

            connection.execute(
                    "CREATE TABLE [UAT WAG CZ$Fixed Asset Two] (id int primary key, [my col$] varchar(30), Description varchar(30) NOT NULL)");
            TestHelper.enableTableCdc(connection, databaseName, "UAT WAG CZ$Fixed Asset Two");
            connection.execute("INSERT INTO [UAT WAG CZ$Fixed Asset Two] VALUES(3, 'b', 'empty')");
            SourceRecords records = consumeRecordsByTopic(1);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "UAT_WAG_CZ_Fixed_Asset_Two"))).hasSize(1);
            record = records.recordsForTopic(TestHelper.topicName(databaseName, "UAT_WAG_CZ_Fixed_Asset_Two")).get(0);
            assertSchemaMatchesStruct(
                    (Struct) ((Struct) record.value()).get("after"),
                    SchemaBuilder.struct()
                            .optional()
                            .name(TestHelper.schemaName(databaseName, "UAT_WAG_CZ_Fixed_Asset_Two", "Value"))
                            .field("id", Schema.INT32_SCHEMA)
                            .field("my_col_", Schema.OPTIONAL_STRING_SCHEMA)
                            .field("Description", Schema.STRING_SCHEMA)
                            .build());
            assertSchemaMatchesStruct(
                    (Struct) record.key(),
                    SchemaBuilder.struct()
                            .name(TestHelper.schemaName(databaseName, "UAT_WAG_CZ_Fixed_Asset_Two", "Key"))
                            .field("id", Schema.INT32_SCHEMA)
                            .build());
            Assertions.assertThat(((Struct) record.value()).getStruct("after").getInt32("id")).isEqualTo(3);

            connection.execute("UPDATE [UAT WAG CZ$Fixed Asset Two] SET Description='c1' WHERE id=3");
            records = consumeRecordsByTopic(1);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "UAT_WAG_CZ_Fixed_Asset_Two"))).hasSize(1);
            record = records.recordsForTopic(TestHelper.topicName(databaseName, "UAT_WAG_CZ_Fixed_Asset_Two")).get(0);
            assertSchemaMatchesStruct(
                    (Struct) ((Struct) record.value()).get("after"),
                    SchemaBuilder.struct()
                            .optional()
                            .name(TestHelper.schemaName(databaseName, "UAT_WAG_CZ_Fixed_Asset_Two", "Value"))
                            .field("id", Schema.INT32_SCHEMA)
                            .field("my_col_", Schema.OPTIONAL_STRING_SCHEMA)
                            .field("Description", Schema.STRING_SCHEMA)
                            .build());
            assertSchemaMatchesStruct(
                    (Struct) ((Struct) record.value()).get("before"),
                    SchemaBuilder.struct()
                            .optional()
                            .name(TestHelper.schemaName(databaseName, "UAT_WAG_CZ_Fixed_Asset_Two", "Value"))
                            .field("id", Schema.INT32_SCHEMA)
                            .field("my_col_", Schema.OPTIONAL_STRING_SCHEMA)
                            .field("Description", Schema.STRING_SCHEMA)
                            .build());
            Assertions.assertThat(((Struct) record.value()).getStruct("after").getString("Description")).isEqualTo("c1");
            Assertions.assertThat(((Struct) record.value()).getStruct("before").getString("Description")).isEqualTo("empty");
        });

        stopConnector();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            connection.execute("INSERT INTO [UAT WAG CZ$Fixed Asset] VALUES(4, 'b')");
            SourceRecords records = consumeRecordsByTopic(1);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "UAT_WAG_CZ_Fixed_Asset"))).hasSize(1);
            SourceRecord record = records.recordsForTopic(TestHelper.topicName(databaseName, "UAT_WAG_CZ_Fixed_Asset")).get(0);
            assertSchemaMatchesStruct(
                    (Struct) ((Struct) record.value()).get("after"),
                    SchemaBuilder.struct()
                            .optional()
                            .name(TestHelper.schemaName(databaseName, "UAT_WAG_CZ_Fixed_Asset", "Value"))
                            .field("id", Schema.INT32_SCHEMA)
                            .field("my_col_a", Schema.OPTIONAL_STRING_SCHEMA)
                            .build());
            assertSchemaMatchesStruct(
                    (Struct) record.key(),
                    SchemaBuilder.struct()
                            .name(TestHelper.schemaName(databaseName, "UAT_WAG_CZ_Fixed_Asset", "Key"))
                            .field("id", Schema.INT32_SCHEMA)
                            .build());
            Assertions.assertThat(((Struct) record.value()).getStruct("after").getInt32("id")).isEqualTo(4);
        });
    }
}
