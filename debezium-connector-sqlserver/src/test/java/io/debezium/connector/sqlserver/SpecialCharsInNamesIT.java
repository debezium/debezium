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
        TestHelper.createTestDatabase();
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
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_WHITELIST, "dbo.car, dbo.person")
                .build();

        connection.execute(
                "CREATE TABLE car (id int primary key, name varchar(30))",
                "CREATE TABLE person (id int primary key, name varchar(30))",
                "INSERT INTO car VALUES(1, 'honda')",
                "INSERT INTO person VALUES(1, 'Jack')"
        );
        TestHelper.enableTableCdc(connection, "car");
        TestHelper.enableTableCdc(connection, "person");

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(2);
        Assertions.assertThat(actualRecords.recordsForTopic("server1.dbo.car")).hasSize(1);
        Assertions.assertThat(actualRecords.recordsForTopic("server1.dbo.person")).hasSize(1);

        List<SourceRecord> carRecords = actualRecords.recordsForTopic("server1.dbo.car");
        Assertions.assertThat(carRecords.size()).isEqualTo(1);
        SourceRecord carRecord = carRecords.get(0);

        assertSchemaMatchesStruct(
                (Struct) ((Struct) carRecord.value()).get("after"),
                SchemaBuilder.struct()
                        .optional()
                        .name("server1.dbo.car.Value")
                        .field("id", Schema.INT32_SCHEMA)
                        .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                        .build()
        );
        assertSchemaMatchesStruct(
                (Struct) carRecord.key(),
                SchemaBuilder.struct()
                        .name("server1.dbo.car.Key")
                        .field("id", Schema.INT32_SCHEMA)
                        .build()
        );
        Assertions.assertThat(((Struct) carRecord.value()).getStruct("after").getString("name")).isEqualTo("honda");

        List<SourceRecord> personRecords = actualRecords.recordsForTopic("server1.dbo.person");
        Assertions.assertThat(personRecords.size()).isEqualTo(1);
        SourceRecord personRecord = personRecords.get(0);

        assertSchemaMatchesStruct(
                (Struct) ((Struct) personRecord.value()).get("after"),
                SchemaBuilder.struct()
                        .optional()
                        .name("server1.dbo.person.Value")
                        .field("id", Schema.INT32_SCHEMA)
                        .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                        .build()
        );
        assertSchemaMatchesStruct(
                (Struct) personRecord.key(),
                SchemaBuilder.struct()
                        .name("server1.dbo.person.Key")
                        .field("id", Schema.INT32_SCHEMA)
                        .build()
        );
        Assertions.assertThat(((Struct) personRecord.value()).getStruct("after").getString("name")).isEqualTo("Jack");
    }

    @Test
    @FixFor("DBZ-1153")
    public void shouldParseSpecialChars() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_WHITELIST, "dbo\\.UAT WAG CZ\\$Fixed Asset.*")
                .build();

        connection.execute(
                "CREATE TABLE [UAT WAG CZ$Fixed Asset] (id int primary key, [my col$a] varchar(30))",
                "INSERT INTO [UAT WAG CZ$Fixed Asset] VALUES(1, 'a')"
        );
        TestHelper.enableTableCdc(connection, "UAT WAG CZ$Fixed Asset");
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(1);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.UAT_WAG_CZ_Fixed_Asset")).hasSize(1);

        SourceRecord record = records.recordsForTopic("server1.dbo.UAT_WAG_CZ_Fixed_Asset").get(0);
        assertSchemaMatchesStruct(
            (Struct) ((Struct) record.value()).get("after"),
            SchemaBuilder.struct()
                .optional()
                .name("server1.dbo.UAT_WAG_CZ_Fixed_Asset.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("my col$a", Schema.OPTIONAL_STRING_SCHEMA)
                .build()
        );
        assertSchemaMatchesStruct(
            (Struct) record.key(),
            SchemaBuilder.struct()
                .name("server1.dbo.UAT_WAG_CZ_Fixed_Asset.Key")
                .field("id", Schema.INT32_SCHEMA)
                .build()
        );
        Assertions.assertThat(((Struct) record.value()).getStruct("after").getInt32("id")).isEqualTo(1);

        connection.execute("INSERT INTO [UAT WAG CZ$Fixed Asset] VALUES(2, 'b')");
        records = consumeRecordsByTopic(1);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.UAT_WAG_CZ_Fixed_Asset")).hasSize(1);
        record = records.recordsForTopic("server1.dbo.UAT_WAG_CZ_Fixed_Asset").get(0);
        assertSchemaMatchesStruct(
            (Struct) ((Struct) record.value()).get("after"),
            SchemaBuilder.struct()
                .optional()
                .name("server1.dbo.UAT_WAG_CZ_Fixed_Asset.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("my col$a", Schema.OPTIONAL_STRING_SCHEMA)
                .build()
        );
        assertSchemaMatchesStruct(
            (Struct) record.key(),
            SchemaBuilder.struct()
                .name("server1.dbo.UAT_WAG_CZ_Fixed_Asset.Key")
                .field("id", Schema.INT32_SCHEMA)
                .build()
        );
        Assertions.assertThat(((Struct) record.value()).getStruct("after").getInt32("id")).isEqualTo(2);

        connection.execute(
                "CREATE TABLE [UAT WAG CZ$Fixed Asset Two] (id int primary key, [my col$] varchar(30), Description varchar(30) NOT NULL)"
        );
        TestHelper.enableTableCdc(connection, "UAT WAG CZ$Fixed Asset Two");
        connection.execute("INSERT INTO [UAT WAG CZ$Fixed Asset Two] VALUES(3, 'b', 'empty')");
        records = consumeRecordsByTopic(1);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.UAT_WAG_CZ_Fixed_Asset_Two")).hasSize(1);
        record = records.recordsForTopic("server1.dbo.UAT_WAG_CZ_Fixed_Asset_Two").get(0);
        assertSchemaMatchesStruct(
            (Struct) ((Struct) record.value()).get("after"),
            SchemaBuilder.struct()
                .optional()
                .name("server1.dbo.UAT_WAG_CZ_Fixed_Asset_Two.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("my col$", Schema.OPTIONAL_STRING_SCHEMA)
                .field("Description", Schema.STRING_SCHEMA)
                .build()
        );
        assertSchemaMatchesStruct(
            (Struct) record.key(),
            SchemaBuilder.struct()
                .name("server1.dbo.UAT_WAG_CZ_Fixed_Asset_Two.Key")
                .field("id", Schema.INT32_SCHEMA)
                .build()
        );
        Assertions.assertThat(((Struct) record.value()).getStruct("after").getInt32("id")).isEqualTo(3);

        connection.execute("UPDATE [UAT WAG CZ$Fixed Asset Two] SET Description='c1' WHERE id=3");
        records = consumeRecordsByTopic(1);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.UAT_WAG_CZ_Fixed_Asset_Two")).hasSize(1);
        record = records.recordsForTopic("server1.dbo.UAT_WAG_CZ_Fixed_Asset_Two").get(0);
        assertSchemaMatchesStruct(
                (Struct) ((Struct) record.value()).get("after"),
                SchemaBuilder.struct()
                    .optional()
                    .name("server1.dbo.UAT_WAG_CZ_Fixed_Asset_Two.Value")
                    .field("id", Schema.INT32_SCHEMA)
                    .field("my col$", Schema.OPTIONAL_STRING_SCHEMA)
                    .field("Description", Schema.STRING_SCHEMA)
                    .build()
        );
        assertSchemaMatchesStruct(
                (Struct) ((Struct) record.value()).get("before"),
                SchemaBuilder.struct()
                    .optional()
                    .name("server1.dbo.UAT_WAG_CZ_Fixed_Asset_Two.Value")
                    .field("id", Schema.INT32_SCHEMA)
                    .field("my col$", Schema.OPTIONAL_STRING_SCHEMA)
                    .field("Description", Schema.STRING_SCHEMA)
                    .build()
        );
        Assertions.assertThat(((Struct) record.value()).getStruct("after").getString("Description")).isEqualTo("c1");
        Assertions.assertThat(((Struct) record.value()).getStruct("before").getString("Description")).isEqualTo("empty");

        stopConnector();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        connection.execute("INSERT INTO [UAT WAG CZ$Fixed Asset] VALUES(4, 'b')");
        records = consumeRecordsByTopic(1);
        Assertions.assertThat(records.recordsForTopic("server1.dbo.UAT_WAG_CZ_Fixed_Asset")).hasSize(1);
        record = records.recordsForTopic("server1.dbo.UAT_WAG_CZ_Fixed_Asset").get(0);
        assertSchemaMatchesStruct(
            (Struct) ((Struct) record.value()).get("after"),
            SchemaBuilder.struct()
                .optional()
                .name("server1.dbo.UAT_WAG_CZ_Fixed_Asset.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("my col$a", Schema.OPTIONAL_STRING_SCHEMA)
                .build()
        );
        assertSchemaMatchesStruct(
            (Struct) record.key(),
            SchemaBuilder.struct()
                .name("server1.dbo.UAT_WAG_CZ_Fixed_Asset.Key")
                .field("id", Schema.INT32_SCHEMA)
                .build()
        );
        Assertions.assertThat(((Struct) record.value()).getStruct("after").getInt32("id")).isEqualTo(4);
    }
}
