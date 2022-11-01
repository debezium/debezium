/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Testing;

/**
 * Integration test for using snapshot SELECT overrides with the Debezium SQL Server connector.
 *
 * @author Gunnar Morling
 */
public class SnapshotWithSelectOverridesIT extends AbstractConnectorTest {

    private static final int INITIAL_RECORDS_PER_TABLE = 10;

    private SqlServerConnection connection;

    @Before
    public void before() throws SQLException {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        connection.execute(
                "CREATE TABLE table1 (id int, name varchar(30), price decimal(8,2), ts datetime2(0), soft_deleted bit, primary key(id))");
        connection.execute(
                "CREATE TABLE table2 (id int, name varchar(30), price decimal(8,2), ts datetime2(0), soft_deleted bit, primary key(id))");
        connection.execute(
                "CREATE TABLE table3 (id int, name varchar(30), price decimal(8,2), ts datetime2(0), soft_deleted bit, primary key(id))");

        // Populate database
        for (int i = 0; i < INITIAL_RECORDS_PER_TABLE; i++) {
            connection.execute(
                    String.format(
                            "INSERT INTO table1 VALUES(%s, '%s', %s, '%s', %s)",
                            i,
                            "name" + i,
                            new BigDecimal(i + ".23"),
                            "2018-07-18 13:28:56",
                            i % 2));
            connection.execute(
                    String.format(
                            "INSERT INTO table2 VALUES(%s, '%s', %s, '%s', %s)",
                            i,
                            "name" + i,
                            new BigDecimal(i + ".23"),
                            "2018-07-18 13:28:56",
                            i % 2));
            connection.execute(
                    String.format(
                            "INSERT INTO table3 VALUES(%s, '%s', %s, '%s', %s)",
                            i,
                            "name" + i,
                            new BigDecimal(i + ".23"),
                            "2018-07-18 13:28:56",
                            i % 2));
        }

        TestHelper.enableTableCdc(connection, "table1");
        TestHelper.enableTableCdc(connection, "table2");
        TestHelper.enableTableCdc(connection, "table3");

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
    @FixFor({ "DBZ-1224", "DBZ-2975" })
    public void takeSnapshotWithOverridesInMultiPartitionMode() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(
                        RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE,
                        "dbo.table1,dbo.table3")
                .with(
                        RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE + ".dbo.table1",
                        "SELECT * FROM [" + TestHelper.TEST_DATABASE_1 + "].[dbo].[table1] where soft_deleted = 0 order by id desc")
                .with(
                        RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE + ".dbo.table3",
                        "SELECT * FROM [" + TestHelper.TEST_DATABASE_1 + "].[dbo].[table3] where soft_deleted = 0")
                .build();
        takeSnapshotWithOverrides(config, "server1.testDB1.dbo.");
    }

    private void takeSnapshotWithOverrides(Configuration config, String topicPrefix) throws Exception {
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(INITIAL_RECORDS_PER_TABLE + (INITIAL_RECORDS_PER_TABLE + INITIAL_RECORDS_PER_TABLE) / 2);
        List<SourceRecord> table1 = records.recordsForTopic(topicPrefix + "table1");
        List<SourceRecord> table2 = records.recordsForTopic(topicPrefix + "table2");
        List<SourceRecord> table3 = records.recordsForTopic(topicPrefix + "table3");

        // soft_deleted records should be excluded for table1 and table3
        assertThat(table1).hasSize(INITIAL_RECORDS_PER_TABLE / 2);
        assertThat(table2).hasSize(INITIAL_RECORDS_PER_TABLE);
        assertThat(table3).hasSize(INITIAL_RECORDS_PER_TABLE / 2);

        String expectedIdsForTable1 = "86420";
        StringBuilder actualIdsForTable1 = new StringBuilder();

        for (int i = 0; i < INITIAL_RECORDS_PER_TABLE / 2; i++) {
            SourceRecord record = table1.get(i);

            Struct key = (Struct) record.key();
            actualIdsForTable1.append(key.get("id"));

            // soft_deleted records should be excluded
            Struct value = (Struct) record.value();
            assertThat(((Struct) value.get("after")).get("soft_deleted")).isEqualTo(false);
        }

        // the ORDER BY clause should be applied, too
        assertThat(actualIdsForTable1.toString()).isEqualTo(expectedIdsForTable1);
    }

    @Test
    @FixFor({ "DBZ-3429", "DBZ-2975" })
    public void takeSnapshotWithOverridesWithAdditionalWhitespaceInMultiPartitionMode() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(
                        RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE,
                        "  dbo.table1 , dbo.table3  ")
                .with(
                        RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE + ".dbo.table1",
                        "SELECT * FROM [" + TestHelper.TEST_DATABASE_1 + "].[dbo].[table1] where soft_deleted = 0 order by id desc")
                .with(
                        RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE + ".dbo.table3",
                        "SELECT * FROM [" + TestHelper.TEST_DATABASE_1 + "].[dbo].[table3] where soft_deleted = 0")
                .build();
        takeSnapshotWithOverridesWithAdditionalWhitespace(config, "server1.testDB1.dbo.");
    }

    private void takeSnapshotWithOverridesWithAdditionalWhitespace(Configuration config, String topicPrefix) throws Exception {
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(INITIAL_RECORDS_PER_TABLE + (INITIAL_RECORDS_PER_TABLE + INITIAL_RECORDS_PER_TABLE) / 2);
        List<SourceRecord> table1 = records.recordsForTopic(topicPrefix + "table1");
        List<SourceRecord> table2 = records.recordsForTopic(topicPrefix + "table2");
        List<SourceRecord> table3 = records.recordsForTopic(topicPrefix + "table3");

        // soft_deleted records should be excluded for table1 and table3
        assertThat(table1).hasSize(INITIAL_RECORDS_PER_TABLE / 2);
        assertThat(table2).hasSize(INITIAL_RECORDS_PER_TABLE);
        assertThat(table3).hasSize(INITIAL_RECORDS_PER_TABLE / 2);

        String expectedIdsForTable1 = "86420";
        StringBuilder actualIdsForTable1 = new StringBuilder();

        for (int i = 0; i < INITIAL_RECORDS_PER_TABLE / 2; i++) {
            SourceRecord record = table1.get(i);

            Struct key = (Struct) record.key();
            actualIdsForTable1.append(key.get("id"));

            // soft_deleted records should be excluded
            Struct value = (Struct) record.value();
            assertThat(((Struct) value.get("after")).get("soft_deleted")).isEqualTo(false);
        }

        // the ORDER BY clause should be applied, too
        assertThat(actualIdsForTable1.toString()).isEqualTo(expectedIdsForTable1);
    }
}
