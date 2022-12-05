/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
 * Integration test to verify behaviour of tables that do not have primary key
 *
 * @author Jiri Pechanec (jpechane@redhat.com)
 */
public class TablesWithoutPrimaryKeyIT extends AbstractConnectorTest {

    private static final String DDL_STATEMENTS = "CREATE TABLE t1 (pk INT UNIQUE, val INT);" +
            "CREATE TABLE t2 (pk INT UNIQUE, val INT UNIQUE);" +
            "CREATE TABLE t3 (pk INT, val INT);";

    private static final String DML_STATEMENTS = "INSERT INTO t1 VALUES (1,10);" +
            "INSERT INTO t2 VALUES (2,20);" +
            "INSERT INTO t3 VALUES (3,30);";

    private SqlServerConnection connection;

    @Before
    public void before() throws SQLException {
        TestHelper.createTestDatabase();
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
    public void shouldProcessFromSnapshot() throws Exception {
        connection = TestHelper.testConnection();
        connection.execute(DDL_STATEMENTS + DML_STATEMENTS);

        TestHelper.enableTableCdc(connection, "t1");
        TestHelper.enableTableCdc(connection, "t2");
        TestHelper.enableTableCdc(connection, "t3");

        start(SqlServerConnector.class, TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.t[123]")
                .build());
        assertConnectorIsRunning();

        final int expectedRecordsCount = 1 + 1 + 1;

        final SourceRecords records = consumeRecordsByTopic(expectedRecordsCount);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.t1").get(0).keySchema().field("pk")).isNotNull();
        assertThat(records.recordsForTopic("server1.testDB1.dbo.t1").get(0).keySchema().fields()).hasSize(1);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.t2").get(0).keySchema().field("pk")).isNotNull();
        assertThat(records.recordsForTopic("server1.testDB1.dbo.t2").get(0).keySchema().fields()).hasSize(1);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.t3").get(0).keySchema()).isNull();
    }

    @Test
    public void shouldProcessFromStreaming() throws Exception {
        connection = TestHelper.testConnection();
        connection.execute(
                "CREATE TABLE init (pk INT PRIMARY KEY);",
                "INSERT INTO init VALUES (1);");
        TestHelper.enableTableCdc(connection, "init");

        TestHelper.waitForDisabledCdc(connection, "t1");
        TestHelper.waitForDisabledCdc(connection, "t2");
        TestHelper.waitForDisabledCdc(connection, "t3");

        start(SqlServerConnector.class, TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build());
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        consumeRecordsByTopic(1);

        TestHelper.waitForStreamingStarted();
        TestHelper.waitForMaxLsnAvailable(connection);

        connection.execute(DDL_STATEMENTS);

        Testing.Print.enable();
        TestHelper.enableTableCdc(connection, "t1");
        TestHelper.enableTableCdc(connection, "t2");
        TestHelper.enableTableCdc(connection, "t3");

        TestHelper.waitForEnabledCdc(connection, "t1");
        TestHelper.waitForEnabledCdc(connection, "t2");
        TestHelper.waitForEnabledCdc(connection, "t3");

        connection.execute("INSERT INTO t1 VALUES (1,10);");
        connection.execute("INSERT INTO t2 VALUES (2,20);");
        connection.execute("INSERT INTO t3 VALUES (3,30);");

        TestHelper.waitForCdcRecord(connection, "t1", rs -> rs.getInt("pk") == 1);
        TestHelper.waitForCdcRecord(connection, "t2", rs -> rs.getInt("pk") == 2);
        TestHelper.waitForCdcRecord(connection, "t3", rs -> rs.getInt("pk") == 3);

        final int expectedRecordsCount = 1 + 1 + 1;

        final SourceRecords records = consumeRecordsByTopic(expectedRecordsCount, 24);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.t1").get(0).keySchema().field("pk")).isNotNull();
        assertThat(records.recordsForTopic("server1.testDB1.dbo.t1").get(0).keySchema().fields()).hasSize(1);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.t2").get(0).keySchema().field("pk")).isNotNull();
        assertThat(records.recordsForTopic("server1.testDB1.dbo.t2").get(0).keySchema().fields()).hasSize(1);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.t3").get(0).keySchema()).isNull();
    }
}
