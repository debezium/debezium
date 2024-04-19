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
import io.debezium.util.Collect;
import io.debezium.util.Testing;

/**
 * Integration test to verify behaviour of tables that do not have primary key
 *
 * @author Jiri Pechanec (jpechane@redhat.com)
 */
public class TablesWithUniqueIndexOnlyIT extends AbstractConnectorTest {

    private static final String DDL_STATEMENTS = "CREATE TABLE t1 (key1 INT NOT NULL, key2 INT NOT NULL, data VARCHAR(255) NOT NULL, col4 INT NOT NULL);" +
            "CREATE UNIQUE NONCLUSTERED INDEX indexa ON t1 (col4)" +
            "CREATE UNIQUE NONCLUSTERED INDEX indexb ON t1 (key1, key2)";

    private static final String DDL_STATEMENTS_STREAM = "CREATE TABLE t2 (key1 INT NOT NULL, key2 INT NOT NULL, data VARCHAR(255) NOT NULL, col4 INT NOT NULL);" +
            "CREATE UNIQUE NONCLUSTERED INDEX indexb ON t2 (key1, key2)" +
            "CREATE UNIQUE NONCLUSTERED INDEX indexa ON t2 (col4)";

    private static final String DML_STATEMENTS = "INSERT INTO t1 VALUES (1, 10, 'data1', 100);";

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

        TestHelper.enableTableCdc(connection, "t1", "t1_CT", Collect.arrayListOf("key1", "key2", "data"));

        start(SqlServerConnector.class, TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.t1")
                .with(SqlServerConnectorConfig.MSG_KEY_COLUMNS, "dbo.t1:key1,key2;")
                .build());
        assertConnectorIsRunning();

        final int expectedRecordsCount = 1;

        final SourceRecords records = consumeRecordsByTopic(expectedRecordsCount);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.t1").get(0).keySchema().field("key1")).isNotNull();
        assertThat(records.recordsForTopic("server1.testDB1.dbo.t1").get(0).keySchema().field("key2")).isNotNull();
    }

    @Test
    public void shouldProcessFromStreaming() throws Exception {
        connection = TestHelper.testConnection();
        connection.execute(DDL_STATEMENTS + DML_STATEMENTS);
        connection.execute(DDL_STATEMENTS_STREAM);

        TestHelper.enableTableCdc(connection, "t1", "t1_CT", Collect.arrayListOf("key1", "key2", "data"));

        start(SqlServerConnector.class, TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.MSG_KEY_COLUMNS, "dbo.t1:key1,key2;dbo.t2:key1,key2")
                .build());
        assertConnectorIsRunning();

        final int expectedRecordsCount = 1;

        SourceRecords records = consumeRecordsByTopic(expectedRecordsCount);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.t1").get(0).keySchema().field("key1")).isNotNull();
        assertThat(records.recordsForTopic("server1.testDB1.dbo.t1").get(0).keySchema().field("key2")).isNotNull();

        connection.execute("INSERT INTO t1 VALUES (2, 20, 'data2', 200);");

        records = consumeRecordsByTopic(expectedRecordsCount);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.t1").get(0).keySchema().field("key1")).isNotNull();
        assertThat(records.recordsForTopic("server1.testDB1.dbo.t1").get(0).keySchema().field("key2")).isNotNull();

        TestHelper.enableTableCdc(connection, "t2", "t2_CT", Collect.arrayListOf("key1", "key2"));
        TestHelper.waitForEnabledCdc(connection, "t2");
        connection.execute("INSERT INTO t2 VALUES (2, 20, 'data2', 200);");

        records = consumeRecordsByTopic(expectedRecordsCount);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.t2").get(0).keySchema().field("key1")).isNotNull();
        assertThat(records.recordsForTopic("server1.testDB1.dbo.t2").get(0).keySchema().field("key2")).isNotNull();
    }
}
