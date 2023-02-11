/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

public class SqlServerConnectorMultiPartitionModeIT extends AbstractConnectorTest {

    private SqlServerConnection connection;

    @Before
    public void before() throws SQLException {
        TestHelper.createTestDatabases(TestHelper.TEST_DATABASE_1, TestHelper.TEST_DATABASE_2);
        connection = TestHelper.multiPartitionTestConnection();
        connection.execute(
                "USE " + TestHelper.TEST_DATABASE_1,
                "CREATE TABLE tableA (id int primary key, colA varchar(32))",
                "CREATE TABLE tableB (id int primary key, colB varchar(32))",
                "INSERT INTO tableA VALUES(1, 'a1')",
                "INSERT INTO tableB VALUES(2, 'b')");
        TestHelper.enableTableCdc(connection, "tableA");
        TestHelper.enableTableCdc(connection, "tableB");
        connection.execute(
                "USE " + TestHelper.TEST_DATABASE_2,
                "CREATE TABLE tableA (id int primary key, colA varchar(32))",
                "CREATE TABLE tableC (id int primary key, colC varchar(32))",
                "INSERT INTO tableA VALUES(3, 'a2')",
                "INSERT INTO tableC VALUES(4, 'c')");
        TestHelper.enableTableCdc(connection, "tableA");
        TestHelper.enableTableCdc(connection, "tableC");

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
    public void snapshotAndStreaming() throws Exception {
        final Configuration config = TestHelper.defaultConfig(
                TestHelper.TEST_DATABASE_1,
                TestHelper.TEST_DATABASE_2)
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SqlServerConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        TestHelper.waitForDatabaseSnapshotsToBeCompleted(TestHelper.TEST_DATABASE_1, TestHelper.TEST_DATABASE_2);
        SourceRecords records = consumeRecordsByTopic(4);

        List<SourceRecord> tableA1 = records.recordsForTopic(TestHelper.topicName(TestHelper.TEST_DATABASE_1, "tableA"));
        assertThat(tableA1).hasSize(1);
        assertValue(tableA1.get(0), "colA", "a1");

        List<SourceRecord> tableB = records.recordsForTopic(TestHelper.topicName(TestHelper.TEST_DATABASE_1, "tableB"));
        assertThat(tableB).hasSize(1);
        assertValue(tableB.get(0), "colB", "b");

        List<SourceRecord> tableA2 = records.recordsForTopic(TestHelper.topicName(TestHelper.TEST_DATABASE_2, "tableA"));
        assertThat(tableA2).hasSize(1);
        assertValue(tableA2.get(0), "colA", "a2");

        List<SourceRecord> tableC = records.recordsForTopic(TestHelper.topicName(TestHelper.TEST_DATABASE_2, "tableC"));
        assertThat(tableC).hasSize(1);
        assertValue(tableC.get(0), "colC", "c");

        connection.execute(
                "USE " + TestHelper.TEST_DATABASE_1,
                "INSERT INTO tableA VALUES(5, 'a1s')");
        connection.execute(
                "USE " + TestHelper.TEST_DATABASE_2,
                "INSERT INTO tableA VALUES(6, 'a2s')");

        TestHelper.waitForStreamingStarted();
        records = consumeRecordsByTopic(2);

        tableA1 = records.recordsForTopic(TestHelper.topicName(TestHelper.TEST_DATABASE_1, "tableA"));
        assertThat(tableA1).hasSize(1);
        assertValue(tableA1.get(0), "colA", "a1s");

        tableA2 = records.recordsForTopic(TestHelper.topicName(TestHelper.TEST_DATABASE_2, "tableA"));
        assertThat(tableA1).hasSize(1);
        assertValue(tableA2.get(0), "colA", "a2s");
    }

    private void assertValue(SourceRecord record, String fieldName, Object expected) {
        final Struct value = (Struct) record.value();
        final Struct after = (Struct) value.get("after");
        assertThat(after.get(fieldName)).isEqualTo(expected);
    }
}
