/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;
import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;

/**
 * Integration test to verify behaviour of tables that do not have primary key
 *
 * @author Jiri Pechanec (jpechane@redhat.com)
 */
public class TablesWithoutPrimaryKeyIT extends AbstractRecordsProducerTest {

    private static final String STATEMENTS = "CREATE SCHEMA nopk;" +
            "CREATE TABLE nopk.t1 (pk INT UNIQUE, val INT);" +
            "CREATE TABLE nopk.t2 (pk INT UNIQUE, val INT UNIQUE);" +
            "CREATE TABLE nopk.t3 (pk INT, val INT);" +
            "INSERT INTO nopk.t1 VALUES (1,10);" +
            "INSERT INTO nopk.t2 VALUES (2,20);" +
            "INSERT INTO nopk.t3 VALUES (3,30);";

    @Before
    public void before() throws SQLException {
        TestHelper.dropAllSchemas();
    }

    @Test
    public void shouldProcessFromSnapshot() throws Exception {
        TestHelper.execute(STATEMENTS);

        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "nopk")
                .build());
        assertConnectorIsRunning();

        final int expectedRecordsCount = 1 + 1 + 1;

        TestConsumer consumer = testConsumer(expectedRecordsCount, "nopk");
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
        final Map<String, List<SourceRecord>> recordsByTopic = recordsByTopic(expectedRecordsCount, consumer);
        Assertions.assertThat(recordsByTopic.get("test_server.nopk.t1").get(0).keySchema().field("pk")).isNotNull();
        Assertions.assertThat(recordsByTopic.get("test_server.nopk.t1").get(0).keySchema().fields()).hasSize(1);
        Assertions.assertThat(recordsByTopic.get("test_server.nopk.t2").get(0).keySchema().field("pk")).isNotNull();
        Assertions.assertThat(recordsByTopic.get("test_server.nopk.t2").get(0).keySchema().fields()).hasSize(1);
        Assertions.assertThat(recordsByTopic.get("test_server.nopk.t3").get(0).keySchema()).isNull();
    }

    @Test
    public void shouldProcessFromSnapshotOld() throws Exception {
        TestHelper.execute(STATEMENTS);

        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY)
                .with(PostgresConnectorConfig.SCHEMA_WHITELIST, "nopk")
                .build());
        assertConnectorIsRunning();

        final int expectedRecordsCount = 1 + 1 + 1;

        TestConsumer consumer = testConsumer(expectedRecordsCount, "nopk");
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
        final Map<String, List<SourceRecord>> recordsByTopic = recordsByTopic(expectedRecordsCount, consumer);
        Assertions.assertThat(recordsByTopic.get("test_server.nopk.t1").get(0).keySchema().field("pk")).isNotNull();
        Assertions.assertThat(recordsByTopic.get("test_server.nopk.t1").get(0).keySchema().fields()).hasSize(1);
        Assertions.assertThat(recordsByTopic.get("test_server.nopk.t2").get(0).keySchema().field("pk")).isNotNull();
        Assertions.assertThat(recordsByTopic.get("test_server.nopk.t2").get(0).keySchema().fields()).hasSize(1);
        Assertions.assertThat(recordsByTopic.get("test_server.nopk.t3").get(0).keySchema()).isNull();
    }

    @Test
    public void shouldProcessFromStreaming() throws Exception {
        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "nopk")
                .build());
        assertConnectorIsRunning();
        waitForStreamingToStart();

        TestHelper.execute(STATEMENTS);

        final int expectedRecordsCount = 1 + 1 + 1;

        TestConsumer consumer = testConsumer(expectedRecordsCount, "nopk");
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
        final Map<String, List<SourceRecord>> recordsByTopic = recordsByTopic(expectedRecordsCount, consumer);
        Assertions.assertThat(recordsByTopic.get("test_server.nopk.t1").get(0).keySchema().field("pk")).isNotNull();
        Assertions.assertThat(recordsByTopic.get("test_server.nopk.t1").get(0).keySchema().fields()).hasSize(1);
        Assertions.assertThat(recordsByTopic.get("test_server.nopk.t2").get(0).keySchema().field("pk")).isNotNull();
        Assertions.assertThat(recordsByTopic.get("test_server.nopk.t2").get(0).keySchema().fields()).hasSize(1);
        Assertions.assertThat(recordsByTopic.get("test_server.nopk.t3").get(0).keySchema()).isNull();
    }

    @Test
    public void shouldProcessFromStreamingOld() throws Exception {
        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with(PostgresConnectorConfig.SCHEMA_WHITELIST, "nopk")
                .build());
        assertConnectorIsRunning();
        waitForStreamingToStart();

        TestHelper.execute(STATEMENTS);

        final int expectedRecordsCount = 1 + 1 + 1;

        TestConsumer consumer = testConsumer(expectedRecordsCount, "nopk");
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
        final Map<String, List<SourceRecord>> recordsByTopic = recordsByTopic(expectedRecordsCount, consumer);
        Assertions.assertThat(recordsByTopic.get("test_server.nopk.t1").get(0).keySchema().field("pk")).isNotNull();
        Assertions.assertThat(recordsByTopic.get("test_server.nopk.t1").get(0).keySchema().fields()).hasSize(1);
        Assertions.assertThat(recordsByTopic.get("test_server.nopk.t2").get(0).keySchema().field("pk")).isNotNull();
        Assertions.assertThat(recordsByTopic.get("test_server.nopk.t2").get(0).keySchema().fields()).hasSize(1);
        Assertions.assertThat(recordsByTopic.get("test_server.nopk.t3").get(0).keySchema()).isNull();
    }
}
