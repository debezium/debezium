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

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;

/**
 * Integration test for {@link io.debezium.connector.postgresql.PostgresConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE}
 *
 * @author Jiri Pechanec (jpechane@redhat.com)
 */
public class SnapshotWithOverridesProducerIT extends AbstractRecordsProducerTest {

    private static final String STATEMENTS = "CREATE SCHEMA over;" +
            "CREATE TABLE over.t1 (pk INT, PRIMARY KEY(pk));" +
            "CREATE TABLE over.t2 (pk INT, PRIMARY KEY(pk));" +
            "INSERT INTO over.t1 VALUES (1);" +
            "INSERT INTO over.t1 VALUES (2);" +
            "INSERT INTO over.t1 VALUES (3);" +
            "INSERT INTO over.t1 VALUES (101);" +
            "INSERT INTO over.t1 VALUES (102);" +
            "INSERT INTO over.t1 VALUES (103);" +
            "INSERT INTO over.t2 VALUES (1);" +
            "INSERT INTO over.t2 VALUES (2);" +
            "INSERT INTO over.t2 VALUES (3);" +
            "INSERT INTO over.t2 VALUES (101);" +
            "INSERT INTO over.t2 VALUES (102);" +
            "INSERT INTO over.t2 VALUES (103);";

    @Before
    public void before() throws SQLException {
        TestHelper.dropAllSchemas();
    }

    @Test
    public void shouldUseOverriddenSelectStatementDuringSnapshotting() throws Exception {
        TestHelper.execute(STATEMENTS);

        buildProducer(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE, "over.t1")
                .with(PostgresConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE.name() + ".over.t1", "SELECT * FROM over.t1 WHERE pk > 100"));

        final int expectedRecordsCount = 3 + 6;

        TestConsumer consumer = testConsumer(expectedRecordsCount, "over");
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        final Map<String, List<SourceRecord>> recordsByTopic = recordsByTopic(expectedRecordsCount, consumer);
        Assertions.assertThat(recordsByTopic.get("test_server.over.t1")).hasSize(3);
        Assertions.assertThat(recordsByTopic.get("test_server.over.t2")).hasSize(6);
    }

    @Test
    public void shouldUseMultipleOverriddenSelectStatementsDuringSnapshotting() throws Exception {
        TestHelper.execute(STATEMENTS);

        buildProducer(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE, "over.t1,over.t2")
                .with(PostgresConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE.name() + ".over.t1", "SELECT * FROM over.t1 WHERE pk > 101")
                .with(PostgresConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE.name() + ".over.t2", "SELECT * FROM over.t2 WHERE pk > 100"));

        final int expectedRecordsCount = 2 + 3;

        TestConsumer consumer = testConsumer(expectedRecordsCount, "over");
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        final Map<String, List<SourceRecord>> recordsByTopic = recordsByTopic(expectedRecordsCount, consumer);
        Assertions.assertThat(recordsByTopic.get("test_server.over.t1")).hasSize(2);
        Assertions.assertThat(recordsByTopic.get("test_server.over.t2")).hasSize(3);
    }

    private void buildProducer(Configuration.Builder config) {
        start(PostgresConnector.class, config
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY)
                .build());
        assertConnectorIsRunning();
    }
}
