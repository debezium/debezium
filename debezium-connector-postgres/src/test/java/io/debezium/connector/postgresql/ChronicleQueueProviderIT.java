/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.TestHelper.PK_FIELD;
import static io.debezium.connector.postgresql.TestHelper.topicName;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;

/**
 * Integration tests for {@link io.debezium.connector.base.QueueProvider} implementations
 * backed by Chronicle Queue, validating streaming CDC with non-default queue providers.
 *
 * @author Chris Cranford
 */
public class ChronicleQueueProviderIT extends AbstractAsyncEngineConnectorTest {

    private static final String SETUP_STMT = "DROP SCHEMA IF EXISTS cq_test CASCADE;"
            + "CREATE SCHEMA cq_test;"
            + "CREATE TABLE cq_test.events (pk SERIAL, data VARCHAR(50), PRIMARY KEY(pk));";

    private static final String TOPIC = "cq_test.events";

    @BeforeAll
    static void beforeClass() throws SQLException {
        TestHelper.dropAllSchemas();
    }

    @BeforeEach
    void before() {
        initializeConnectorTestFramework();
    }

    @AfterEach
    void after() {
        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
    }

    @Test
    @FixFor("dbz#1938")
    public void shouldStreamWithChronicleQueueProvider() throws Exception {
        TestHelper.execute(SETUP_STMT);

        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "cq_test")
                .with(CommonConnectorConfig.QUEUE_PROVIDER_TYPE, "chronicle")
                .build();

        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        TestHelper.execute(
                "INSERT INTO cq_test.events (data) VALUES ('insert-test');",
                "UPDATE cq_test.events SET data = 'update-test' WHERE pk = 1;",
                "DELETE FROM cq_test.events WHERE pk = 1;");

        // 1 insert + 1 update + 1 delete + 1 tombstone = 4
        SourceRecords records = consumeRecordsByTopic(4);
        List<SourceRecord> events = records.recordsForTopic(topicName(TOPIC));
        assertThat(events).hasSize(4);

        VerifyRecord.isValidInsert(events.get(0), PK_FIELD, 1);
        VerifyRecord.isValidUpdate(events.get(1), PK_FIELD, 1);
        VerifyRecord.isValidDelete(events.get(2), PK_FIELD, 1);
        VerifyRecord.isValidTombstone(events.get(3), PK_FIELD, 1);
    }

    @Test
    @FixFor("dbz#1938")
    public void shouldStreamWithHybridChronicleQueueProvider() throws Exception {
        TestHelper.execute(SETUP_STMT);

        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "cq_test")
                .with(CommonConnectorConfig.QUEUE_PROVIDER_TYPE, "hybrid_chronicle")
                .with(CommonConnectorConfig.MAX_QUEUE_SIZE, 10)
                .with(CommonConnectorConfig.MAX_BATCH_SIZE, 5)
                .build();

        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        TestHelper.execute(
                "INSERT INTO cq_test.events (data) SELECT 'row-' || g FROM generate_series(1, 100) g;",
                "UPDATE cq_test.events SET data = 'updated-' || pk WHERE pk BETWEEN 1 AND 100;",
                "DELETE FROM cq_test.events WHERE pk BETWEEN 1 AND 100;");

        // 100 inserts + 100 updates + 100 deletes + 100 tombstones = 400
        SourceRecords records = consumeRecordsByTopic(400);
        List<SourceRecord> events = records.recordsForTopic(topicName(TOPIC));
        assertThat(events).hasSize(400);

        for (int i = 0; i < 100; i++) {
            VerifyRecord.isValidInsert(events.get(i), PK_FIELD, i + 1);
        }
        for (int i = 0; i < 100; i++) {
            VerifyRecord.isValidUpdate(events.get(100 + i), PK_FIELD, i + 1);
        }
        for (int i = 0; i < 100; i++) {
            VerifyRecord.isValidDelete(events.get(200 + (i * 2)), PK_FIELD, i + 1);
            VerifyRecord.isValidTombstone(events.get(200 + (i * 2) + 1), PK_FIELD, i + 1);
        }
    }
}