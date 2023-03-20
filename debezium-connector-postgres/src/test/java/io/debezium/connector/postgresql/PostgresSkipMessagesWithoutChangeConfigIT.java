/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.TestHelper.topicName;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;

/**
 * Integration Tests for config skip.messages.without.change
 *
 * @author Ronak Jain
 */
public class PostgresSkipMessagesWithoutChangeConfigIT extends AbstractConnectorTest {

    @Before
    public void before() throws Exception {
        initializeConnectorTestFramework();
        TestHelper.dropAllSchemas();
    }

    @After
    public void after() {
        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
    }

    @Test
    @FixFor("DBZ-2979")
    public void shouldSkipEventsWithNoChangeInWhitelistedColumnsWhenSkipEnabled() throws Exception {

        TestHelper.execute(
                "DROP SCHEMA IF EXISTS updates_test CASCADE;",
                "CREATE SCHEMA updates_test;",
                "CREATE TABLE updates_test.debezium_test (id int4 NOT NULL, white int, black int, PRIMARY KEY(id));",
                "ALTER TABLE updates_test.debezium_test REPLICA IDENTITY FULL;");

        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.COLUMN_INCLUDE_LIST, "updates_test.debezium_test.id, updates_test.debezium_test.white")
                .with(PostgresConnectorConfig.SKIP_MESSAGES_WITHOUT_CHANGE, true)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER)
                .build();

        start(PostgresConnector.class, config);
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        TestHelper.execute("INSERT INTO updates_test.debezium_test (id,white,black) VALUES (1,1,1);");
        TestHelper.execute("UPDATE updates_test.debezium_test SET black=2 where id = 1;");
        TestHelper.execute("UPDATE updates_test.debezium_test SET white=2 where id = 1;");

        final SourceRecords records = consumeRecordsByTopic(10);
        final List<SourceRecord> recordsForTopic = records.recordsForTopic(topicName("updates_test.debezium_test"));
        assertThat(recordsForTopic).hasSize(2);
        Struct after = ((Struct) recordsForTopic.get(1).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("white")).isEqualTo(2);
    }

    @Test
    @FixFor("DBZ-2979")
    public void shouldSkipEventsWithNoChangeInWhitelistedColumnsWhenSkipEnabledWithExcludeConfig() throws Exception {

        TestHelper.execute(
                "DROP SCHEMA IF EXISTS updates_test CASCADE;",
                "CREATE SCHEMA updates_test;",
                "CREATE TABLE updates_test.debezium_test (id int4 NOT NULL, white int, black int, PRIMARY KEY(id));",
                "ALTER TABLE updates_test.debezium_test REPLICA IDENTITY FULL;");

        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.COLUMN_EXCLUDE_LIST, "updates_test.debezium_test.black")
                .with(PostgresConnectorConfig.SKIP_MESSAGES_WITHOUT_CHANGE, true)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER)
                .build();

        start(PostgresConnector.class, config);
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        TestHelper.execute("INSERT INTO updates_test.debezium_test (id,white,black) VALUES (1,1,1);");
        TestHelper.execute("UPDATE updates_test.debezium_test SET black=2 where id = 1;");
        TestHelper.execute("UPDATE updates_test.debezium_test SET white=2 where id = 1;");

        final SourceRecords records = consumeRecordsByTopic(3);
        final List<SourceRecord> recordsForTopic = records.recordsForTopic(topicName("updates_test.debezium_test"));
        assertThat(recordsForTopic).hasSize(2);
        Struct after = ((Struct) recordsForTopic.get(1).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("white")).isEqualTo(2);
    }

    @Test
    @FixFor("DBZ-2979")
    public void shouldNotSkipEventsWithNoChangeInWhitelistedColumnsWhenSkipEnabledButTableReplicaIdentityNotFull() throws Exception {

        TestHelper.execute("DROP SCHEMA IF EXISTS updates_test CASCADE;",
                "CREATE SCHEMA updates_test;",
                "CREATE TABLE updates_test.debezium_test (id int4 NOT NULL, white int, black int, PRIMARY KEY(id));");

        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.COLUMN_INCLUDE_LIST, "updates_test.debezium_test.id, updates_test.debezium_test.white")
                .with(PostgresConnectorConfig.SKIP_MESSAGES_WITHOUT_CHANGE, true)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER)
                .build();

        start(PostgresConnector.class, config);
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        TestHelper.execute("INSERT INTO updates_test.debezium_test (id,white,black) VALUES (1,1,1);");
        TestHelper.execute("UPDATE updates_test.debezium_test SET black=2 where id = 1;");
        TestHelper.execute("UPDATE updates_test.debezium_test SET white=2 where id = 1;");
        final SourceRecords records = consumeRecordsByTopic(3);
        final List<SourceRecord> recordsForTopic = records.recordsForTopic(topicName("updates_test.debezium_test"));
        assertThat(recordsForTopic).hasSize(3);
        Struct after = ((Struct) recordsForTopic.get(1).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("white")).isEqualTo(1);
    }

    @Test
    @FixFor("DBZ-2979")
    public void shouldNotSkipEventsWithNoChangeInWhitelistedColumnsWhenSkipDisabled() throws Exception {

        TestHelper.execute(
                "DROP SCHEMA IF EXISTS updates_test CASCADE;",
                "CREATE SCHEMA updates_test;",
                "CREATE TABLE updates_test.debezium_test (id int4 NOT NULL, white int, black int, PRIMARY KEY(id));",
                "ALTER TABLE updates_test.debezium_test REPLICA IDENTITY FULL;");

        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.COLUMN_INCLUDE_LIST, "updates_test.debezium_test.id, updates_test.debezium_test.white")
                .with(PostgresConnectorConfig.SKIP_MESSAGES_WITHOUT_CHANGE, false)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER)
                .build();

        start(PostgresConnector.class, config);
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        TestHelper.execute("INSERT INTO updates_test.debezium_test (id,white,black) VALUES (1,1,1);");
        TestHelper.execute("UPDATE updates_test.debezium_test SET black=2 where id = 1;");
        TestHelper.execute("UPDATE updates_test.debezium_test SET white=2 where id = 1;");
        final SourceRecords records = consumeRecordsByTopic(3);
        final List<SourceRecord> recordsForTopic = records.recordsForTopic(topicName("updates_test.debezium_test"));

        assertThat(recordsForTopic).hasSize(3);
        Struct after = ((Struct) recordsForTopic.get(1).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("white")).isEqualTo(1);
    }

}
