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
    public void shouldSkipEventsWithNoChangeInIncludedColumnsWhenSkipEnabled() throws Exception {

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

        start(YugabyteDBConnector.class, config);
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        TestHelper.execute("INSERT INTO updates_test.debezium_test (id,white,black) VALUES (1,1,1);");
        TestHelper.execute("UPDATE updates_test.debezium_test SET black=2 where id = 1;");
        TestHelper.execute("UPDATE updates_test.debezium_test SET white=2 where id = 1;");
        TestHelper.execute("UPDATE updates_test.debezium_test SET white=3, black=3 where id = 1;");

        final SourceRecords records = consumeRecordsByTopic(3);
        /*
         * Total Events
         * 1,1,1 (I)
         * 1,1,2 (U) (Skipped)
         * 1,2,2 (U)
         * 1,3,3 (U)
         */
        final List<SourceRecord> recordsForTopic = records.recordsForTopic(topicName("updates_test.debezium_test"));
        assertThat(recordsForTopic).hasSize(3);
        Struct secondMessage = ((Struct) recordsForTopic.get(1).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(secondMessage.getStruct("white").getInt32("value")).isEqualTo(2);
        Struct thirdMessage = ((Struct) recordsForTopic.get(2).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(thirdMessage.getStruct("white").getInt32("value")).isEqualTo(3);
    }

    @Test
    @FixFor("DBZ-2979")
    public void shouldSkipEventsWithNoChangeInIncludedColumnsWhenSkipEnabledWithExcludeConfig() throws Exception {

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

        start(YugabyteDBConnector.class, config);
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        TestHelper.execute("INSERT INTO updates_test.debezium_test (id,white,black) VALUES (1,1,1);");
        TestHelper.execute("UPDATE updates_test.debezium_test SET black=2 where id = 1;");
        TestHelper.execute("UPDATE updates_test.debezium_test SET white=2 where id = 1;");
        TestHelper.execute("UPDATE updates_test.debezium_test SET white=3, black=3 where id = 1;");

        final SourceRecords records = consumeRecordsByTopic(3);
        /*
         * Total Events
         * 1,1,1 (I)
         * 1,1,2 (U) (Skipped)
         * 1,2,2 (U)
         * 1,3,3 (U)
         */
        final List<SourceRecord> recordsForTopic = records.recordsForTopic(topicName("updates_test.debezium_test"));
        assertThat(recordsForTopic).hasSize(3);
        Struct secondMessage = ((Struct) recordsForTopic.get(1).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(secondMessage.getStruct("white").getInt32("value")).isEqualTo(2);
        Struct thirdMessage = ((Struct) recordsForTopic.get(2).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(thirdMessage.getStruct("white").getInt32("value")).isEqualTo(3);
    }

    @Test
    @FixFor("DBZ-2979")
    public void shouldNotSkipEventsWithNoChangeInIncludedColumnsWhenSkipEnabledButTableReplicaIdentityNotFull() throws Exception {

        TestHelper.execute("DROP SCHEMA IF EXISTS updates_test CASCADE;",
                "CREATE SCHEMA updates_test;",
                "CREATE TABLE updates_test.debezium_test (id int4 NOT NULL, white int, black int, PRIMARY KEY(id));");

        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.COLUMN_INCLUDE_LIST, "updates_test.debezium_test.id, updates_test.debezium_test.white")
                .with(PostgresConnectorConfig.SKIP_MESSAGES_WITHOUT_CHANGE, true)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER)
                .build();

        start(YugabyteDBConnector.class, config);
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        TestHelper.execute("INSERT INTO updates_test.debezium_test (id,white,black) VALUES (1,1,1);");
        TestHelper.execute("UPDATE updates_test.debezium_test SET black=2 where id = 1;");
        TestHelper.execute("UPDATE updates_test.debezium_test SET white=2 where id = 1;");
        TestHelper.execute("UPDATE updates_test.debezium_test SET white=3, black=3 where id = 1;");
        final SourceRecords records = consumeRecordsByTopic(4);
        /*
         * Total Events
         * 1,1,1 (I)
         * 1,1,2 (U)
         * 1,2,2 (U)
         * 1,3,3 (U)
         */
        final List<SourceRecord> recordsForTopic = records.recordsForTopic(topicName("updates_test.debezium_test"));
        assertThat(recordsForTopic).hasSize(4);
        Struct secondMessage = ((Struct) recordsForTopic.get(1).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(secondMessage.getStruct("white").getInt32("value")).isEqualTo(1);
        Struct thirdMessage = ((Struct) recordsForTopic.get(2).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(thirdMessage.getStruct("white").getInt32("value")).isEqualTo(2);
        Struct forthMessage = ((Struct) recordsForTopic.get(3).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(forthMessage.getStruct("white").getInt32("value")).isEqualTo(3);
    }

    @Test
    @FixFor("DBZ-2979")
    public void shouldNotSkipEventsWithNoChangeInIncludedColumnsWhenSkipDisabled() throws Exception {

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

        start(YugabyteDBConnector.class, config);
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        TestHelper.execute("INSERT INTO updates_test.debezium_test (id,white,black) VALUES (1,1,1);");
        TestHelper.execute("UPDATE updates_test.debezium_test SET black=2 where id = 1;");
        TestHelper.execute("UPDATE updates_test.debezium_test SET white=2 where id = 1;");
        TestHelper.execute("UPDATE updates_test.debezium_test SET white=3, black=3 where id = 1;");
        /*
         * Total Events
         * 1,1,1 (I)
         * 1,1,2 (U)
         * 1,2,2 (U)
         * 1,3,3 (U)
         */
        final SourceRecords records = consumeRecordsByTopic(4);
        final List<SourceRecord> recordsForTopic = records.recordsForTopic(topicName("updates_test.debezium_test"));

        assertThat(recordsForTopic).hasSize(4);
        Struct secondMessage = ((Struct) recordsForTopic.get(1).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(secondMessage.getStruct("white").getInt32("value")).isEqualTo(1);
        Struct thirdMessage = ((Struct) recordsForTopic.get(2).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(thirdMessage.getStruct("white").getInt32("value")).isEqualTo(2);
        Struct forthMessage = ((Struct) recordsForTopic.get(3).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(forthMessage.getStruct("white").getInt32("value")).isEqualTo(3);
    }

}
