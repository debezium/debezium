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
 * Integration test to verify postgres ltree types defined in public schema.
 *
 * @author Harvey Yue
 */
public class PostgresLtreeIT extends AbstractConnectorTest {
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
    @FixFor("DBZ-6524")
    public void shouldReceiveChangesForInsertsIncludingLtreeColumn() throws Exception {
        createTable();

        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER)
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, "true")
                .build();
        start(PostgresConnector.class, config);
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        // insert 3 records for testing
        insertThreeRecords();

        final SourceRecords records = consumeRecordsByTopic(3);
        final List<SourceRecord> recordsForTopic = records.recordsForTopic(topicName("post_ltree.dbz_6524"));

        assertRecords(recordsForTopic);
    }

    @Test
    @FixFor("DBZ-6524")
    public void shouldReceiveChangesForAddingLtreeColumn() throws Exception {
        TestHelper.execute(
                "DROP SCHEMA IF EXISTS post_ltree CASCADE;",
                "CREATE SCHEMA post_ltree;",
                "CREATE TABLE post_ltree.dbz_6524(id int4 NOT NULL, CONSTRAINT dbz_6524_pkey PRIMARY KEY (id));");
        TestHelper.execute("insert into post_ltree.dbz_6524(id) values(1),(2),(3)");

        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER)
                .build();
        start(PostgresConnector.class, config);
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        // adding ltree column and update values
        TestHelper.execute("ALTER TABLE post_ltree.dbz_6524 ADD COLUMN l ltree;");
        TestHelper.execute(
                "update post_ltree.dbz_6524 set l='Top' where id=1;",
                "update post_ltree.dbz_6524 set l='Top.Science' where id=2;",
                "update post_ltree.dbz_6524 set l='Top.Science.Astrononmy4' where id=3;");

        final SourceRecords records = consumeRecordsByTopic(3);
        final List<SourceRecord> recordsForTopic = records.recordsForTopic(topicName("post_ltree.dbz_6524"));

        assertRecords(recordsForTopic);
    }

    private void createTable() {
        TestHelper.execute(
                "DROP SCHEMA IF EXISTS post_ltree CASCADE;",
                "CREATE SCHEMA post_ltree;",
                "CREATE TABLE post_ltree.dbz_6524(id int4 NOT NULL, l ltree, CONSTRAINT dbz_6524_pkey PRIMARY KEY (id));");
    }

    private void insertThreeRecords() {
        TestHelper.execute("insert into post_ltree.dbz_6524(id, l) values(1,'Top'),(2,'Top.Science'),(3,'Top.Science.Astrononmy4')");
    }

    private void assertRecords(List<SourceRecord> sourceRecords) {
        assertThat(sourceRecords).hasSize(3);

        Struct after = ((Struct) sourceRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("l")).isEqualTo("Top");
        after = ((Struct) sourceRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("l")).isEqualTo("Top.Science");
        after = ((Struct) sourceRecords.get(2).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("l")).isEqualTo("Top.Science.Astrononmy4");
    }
}
