/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.junit.SkipTestRule;
import io.debezium.junit.SkipWhenDatabaseVersion;
import org.apache.kafka.connect.data.Struct;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static io.debezium.junit.EqualityCheck.LESS_THAN;

/**
 * Integration test to verify tsvector data type.
 *
 * @author Pranav Tiwari
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 16, reason = "tsvector is tested only with PostgreSQL 16+")
public class TsVectorDataTypeIT extends AbstractRecordsProducerTest {

    @Rule
    public final SkipTestRule skipTest = new SkipTestRule();

    @Before
    public void before() throws Exception {

        System.setProperty(PostgresConnectorConfig.PLUGIN_NAME.name(), PostgresConnectorConfig.LogicalDecoder.PGOUTPUT.name());

        try (PostgresConnection conn = TestHelper.create()) {
            conn.dropReplicationSlot(ReplicationConnection.Builder.DEFAULT_SLOT_NAME);
        }
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("init_tsvector.ddl");
        TestHelper.execute(
                "DROP TABLE IF EXISTS tsvector.text_search_test;",
                "CREATE TABLE tsvector.text_search_test (id SERIAL PRIMARY KEY, title TEXT, content TEXT, search_vector tsvector);"
        );
        TestHelper.execute("INSERT INTO tsvector.text_search_test (title, content, search_vector) VALUES ('TC1 - Direct TSV', 'This is a test for direct tsvector insert', to_tsvector('english', 'This is a test for direct tsvector insert'));");
        initializeConnectorTestFramework();
    }

    @Test
    public void shouldSnapshotAndStreamTsvectorData() throws Exception {

        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(PostgresConnectorConfig.PLUGIN_NAME, PostgresConnectorConfig.LogicalDecoder.PGOUTPUT)
                .build());
        assertConnectorIsRunning();

        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        TestHelper.execute("INSERT INTO tsvector.text_search_test (title, content, search_vector) VALUES ('TC1 - Direct TSV', 'This is a test for direct tsvector insert second case', to_tsvector('english', 'This is a test for direct tsvector insert second case'));");

        var actualRecords = consumeRecordsByTopic(2);
        var recs = actualRecords.recordsForTopic("test_server.tsvector.text_search_test");
        Assertions.assertThat(recs).hasSize(2);

        var rec = ((Struct) recs.get(0).value());
        Assertions.assertThat(rec.schema().field("after").schema().field("search_vector").schema().name()).isEqualTo("io.debezium.data.Tsvector");
        Assertions.assertThat(rec.getStruct("after").getString("search_vector")).isEqualTo("'direct':6 'insert':8 'test':4 'tsvector':7");

        rec = ((Struct) recs.get(1).value());
        Assertions.assertThat(rec.schema().field("after").schema().field("search_vector").schema().name()).isEqualTo("io.debezium.data.Tsvector");
        Assertions.assertThat(rec.getStruct("after").getString("search_vector")).isEqualTo("'case':10 'direct':6 'insert':8 'second':9 'test':4 'tsvector':7");
    }

    @Test
    public void shouldStreamTsvectorDataAfterUpdate() throws Exception {
        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .build());
        assertConnectorIsRunning();

        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        TestHelper.execute("INSERT INTO tsvector.text_search_test (title, content) VALUES ('TC2 - Post Update TSV', 'This is a test for updating tsvector after insert');");
        TestHelper.execute("UPDATE tsvector.text_search_test SET search_vector = to_tsvector('english', content) WHERE title = 'TC2 - Post Update TSV';");

        var actualRecords = consumeRecordsByTopic(2);
        var recs = actualRecords.recordsForTopic("test_server.tsvector.text_search_test");
        Assertions.assertThat(recs).hasSize(2);

        var rec = ((Struct) recs.get(1).value());
        Assertions.assertThat(rec.schema().field("after").schema().field("search_vector").schema().name()).isEqualTo("io.debezium.data.Tsvector");
        Assertions.assertThat(rec.getStruct("after").getString("search_vector")).isEqualTo("'insert':9 'test':4 'tsvector':7 'updat':6");
    }

    @Test
    public void shouldStreamTsvectorDataWithTriggerBasedUpdate() throws Exception {
        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .build());
        assertConnectorIsRunning();

        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        TestHelper.execute("DROP TABLE IF EXISTS tsvector.text_search_test;");
        TestHelper.execute("CREATE TABLE tsvector.text_search_test ( id SERIAL PRIMARY KEY, title TEXT, content TEXT,  search_vector tsvector);");
        TestHelper.execute("CREATE OR REPLACE FUNCTION tsvector.trg_update_search_vector() RETURNS trigger AS $$ BEGIN NEW.search_vector := to_tsvector('english', NEW.content); RETURN NEW; END; $$ LANGUAGE plpgsql;");
        TestHelper.execute("CREATE TRIGGER trg_set_tsvector BEFORE INSERT OR UPDATE ON tsvector.text_search_test FOR EACH ROW EXECUTE FUNCTION tsvector.trg_update_search_vector();");
        TestHelper.execute("INSERT INTO tsvector.text_search_test (title, content) VALUES ('TC4 - Trigger TSV', 'This is a test for tsvector populated via trigger');");

        var actualRecords = consumeRecordsByTopic(1);
        var recs = actualRecords.recordsForTopic("test_server.tsvector.text_search_test");
        Assertions.assertThat(recs).hasSize(1);

        var rec = ((Struct) recs.get(0).value());
        Assertions.assertThat(rec.schema().field("after").schema().field("search_vector").schema().name()).isEqualTo("io.debezium.data.Tsvector");
        Assertions.assertThat(rec.getStruct("after").getString("search_vector")).isEqualTo("'popul':7 'test':4 'trigger':9 'tsvector':6 'via':8");
    }
}
