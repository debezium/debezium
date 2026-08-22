/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.transforms.StringifyFields;

/**
 * End-to-end tests of the {@link io.debezium.transforms.StringifyFields} transformation against a
 * real PostgreSQL source, exercising the transform in envelope mode (no prior flattening) so that it
 * reaches into the change-event envelope with dot-notation field paths.
 */
public class PostgresStringifyFieldsIT extends AbstractAsyncEngineConnectorTest {

    private PostgresConnection connection;

    @BeforeEach
    void before() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.execute("DROP SCHEMA IF EXISTS s1 CASCADE;CREATE SCHEMA s1;");
        connection = TestHelper.create();
        connection.setAutoCommit(false);
    }

    @AfterEach
    void after() throws Exception {
        stopConnector();
        assertNoRecordsToConsume();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
        if (connection != null) {
            connection.close();
        }
    }

    private String topicName() {
        return TestHelper.TEST_SERVER + ".s1.dbz_sf";
    }

    private Struct after(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
    }

    private Configuration.Builder baseConfig() {
        return TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.PLUGIN_NAME, "pgoutput")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1\\.dbz_sf");
    }

    @Test
    public void shouldStringifyNestedFieldViaDotNotation() throws Exception {
        TestHelper.execute("CREATE TABLE s1.dbz_sf (id int primary key, data int, name text);");
        TestHelper.execute("ALTER TABLE s1.dbz_sf REPLICA IDENTITY FULL;");
        TestHelper.execute("INSERT INTO s1.dbz_sf (id,data,name) values (1,101,'a');");

        // Envelope mode: target a nested scalar. Because 'before' and 'after' share the same named
        // record schema, the path is applied under both so the retyped schemas stay consistent; 'name'
        // is already a string and stays as-is.
        start(PostgresConnector.class, baseConfig()
                .with("transforms", "stringify")
                .with("transforms.stringify.type", StringifyFields.class.getName())
                .with("transforms.stringify.fields", "after.data,after.name,before.data,before.name")
                .build());
        assertConnectorIsRunning();
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        TestHelper.execute("INSERT INTO s1.dbz_sf (id,data,name) values (2,202,'b');");
        TestHelper.execute("UPDATE s1.dbz_sf set data = 303 WHERE id = 2;");

        final SourceRecords records = consumeRecordsByTopic(3);
        final List<SourceRecord> tableRecords = records.recordsForTopic(topicName());
        assertThat(tableRecords).hasSize(3);

        // Read (snapshot) of id=1.
        VerifyRecord.isValidRead(tableRecords.get(0), "id", 1);
        Struct read = after(tableRecords.get(0));
        assertThat(read.schema().field("data").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(read.schema().field("id").schema().type()).isEqualTo(Schema.Type.INT32);
        assertThat((String) read.get("data")).isEqualTo("101");
        assertThat(read.get("id")).isEqualTo(1);
        assertThat(read.get("name")).isEqualTo("a"); // already a string, left untouched

        // Insert of id=2.
        VerifyRecord.isValidInsert(tableRecords.get(1), "id", 2);
        assertThat((String) after(tableRecords.get(1)).get("data")).isEqualTo("202");

        // Update of id=2.
        VerifyRecord.isValidUpdate(tableRecords.get(2), "id", 2);
        assertThat((String) after(tableRecords.get(2)).get("data")).isEqualTo("303");
    }

    @Test
    public void shouldStringifyWholeAfterStructForVariantTargetColumn() throws Exception {
        TestHelper.execute("CREATE TABLE s1.dbz_sf (id int primary key, data int, name text);");
        TestHelper.execute("ALTER TABLE s1.dbz_sf REPLICA IDENTITY FULL;");
        TestHelper.execute("INSERT INTO s1.dbz_sf (id,data,name) values (1,101,'a');");

        // Top-level target: the whole 'after' struct becomes a single JSON string, the VARIANT use case.
        start(PostgresConnector.class, baseConfig()
                .with("transforms", "stringify")
                .with("transforms.stringify.type", StringifyFields.class.getName())
                .with("transforms.stringify.fields", "after")
                .build());
        assertConnectorIsRunning();
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        final SourceRecords records = consumeRecordsByTopic(1);
        final SourceRecord record = records.recordsForTopic(topicName()).get(0);

        VerifyRecord.isValidRead(record, "id", 1);
        Struct envelope = (Struct) record.value();
        assertThat(envelope.schema().field("after").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat((String) envelope.get("after"))
                .contains("\"id\":1")
                .contains("\"data\":101")
                .contains("\"name\":\"a\"");
    }
}
