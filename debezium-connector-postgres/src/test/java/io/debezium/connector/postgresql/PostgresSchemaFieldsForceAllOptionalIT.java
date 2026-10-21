/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.TestHelper.topicName;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;

/**
 * Integration tests for the {@code schema.fields.force.all.optional} configuration option.
 *
 * @author Paolo Cristofanelli
 */
public class PostgresSchemaFieldsForceAllOptionalIT extends AbstractAsyncEngineConnectorTest {

    private static final String TABLE = "force_optional.test_table";
    private static final List<String> NOT_NULL_COLUMNS = List.of("id", "required_text", "required_int");

    @BeforeEach
    void before() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.execute(
                "CREATE SCHEMA force_optional;",
                "CREATE TABLE force_optional.test_table (id int4 NOT NULL, required_text text NOT NULL, "
                        + "required_int int4 NOT NULL DEFAULT 7, optional_text text, PRIMARY KEY (id));",
                "INSERT INTO force_optional.test_table (id, required_text, required_int) VALUES (1, 'snapshot', 1);");
        initializeConnectorTestFramework();
    }

    @AfterEach
    void after() {
        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
    }

    @Test
    @FixFor("debezium/dbz#142")
    public void shouldMarkAllKeyAndValueFieldsOptionalForSnapshotAndStreamingWhenEnabled() throws Exception {
        start(PostgresConnector.class, config(true));

        // snapshot
        SourceRecord snapshotRecord = consumeSingleRecord();
        assertNotNullColumnSchemasOptional(snapshotRecord, true);
        assertAfterValues(snapshotRecord, 1, "snapshot", 1);

        // streaming; required_int is omitted so the column default applies
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
        TestHelper.execute("INSERT INTO force_optional.test_table (id, required_text) VALUES (2, 'streaming');");
        SourceRecord streamingRecord = consumeSingleRecord();
        assertNotNullColumnSchemasOptional(streamingRecord, true);
        assertAfterValues(streamingRecord, 2, "streaming", 7);
    }

    @Test
    @FixFor("debezium/dbz#142")
    public void shouldKeepColumnNullabilityForSnapshotAndStreamingWhenDisabled() throws Exception {
        start(PostgresConnector.class, config(false));

        SourceRecord snapshotRecord = consumeSingleRecord();
        assertNotNullColumnSchemasOptional(snapshotRecord, false);
        assertAfterValues(snapshotRecord, 1, "snapshot", 1);

        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
        TestHelper.execute("INSERT INTO force_optional.test_table (id, required_text) VALUES (2, 'streaming');");
        SourceRecord streamingRecord = consumeSingleRecord();
        assertNotNullColumnSchemasOptional(streamingRecord, false);
        assertAfterValues(streamingRecord, 2, "streaming", 7);
    }

    private Configuration config(boolean forceAllOptional) {
        return TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL)
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, TABLE)
                .with(PostgresConnectorConfig.SCHEMA_FIELDS_FORCE_ALL_OPTIONAL, forceAllOptional)
                .build();
    }

    private SourceRecord consumeSingleRecord() throws InterruptedException {
        List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic(topicName(TABLE));
        assertThat(records).hasSize(1);
        return records.get(0);
    }

    private static void assertNotNullColumnSchemasOptional(SourceRecord record, boolean expectedOptional) {
        Schema valueSchema = record.valueSchema().field(Envelope.FieldName.AFTER).schema();
        for (String column : NOT_NULL_COLUMNS) {
            assertThat(valueSchema.field(column).schema().isOptional())
                    .as("value field %s optional", column)
                    .isEqualTo(expectedOptional);
        }
        // a nullable column is optional either way
        assertThat(valueSchema.field("optional_text").schema().isOptional()).isTrue();

        assertThat(record.keySchema().field("id").schema().isOptional())
                .as("key field id optional")
                .isEqualTo(expectedOptional);
    }

    private static void assertAfterValues(SourceRecord record, int id, String requiredText, int requiredInt) {
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("id")).isEqualTo(id);
        assertThat(after.get("required_text")).isEqualTo(requiredText);
        assertThat(after.get("required_int")).isEqualTo(requiredInt);
        assertThat(after.get("optional_text")).isNull();
        assertThat(((Struct) record.key()).get("id")).isEqualTo(id);
    }
}
