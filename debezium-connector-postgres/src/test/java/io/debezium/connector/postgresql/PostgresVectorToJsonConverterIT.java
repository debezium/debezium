/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.data.Envelope;
import io.debezium.data.Json;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.transforms.VectorToJsonConverter;

/**
 * Performs tests of the {@link io.debezium.transforms.VectorToJsonConverter} for PostgreSQL sources.
 *
 * @author Chris Cranford
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 15, reason = "PgVector is tested only with PostgreSQL 15+")
public class PostgresVectorToJsonConverterIT extends AbstractAsyncEngineConnectorTest {

    private PostgresConnection connection;

    @Before
    public void before() throws Exception {
        TestHelper.dropAllSchemas();

        TestHelper.execute("DROP SCHEMA IF EXISTS s1 CASCADE;CREATE SCHEMA s1;");
        TestHelper.execute("CREATE EXTENSION IF NOT EXISTS vector;");

        connection = TestHelper.create();
        connection.setAutoCommit(false);
    }

    @After
    public void after() throws Exception {
        stopConnector();
        assertNoRecordsToConsume();

        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();

        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-8571")
    public void shouldConvertFloatVectorToJson() throws Exception {
        TestHelper.execute("CREATE TABLE s1.dbz8571 (id int primary key, data halfvec(3));");
        TestHelper.execute("ALTER TABLE s1.dbz8571 REPLICA IDENTITY FULL;");

        TestHelper.execute("INSERT INTO s1.dbz8571 (id,data) values (1,'[101,102,103]');");

        start(PostgresConnector.class, getConfigurationWithTransform());
        assertConnectorIsRunning();

        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        TestHelper.execute("INSERT INTO s1.dbz8571 (id,data) values (2,'[1,2,3]');");
        TestHelper.execute("UPDATE s1.dbz8571 set data = '[5,7,9]' WHERE id = 2;");
        TestHelper.execute("DELETE FROM s1.dbz8571 WHERE id = 2");

        final SourceRecords records = consumeRecordsByTopic(5);
        final List<SourceRecord> tableRecords = records.recordsForTopic(topicName());
        assertThat(tableRecords).hasSize(5);

        assertRead(tableRecords.get(0), 1, "{ \"values\": [101.0, 102.0, 103.0] }");
        assertInsert(tableRecords.get(1), 2, "{ \"values\": [1.0, 2.0, 3.0] }");
        assertUpdate(tableRecords.get(2), 2, "{ \"values\": [1.0, 2.0, 3.0] }", "{ \"values\": [5.0, 7.0, 9.0] }");
        assertDelete(tableRecords.get(3), 2, "{ \"values\": [5.0, 7.0, 9.0] }");
        assertTombstone(tableRecords.get(4), 2);
    }

    @Test
    @FixFor("DBZ-8571")
    public void shouldConvertDoubleVectorToJson() throws Exception {
        TestHelper.execute("CREATE TABLE s1.dbz8571 (id int primary key, data vector(3));");
        TestHelper.execute("ALTER TABLE s1.dbz8571 REPLICA IDENTITY FULL;");

        TestHelper.execute("INSERT INTO s1.dbz8571 (id,data) values (1,'[101,102,103]');");

        start(PostgresConnector.class, getConfigurationWithTransform());
        assertConnectorIsRunning();

        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        TestHelper.execute("INSERT INTO s1.dbz8571 (id,data) values (2,'[1,2,3]');");
        TestHelper.execute("UPDATE s1.dbz8571 set data = '[5,7,9]' WHERE id = 2;");
        TestHelper.execute("DELETE FROM s1.dbz8571 WHERE id = 2");

        final SourceRecords records = consumeRecordsByTopic(5);
        final List<SourceRecord> tableRecords = records.recordsForTopic(topicName());
        assertThat(tableRecords).hasSize(5);

        assertRead(tableRecords.get(0), 1, "{ \"values\": [101.0, 102.0, 103.0] }");
        assertInsert(tableRecords.get(1), 2, "{ \"values\": [1.0, 2.0, 3.0] }");
        assertUpdate(tableRecords.get(2), 2, "{ \"values\": [1.0, 2.0, 3.0] }", "{ \"values\": [5.0, 7.0, 9.0] }");
        assertDelete(tableRecords.get(3), 2, "{ \"values\": [5.0, 7.0, 9.0] }");
        assertTombstone(tableRecords.get(4), 2);
    }

    @Test
    @FixFor("DBZ-8571")
    public void shouldConvertSparseVectorToJson() throws Exception {
        TestHelper.execute("CREATE TABLE s1.dbz8571 (id int primary key, data sparsevec(25));");
        TestHelper.execute("ALTER TABLE s1.dbz8571 REPLICA IDENTITY FULL;");
        TestHelper.execute("INSERT INTO s1.dbz8571 (id,data) values (1,'{1: 25, 2: 15, 10: 100}/25');");

        start(PostgresConnector.class, getConfigurationWithTransform());
        assertConnectorIsRunning();

        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        TestHelper.execute("INSERT INTO s1.dbz8571 (id,data) values (2,'{2: 10, 5: 20, 20: 30}/25');");
        TestHelper.execute("UPDATE s1.dbz8571 set data = '{1:5,2:10,3:25}/25' WHERE id = 2;");
        TestHelper.execute("DELETE FROM s1.dbz8571 WHERE id = 2");

        final SourceRecords records = consumeRecordsByTopic(5);
        final List<SourceRecord> tableRecords = records.recordsForTopic(topicName());
        assertThat(tableRecords).hasSize(5);

        assertRead(tableRecords.get(0), 1, "{ \"dimensions\": 25, \"vector\": { \"1\": 25.0, \"2\": 15.0, \"10\": 100.0 } }");
        assertInsert(tableRecords.get(1), 2, "{ \"dimensions\": 25, \"vector\": { \"2\": 10.0, \"5\": 20.0, \"20\": 30.0 } }");
        assertUpdate(tableRecords.get(2), 2, "{ \"dimensions\": 25, \"vector\": { \"2\": 10.0, \"5\": 20.0, \"20\": 30.0 } }",
                "{ \"dimensions\": 25, \"vector\": { \"1\": 5.0, \"2\": 10.0, \"3\": 25.0 } }");
        assertDelete(tableRecords.get(3), 2, "{ \"dimensions\": 25, \"vector\": { \"1\": 5.0, \"2\": 10.0, \"3\": 25.0 } }");
        assertTombstone(tableRecords.get(4), 2);
    }

    protected Configuration getConfigurationWithTransform() {
        return TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1\\.dbz8571")
                .with("transforms", "vectortojson")
                .with("transforms.vectortojson.type", VectorToJsonConverter.class.getName())
                .build();
    }

    protected void assertRead(SourceRecord record, int keyValue, String json) {
        VerifyRecord.isValidRead(record, "id", keyValue);
        assertFieldIsJsonSchema(getAfter(record), "data");
        assertFieldIsJson(getAfter(record), "data", json);
    }

    protected void assertInsert(SourceRecord record, int keyValue, String json) {
        VerifyRecord.isValidInsert(record, "id", keyValue);
        assertFieldIsJsonSchema(getAfter(record), "data");
        assertFieldIsJson(getAfter(record), "data", json);
    }

    protected void assertUpdate(SourceRecord record, int keyValue, String oldJson, String newJson) {
        VerifyRecord.isValidUpdate(record, "id", keyValue);
        assertFieldIsJsonSchema(getBefore(record), "data");
        assertFieldIsJson(getBefore(record), "data", oldJson);
        assertFieldIsJsonSchema(getAfter(record), "data");
        assertFieldIsJson(getAfter(record), "data", newJson);
    }

    protected void assertDelete(SourceRecord record, int keyValue, String oldJson) {
        VerifyRecord.isValidDelete(record, "id", keyValue);
        assertFieldIsJsonSchema(getBefore(record), "data");
        assertFieldIsJson(getBefore(record), "data", oldJson);
    }

    protected void assertTombstone(SourceRecord record, int keyValue) {
        VerifyRecord.isValidTombstone(record, "id", keyValue);
    }

    protected Struct getBefore(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.BEFORE);
    }

    protected Struct getAfter(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
    }

    protected void assertFieldIsJsonSchema(Struct struct, String fieldName) {
        assertThat(struct.schema().field(fieldName).schema()).isEqualTo(
                struct.schema().isOptional() ? Json.builder().optional().build() : Json.schema());
    }

    protected void assertFieldIsJson(Struct value, String fieldName, String json) {
        assertThat(value.get(fieldName)).isEqualTo(json);
    }

    protected String topicName() {
        return TestHelper.TEST_SERVER + ".s1.dbz8571";
    }

}
