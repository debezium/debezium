/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.data.Json;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.jdbc.JdbcConnection;

/**
 * Abstract base class for testing the {@link VectorToJsonConverter} transformation.
 *
 * @author Chris Cranford
 */
public abstract class AbstractVectorToJsonConverterTest<T extends SourceConnector> extends AbstractAsyncEngineConnectorTest {

    protected abstract Class<T> getConnectorClass();

    protected abstract JdbcConnection databaseConnection();

    protected abstract Configuration.Builder getConfigurationBuilder();

    protected abstract String topicName();

    protected abstract void doBefore() throws Exception;

    protected abstract void doAfter() throws Exception;

    protected abstract void createFloatVectorTable() throws Exception;

    protected abstract void createFloatVectorSnapshotData() throws Exception;

    protected abstract void createFloatVectorStreamData() throws Exception;

    protected abstract void createDoubleVectorTable() throws Exception;

    protected abstract void createDoubleVectorSnapshotData() throws Exception;

    protected abstract void createDoubleVectorStreamData() throws Exception;

    protected abstract void waitForStreamingStarted() throws InterruptedException;

    private JdbcConnection connection;

    @Before
    public void before() throws Exception {
        doBefore();

        connection = databaseConnection();
        connection.setAutoCommit(false);
    }

    @After
    public void after() throws Exception {
        stopConnector();
        assertNoRecordsToConsume();

        doAfter();

        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-8571")
    public void shouldConvertFloatVectorToJson() throws Exception {
        createFloatVectorTable();
        createFloatVectorSnapshotData();

        start(getConnectorClass(), getConfigurationWithTransform());
        assertConnectorIsRunning();

        waitForStreamingStarted();

        createFloatVectorStreamData();

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
        createDoubleVectorTable();
        createDoubleVectorSnapshotData();

        start(getConnectorClass(), getConfigurationWithTransform());
        assertConnectorIsRunning();

        waitForStreamingStarted();

        createDoubleVectorStreamData();

        final SourceRecords records = consumeRecordsByTopic(5);
        final List<SourceRecord> tableRecords = records.recordsForTopic(topicName());
        assertThat(tableRecords).hasSize(5);

        assertRead(tableRecords.get(0), 1, "{ \"values\": [101.0, 102.0, 103.0] }");
        assertInsert(tableRecords.get(1), 2, "{ \"values\": [1.0, 2.0, 3.0] }");
        assertUpdate(tableRecords.get(2), 2, "{ \"values\": [1.0, 2.0, 3.0] }", "{ \"values\": [5.0, 7.0, 9.0] }");
        assertDelete(tableRecords.get(3), 2, "{ \"values\": [5.0, 7.0, 9.0] }");
        assertTombstone(tableRecords.get(4), 2);
    }

    protected Configuration getConfigurationWithTransform() {
        return getConfigurationBuilder()
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
        assertThat(struct.schema().field(fieldName).schema()).isEqualTo(Json.schema());
    }

    protected void assertFieldIsJson(Struct value, String fieldName, String json) {
        assertThat(value.get(fieldName)).isEqualTo(json);
    }
}
