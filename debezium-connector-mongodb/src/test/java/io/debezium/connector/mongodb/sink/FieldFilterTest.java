/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import com.mongodb.client.model.ReplaceOneModel;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.sink.junit.SinkRecordFactory;
import io.debezium.connector.mongodb.sink.junit.SinkRecordFactoryArgumentsProvider;
import io.debezium.data.Envelope;

class FieldFilterTest {

    private static final String TOPIC = "server1.inventory.customers";
    private static final String TOPIC_PREFIX = "server1";
    private static final String CE_PATTERN = ".*CloudEvents\\.Envelope$";

    // --- Framework-level filter tests (KafkaDebeziumSinkRecord.getFilteredPayload) ---

    @Test
    void includeListShouldFilterPayloadFields() {
        var config = createConfig("field.include.list", "id,name");
        KafkaDebeziumSinkRecord record = createFlatRecord(TOPIC, 1, "John", "john@test.com");
        Struct filtered = record.getFilteredPayload(config.fieldFilter());

        assertThat(filtered.schema().fields()).hasSize(2);
        assertThat(filtered.get("id")).isEqualTo(1);
        assertThat(filtered.get("name")).isEqualTo("John");
    }

    @Test
    void excludeListShouldFilterPayloadFields() {
        var config = createConfig("field.exclude.list", "email");
        KafkaDebeziumSinkRecord record = createFlatRecord(TOPIC, 1, "John", "john@test.com");
        Struct filtered = record.getFilteredPayload(config.fieldFilter());

        assertThat(filtered.schema().fields()).hasSize(2);
        assertThat(filtered.get("id")).isEqualTo(1);
        assertThat(filtered.get("name")).isEqualTo("John");
    }

    @Test
    void topicQualifiedIncludeListShouldFilterByTopic() {
        var config = createConfig("field.include.list",
                "server1.inventory.customers:id,server1.inventory.customers:name");
        KafkaDebeziumSinkRecord record = createFlatRecord(TOPIC, 1, "John", "john@test.com");
        Struct filtered = record.getFilteredPayload(config.fieldFilter());

        assertThat(filtered.schema().fields()).hasSize(2);
        assertThat(filtered.get("id")).isEqualTo(1);
        assertThat(filtered.get("name")).isEqualTo("John");
    }

    @Test
    void topicQualifiedIncludeListShouldNotMatchOtherTopics() {
        var config = createConfig("field.include.list", "other.topic:id");
        KafkaDebeziumSinkRecord record = createFlatRecord(TOPIC, 1, "John", "john@test.com");
        Struct filtered = record.getFilteredPayload(config.fieldFilter());

        assertThat(filtered.schema().fields()).isEmpty();
    }

    @Test
    void noFilterShouldReturnAllFields() {
        var config = createConfig();
        KafkaDebeziumSinkRecord record = createFlatRecord(TOPIC, 1, "John", "john@test.com");
        Struct filtered = record.getFilteredPayload(config.fieldFilter());

        assertThat(filtered.schema().fields()).hasSize(3);
    }

    @Test
    void filterShouldWorkWithEnvelopeRecords() {
        var config = createConfig("field.include.list", "id,name");
        KafkaDebeziumSinkRecord record = createEnvelopeKafkaRecord(TOPIC, 1, "John", "john@test.com");
        Struct payload = record.getPayload();

        assertThat(payload).isNotNull();
        assertThat(payload.get("id")).isEqualTo(1);
        assertThat(payload.get("name")).isEqualTo("John");
        assertThat(payload.get("email")).isEqualTo("john@test.com");

        Struct filtered = record.getFilteredPayload(config.fieldFilter());
        assertThat(filtered.schema().fields()).hasSize(2);
        assertThat(filtered.get("id")).isEqualTo(1);
        assertThat(filtered.get("name")).isEqualTo("John");
    }

    // --- Write-path tests: verify filtering in MongoDbSinkRecord (parameterized) ---

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void includeListShouldFilterFieldsInWrittenDocument(SinkRecordFactory factory) {
        var config = createConfig("field.include.list", "id,name", "primary.key.mode", "record_key");
        SinkRecord record = factory.record(TOPIC)
                .keyField("id", Schema.INT32_SCHEMA, 1)
                .field("id", Schema.INT32_SCHEMA, 1)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA, "John")
                .field("email", Schema.OPTIONAL_STRING_SCHEMA, "john@test.com")
                .build(0);

        BsonDocument replacement = replacementDoc(record, config);

        assertThat(replacement.containsKey("name")).isTrue();
        assertThat(replacement.getString("name").getValue()).isEqualTo("John");
        assertThat(replacement.containsKey("email")).isFalse();
        assertThat(replacement.containsKey("id")).isFalse();
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void excludeListShouldFilterFieldsInWrittenDocument(SinkRecordFactory factory) {
        var config = createConfig("field.exclude.list", "email");
        SinkRecord record = factory.record(TOPIC)
                .keyField("id", Schema.INT32_SCHEMA, 1)
                .field("id", Schema.INT32_SCHEMA, 1)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA, "John")
                .field("email", Schema.OPTIONAL_STRING_SCHEMA, "john@test.com")
                .build(0);

        BsonDocument replacement = replacementDoc(record, config);

        assertThat(replacement.containsKey("name")).isTrue();
        assertThat(replacement.containsKey("email")).isFalse();
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void topicQualifiedIncludeListShouldFilterFieldsForMatchingTopic(SinkRecordFactory factory) {
        var config = createConfig("field.include.list",
                "server1.inventory.customers:id,server1.inventory.customers:name");
        SinkRecord record = factory.record(TOPIC)
                .keyField("id", Schema.INT32_SCHEMA, 1)
                .field("id", Schema.INT32_SCHEMA, 1)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA, "John")
                .field("email", Schema.OPTIONAL_STRING_SCHEMA, "john@test.com")
                .build(0);

        BsonDocument replacement = replacementDoc(record, config);

        assertThat(replacement.containsKey("name")).isTrue();
        assertThat(replacement.containsKey("email")).isFalse();
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void topicQualifiedIncludeListShouldExcludeAllNonMatchingTopicFields(SinkRecordFactory factory) {
        var config = createConfig("field.include.list", "other.topic:name");
        SinkRecord record = factory.record(TOPIC)
                .keyField("id", Schema.INT32_SCHEMA, 1)
                .field("id", Schema.INT32_SCHEMA, 1)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA, "John")
                .field("email", Schema.OPTIONAL_STRING_SCHEMA, "john@test.com")
                .build(0);

        BsonDocument replacement = replacementDoc(record, config);

        assertThat(replacement.containsKey("name")).isFalse();
        assertThat(replacement.containsKey("email")).isFalse();
        assertThat(replacement.containsKey("_id")).isTrue();
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void noFilterShouldPreserveAllNonKeyFieldsInWrittenDocument(SinkRecordFactory factory) {
        var config = createConfig();
        SinkRecord record = factory.record(TOPIC)
                .keyField("id", Schema.INT32_SCHEMA, 1)
                .field("id", Schema.INT32_SCHEMA, 1)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA, "John")
                .field("email", Schema.OPTIONAL_STRING_SCHEMA, "john@test.com")
                .build(0);

        BsonDocument replacement = replacementDoc(record, config);

        assertThat(replacement.containsKey("name")).isTrue();
        assertThat(replacement.containsKey("email")).isTrue();
    }

    // --- Helpers ---

    @SuppressWarnings("unchecked")
    private static BsonDocument replacementDoc(SinkRecord record, MongoDbSinkConnectorConfig config) {
        var processedData = new MongoDbSinkRecord(record, config);
        assertThat(processedData.getWriteModel()).isInstanceOf(ReplaceOneModel.class);
        return ((ReplaceOneModel<BsonDocument>) processedData.getWriteModel()).getReplacement();
    }

    private static MongoDbSinkConnectorConfig createConfig(String... keyValues) {
        var builder = Configuration.create()
                .with(MongoDbSinkConnectorConfig.CONNECTION_STRING, "mongodb://localhost:27017")
                .with(MongoDbSinkConnectorConfig.SINK_DATABASE, "testdb");
        for (int i = 0; i < keyValues.length; i += 2) {
            builder = builder.with(keyValues[i], keyValues[i + 1]);
        }
        return new MongoDbSinkConnectorConfig(builder.build());
    }

    private static Schema createFlatSchema() {
        return SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("email", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    private static KafkaDebeziumSinkRecord createFlatRecord(String topic, int id, String name, String email) {
        Schema schema = createFlatSchema();
        Struct value = new Struct(schema).put("id", id).put("name", name).put("email", email);
        SinkRecord kafkaRecord = new SinkRecord(topic, 0, Schema.INT32_SCHEMA, id, schema, value, 0);
        return new KafkaDebeziumSinkRecord(kafkaRecord, CE_PATTERN);
    }

    private static KafkaDebeziumSinkRecord createEnvelopeKafkaRecord(String topic, int id, String name, String email) {
        Schema sourceSchema = SchemaBuilder.struct().field("ts_ms", Schema.INT64_SCHEMA).build();
        Schema recordSchema = createFlatSchema();
        Schema envelopeSchema = SchemaBuilder.struct()
                .name(topic + ".Envelope")
                .field(Envelope.FieldName.BEFORE, recordSchema)
                .field(Envelope.FieldName.AFTER, recordSchema)
                .field(Envelope.FieldName.SOURCE, sourceSchema)
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .build();
        Struct after = new Struct(recordSchema).put("id", id).put("name", name).put("email", email);
        Struct source = new Struct(sourceSchema).put("ts_ms", Instant.now().toEpochMilli());
        Struct envelope = new Struct(envelopeSchema)
                .put(Envelope.FieldName.AFTER, after)
                .put(Envelope.FieldName.SOURCE, source)
                .put(Envelope.FieldName.OPERATION, Envelope.Operation.CREATE.code())
                .put(Envelope.FieldName.TIMESTAMP, Instant.now().toEpochMilli());
        Schema keySchema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
        Struct key = new Struct(keySchema).put("id", id);
        SinkRecord kafkaRecord = new SinkRecord(topic, 0, keySchema, key, envelopeSchema, envelope, 0);
        return new KafkaDebeziumSinkRecord(kafkaRecord, CE_PATTERN);
    }
}
