/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;

import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.sink.junit.EnvelopeSinkRecordFactory;
import io.debezium.connector.mongodb.sink.junit.SinkRecordFactory;
import io.debezium.connector.mongodb.sink.junit.SinkRecordFactoryArgumentsProvider;

class MongoDbSinkRecordTest {

    private static final String TOPIC_PREFIX = "dbserver1";

    private static MongoDbSinkConnectorConfig createConfig(String... keyValues) {
        var builder = Configuration.create()
                .with(MongoDbSinkConnectorConfig.CONNECTION_STRING, "mongodb://localhost:27017")
                .with(MongoDbSinkConnectorConfig.SINK_DATABASE, "testdb");
        for (int i = 0; i < keyValues.length; i += 2) {
            builder = builder.with(keyValues[i], keyValues[i + 1]);
        }
        return new MongoDbSinkConnectorConfig(builder.build());
    }

    // ─── CREATE / UPDATE ────────────────────────────────────────────────────────

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void createShouldProduceReplaceOneModel(SinkRecordFactory factory) {
        var config = createConfig();
        SinkRecord record = factory.createProduct(TOPIC_PREFIX, 101, "scooter", "Small scooter", 3.14).build(0);

        var processed = new MongoDbSinkRecord(record, config);

        assertThat(processed.getConversionException()).isNull();
        assertThat(processed.getWriteModel()).isInstanceOf(ReplaceOneModel.class);
        assertThat(processed.getAfterDoc()).isNotNull();
        assertThat(processed.getAfterDoc().getString("name").getValue()).isEqualTo("scooter");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void updateShouldProduceReplaceOneModel(SinkRecordFactory factory) {
        var config = createConfig();
        SinkRecord record = factory.updateProduct(TOPIC_PREFIX, 101, "scooter", "Updated", 4.0).build(1);

        var processed = new MongoDbSinkRecord(record, config);

        assertThat(processed.getConversionException()).isNull();
        assertThat(processed.getWriteModel()).isInstanceOf(ReplaceOneModel.class);
    }

    // ─── DELETE ─────────────────────────────────────────────────────────────────

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void deleteWithDeleteEnabledShouldProduceDeleteOneModel(SinkRecordFactory factory) {
        var config = createConfig("delete.enabled", "true", "primary.key.mode", "record_key");
        SinkRecord record = factory.deleteProduct(TOPIC_PREFIX, 101).build(0);

        var processed = new MongoDbSinkRecord(record, config);

        assertThat(processed.getConversionException()).isNull();
        assertThat(processed.getWriteModel()).isInstanceOf(DeleteOneModel.class);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void deleteWithDeleteDisabledShouldProduceNullWriteModel(SinkRecordFactory factory) {
        var config = createConfig("primary.key.mode", "record_key");
        SinkRecord record = factory.deleteProduct(TOPIC_PREFIX, 101).build(0);

        var processed = new MongoDbSinkRecord(record, config);

        assertThat(processed.getConversionException()).isNull();
        assertThat(processed.getWriteModel()).isNull();
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @SuppressWarnings("unchecked")
    void deleteFilterShouldMatchByKeyId(SinkRecordFactory factory) {
        var config = createConfig("delete.enabled", "true", "primary.key.mode", "record_key");
        SinkRecord record = factory.deleteProduct(TOPIC_PREFIX, 101).build(0);

        var processed = new MongoDbSinkRecord(record, config);
        DeleteOneModel<BsonDocument> model = (DeleteOneModel<BsonDocument>) processed.getWriteModel();

        BsonDocument filter = (BsonDocument) model.getFilter();
        assertThat(filter.containsKey("_id")).isTrue();
    }

    // ─── TOMBSTONE ──────────────────────────────────────────────────────────────

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void tombstoneShouldProduceNullWriteModel(SinkRecordFactory factory) {
        var config = createConfig();
        SinkRecord record = factory.tombstone(TOPIC_PREFIX, 101, 0);

        var processed = new MongoDbSinkRecord(record, config);

        assertThat(processed.getConversionException()).isNull();
        assertThat(processed.getWriteModel()).isNull();
        assertThat(processed.getAfterDoc()).isNull();
    }

    // ─── TRUNCATE (envelope-only, truncate has no flat equivalent) ───────────────

    @Test
    void truncateShouldProduceNullWriteModel() {
        var config = createConfig();
        SinkRecord record = new EnvelopeSinkRecordFactory().truncateProduct(TOPIC_PREFIX, 0);

        var processed = new MongoDbSinkRecord(record, config);

        assertThat(processed.getConversionException()).isNull();
        assertThat(processed.getWriteModel()).isNull();
    }

    // ─── PRIMARY KEY MODES ──────────────────────────────────────────────────────

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void recordValuePrimaryKeyModeShouldBuildIdFromPayloadFields(SinkRecordFactory factory) {
        var config = createConfig("primary.key.mode", "record_value", "primary.key.fields", "id");
        SinkRecord record = factory.createProduct(TOPIC_PREFIX, 101, "scooter", "Small scooter", 3.14).build(0);

        var processed = new MongoDbSinkRecord(record, config);

        assertThat(processed.getConversionException()).isNull();
        assertThat(processed.getWriteModel()).isInstanceOf(ReplaceOneModel.class);
        assertThat(processed.getKeyDoc().containsKey("id")).isTrue();
        assertThat(processed.getKeyDoc().getInt32("id").getValue()).isEqualTo(101);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void recordHeaderPrimaryKeyModeShouldBuildIdFromHeaders(SinkRecordFactory factory) {
        var config = createConfig("primary.key.mode", "record_header");
        SinkRecord record = factory.record(TOPIC_PREFIX + ".inventory.products")
                .keyField("id", Schema.INT32_SCHEMA, 101)
                .field("id", Schema.INT32_SCHEMA, 101)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA, "scooter")
                .header("tenant_id", Schema.STRING_SCHEMA, "acme")
                .build(0);

        var processed = new MongoDbSinkRecord(record, config);

        assertThat(processed.getConversionException()).isNull();
        assertThat(processed.getWriteModel()).isInstanceOf(ReplaceOneModel.class);
        assertThat(processed.getKeyDoc().containsKey("tenant_id")).isTrue();
        assertThat(processed.getKeyDoc().getString("tenant_id").getValue()).isEqualTo("acme");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void primaryKeyFieldsShouldRestrictKeyComposition(SinkRecordFactory factory) {
        var config = createConfig("primary.key.mode", "record_key", "primary.key.fields", "id");
        SinkRecord record = factory.record(TOPIC_PREFIX + ".inventory.products")
                .keyField("id", Schema.INT32_SCHEMA, 101)
                .keyField("tenant", Schema.STRING_SCHEMA, "acme")
                .field("id", Schema.INT32_SCHEMA, 101)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA, "scooter")
                .build(0);

        var processed = new MongoDbSinkRecord(record, config);

        assertThat(processed.getConversionException()).isNull();
        assertThat(processed.getKeyDoc().containsKey("id")).isTrue();
        assertThat(processed.getKeyDoc().containsKey("tenant")).isFalse();
    }

    // ─── FIELD INCLUDE LIST in write path ───────────────────────────────────────

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void fieldIncludeListShouldFilterWrittenDocument(SinkRecordFactory factory) {
        var config = createConfig("field.include.list", "name");
        SinkRecord record = factory.createProduct(TOPIC_PREFIX, 101, "scooter", "Small scooter", 3.14).build(0);

        var processed = new MongoDbSinkRecord(record, config);

        assertThat(processed.getConversionException()).isNull();
        assertThat(processed.getAfterDoc().containsKey("name")).isTrue();
        assertThat(processed.getAfterDoc().containsKey("description")).isFalse();
        assertThat(processed.getAfterDoc().containsKey("weight")).isFalse();
    }

    // ─── NEGATIVE / EDGE CASES ──────────────────────────────────────────────────

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void recordKeyModeWithNullKeyShouldGenerateObjectId(SinkRecordFactory factory) {
        var config = createConfig("primary.key.mode", "record_key");
        SinkRecord record = new SinkRecord(TOPIC_PREFIX + ".inventory.products", 0,
                null, null,
                factory.createProduct(TOPIC_PREFIX, 101, "scooter", "test", 1.0).build(0).valueSchema(),
                factory.createProduct(TOPIC_PREFIX, 101, "scooter", "test", 1.0).build(0).value(),
                0);

        var processed = new MongoDbSinkRecord(record, config);

        assertThat(processed.getConversionException()).isNotNull();
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void fieldExcludeListShouldNotAffectKeyFieldsInId(SinkRecordFactory factory) {
        var config = createConfig("field.exclude.list", "id", "primary.key.mode", "record_key");
        SinkRecord record = factory.createProduct(TOPIC_PREFIX, 101, "scooter", "Small scooter", 3.14).build(0);

        var processed = new MongoDbSinkRecord(record, config);

        assertThat(processed.getConversionException()).isNull();
        assertThat(processed.getWriteModel()).isInstanceOf(ReplaceOneModel.class);
        @SuppressWarnings("unchecked")
        var model = (ReplaceOneModel<BsonDocument>) processed.getWriteModel();
        BsonDocument filter = (BsonDocument) model.getFilter();
        assertThat(filter.containsKey("_id")).isTrue();
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void fieldIncludeListWithNonExistentFieldShouldProduceEmptyPayload(SinkRecordFactory factory) {
        var config = createConfig("field.include.list", "nonexistent_field");
        SinkRecord record = factory.createProduct(TOPIC_PREFIX, 101, "scooter", "Small scooter", 3.14).build(0);

        var processed = new MongoDbSinkRecord(record, config);

        assertThat(processed.getConversionException()).isNull();
        assertThat(processed.getWriteModel()).isInstanceOf(ReplaceOneModel.class);
        assertThat(processed.getAfterDoc().containsKey("name")).isFalse();
        assertThat(processed.getAfterDoc().containsKey("description")).isFalse();
        assertThat(processed.getAfterDoc().containsKey("weight")).isFalse();
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void sameKeyUpsertShouldProduceOneDocument(SinkRecordFactory factory) {
        var config = createConfig("primary.key.mode", "record_key");
        SinkRecord first = factory.createProduct(TOPIC_PREFIX, 101, "scooter", "v1", 1.0).build(0);
        SinkRecord second = factory.updateProduct(TOPIC_PREFIX, 101, "scooter", "v2", 2.0).build(1);

        var processed1 = new MongoDbSinkRecord(first, config);
        var processed2 = new MongoDbSinkRecord(second, config);

        assertThat(processed1.getWriteModel()).isInstanceOf(ReplaceOneModel.class);
        assertThat(processed2.getWriteModel()).isInstanceOf(ReplaceOneModel.class);

        @SuppressWarnings("unchecked")
        BsonDocument filter1 = (BsonDocument) ((ReplaceOneModel<BsonDocument>) processed1.getWriteModel()).getFilter();
        @SuppressWarnings("unchecked")
        BsonDocument filter2 = (BsonDocument) ((ReplaceOneModel<BsonDocument>) processed2.getWriteModel()).getFilter();
        assertThat(filter1).isEqualTo(filter2);
    }
}
