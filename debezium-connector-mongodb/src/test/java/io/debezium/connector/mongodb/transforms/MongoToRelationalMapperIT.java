/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.model.ChangeStreamPreAndPostImagesOptions;
import com.mongodb.client.model.CreateCollectionOptions;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.mongodb.AbstractMongoConnectorIT;
import io.debezium.connector.mongodb.MongoDbConnector;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.connector.mongodb.MongoDbConnectorConfig.CaptureMode;
import io.debezium.connector.mongodb.MongoDbConnectorConfig.JsonSerializationMode;
import io.debezium.connector.mongodb.TestHelper;
import io.debezium.connector.mongodb.transforms.MongoToRelationalMapper.JsonOutputMode;
import io.debezium.data.Json;
import io.debezium.doc.FixFor;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.transforms.ExtractChangedRecordState;

/**
 * Exercises the mapper with snapshot and change stream events from a real MongoDB replica set.
 */
public class MongoToRelationalMapperIT extends AbstractMongoConnectorIT {

    private static final String DATABASE = "mapperTypes";
    private static final String COLLECTION = "documents";
    private static final String SERVER = "mapperServer";
    private static final JsonWriterSettings CANONICAL = JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();

    @Test
    void shouldConvertUnsignedTimestampsThroughBothMongoTransformations() throws Exception {
        try (var client = TestHelper.connect(mongo)) {
            final var collection = client.getDatabase(DATABASE).getCollection(COLLECTION, BsonDocument.class);
            collection.insertOne(new BsonDocument("_id", new BsonInt32(1))
                    .append("value", new BsonTimestamp(Integer.MAX_VALUE, 7)));
            startInferenceConnector();
            assertTimestampEvent(consumeSingleRecord(), "r", 2_147_483_647_000L);
            waitForSnapshotToBeCompleted("mongodb", SERVER);
            waitForStreamingRunning("mongodb", SERVER);

            collection.insertOne(new BsonDocument("_id", new BsonInt32(2))
                    .append("value", new BsonTimestamp(Integer.MIN_VALUE, 7)));
            assertTimestampEvent(consumeSingleRecord(), "c", 2_147_483_648_000L);
            collection.updateOne(new BsonDocument("_id", new BsonInt32(2)),
                    new BsonDocument("$set", new BsonDocument("value", new BsonTimestamp(-1, -1))));
            assertTimestampEvent(consumeSingleRecord(), "u", 4_294_967_295_000L);
        }
    }

    @ParameterizedTest
    @EnumSource(value = BsonType.class, names = { "UNDEFINED", "DB_POINTER" })
    void shouldRejectUnsupportedInferenceButAllowJsonProjection(BsonType type) throws Exception {
        final var value = MongoBsonTypeTestData.values().get(type == BsonType.UNDEFINED ? "undefinedValue" : "dbPointerValue");
        try (var client = TestHelper.connect(mongo)) {
            final var collection = client.getDatabase(DATABASE).getCollection(COLLECTION, BsonDocument.class);
            collection.insertOne(new BsonDocument("_id", new BsonInt32(1)).append("value", value));
            startInferenceConnector();
            assertUnsupportedEvent(consumeSingleRecord(), type, "/value");
            waitForSnapshotToBeCompleted("mongodb", SERVER);
            waitForStreamingRunning("mongodb", SERVER);

            collection.insertOne(new BsonDocument("_id", new BsonInt32(2))
                    .append("items", new BsonArray(List.of(new BsonDocument("value", value)))));
            assertUnsupportedEvent(consumeSingleRecord(), type, "/items/0/value");
        }
    }

    private void startInferenceConnector() {
        config = TestHelper.getConfiguration(mongo).edit()
                .with(CommonConnectorConfig.TOPIC_PREFIX, SERVER)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, DATABASE + "." + COLLECTION)
                .with(MongoDbConnectorConfig.CAPTURE_MODE, CaptureMode.CHANGE_STREAMS_UPDATE_FULL)
                .with(MongoDbConnectorConfig.JSON_SERIALIZATION_MODE, JsonSerializationMode.EXTENDED)
                .build();
        start(MongoDbConnector.class, config);
    }

    private static void assertTimestampEvent(SourceRecord original, String operation, long expectedMillis) {
        assertThat(((Struct) original.value()).getString("op")).isEqualTo(operation);
        try (var mapper = new MongoToRelationalMapper<SourceRecord>(); var unwrap = new ExtractNewDocumentState<SourceRecord>()) {
            mapper.configure(Map.of());
            unwrap.configure(Map.of());
            final var inferred = ((Struct) mapper.apply(original).value()).getStruct("after");
            final var unwrapped = (Struct) unwrap.apply(original).value();
            for (Struct value : List.of(inferred, unwrapped)) {
                value.validate();
                assertThat(value.get("value")).isEqualTo(new Date(expectedMillis));
            }
        }
    }

    private static void assertUnsupportedEvent(SourceRecord original, BsonType type, String path) throws JsonProcessingException {
        final var incoming = ((Struct) original.value()).getString("after");
        assertThat(new MongoDocumentPath(path).read(BsonDocument.parse(incoming)).getBsonType()).isEqualTo(type);
        try (var mapper = new MongoToRelationalMapper<SourceRecord>()) {
            mapper.configure(Map.of());
            assertThatThrownBy(() -> mapper.apply(original))
                    .isInstanceOf(DataException.class)
                    .hasMessageContaining("unsupported BSON type " + type)
                    .hasMessageContaining("'" + path + "'")
                    .hasMessageContaining("io.debezium.data.Json");
            final var projection = new ObjectMapper().writeValueAsString(Map.of(
                    "documentJson", Map.of("path", "", "type", Json.LOGICAL_NAME),
                    "selected", Map.of("path", path, "type", Json.LOGICAL_NAME)));
            mapper.configure(Map.of("schema.mapping." + DATABASE + "." + COLLECTION, projection));
            final var result = ((Struct) mapper.apply(original).value()).getStruct("after");
            result.validate();
            assertThat(result.getString("documentJson")).isEqualTo(((Struct) original.value()).getString("after"));
            assertThat(BsonDocument.parse("{\"v\":" + result.getString("selected") + "}").get("v").getBsonType()).isEqualTo(type);
        }
    }

    @ParameterizedTest
    @EnumSource(JsonSerializationMode.class)
    @FixFor("dbz#1715")
    @SkipWhenDatabaseVersion(check = LESS_THAN, major = 6, reason = "Change stream pre-images require MongoDB 6.0 or later")
    void shouldMapAllBsonTypesThroughSnapshotAndStreaming(JsonSerializationMode sourceMode) throws Exception {
        final var snapshotDocument = new BsonDocument("_id", new BsonInt32(1))
                .append("values", MongoBsonTypeTestData.values());
        try (var client = TestHelper.connect(mongo)) {
            final var database = client.getDatabase(DATABASE);
            database.createCollection(COLLECTION, new CreateCollectionOptions()
                    .changeStreamPreAndPostImagesOptions(new ChangeStreamPreAndPostImagesOptions(true)));
            final var collection = database.getCollection(COLLECTION, BsonDocument.class);
            collection.insertOne(snapshotDocument);
            // Read BSON directly so deprecated types and binary subtypes cannot disappear in a JSON round trip.
            assertThat(collection.find(new BsonDocument("_id", new BsonInt32(1))).first()).isEqualTo(snapshotDocument);

            config = TestHelper.getConfiguration(mongo).edit()
                    .with(CommonConnectorConfig.TOPIC_PREFIX, SERVER)
                    .with(CommonConnectorConfig.TOMBSTONES_ON_DELETE, true)
                    .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, DATABASE + "." + COLLECTION)
                    .with(MongoDbConnectorConfig.JSON_SERIALIZATION_MODE, sourceMode)
                    .with(MongoDbConnectorConfig.CAPTURE_MODE, CaptureMode.CHANGE_STREAMS_UPDATE_FULL_WITH_PRE_IMAGE)
                    .build();
            start(MongoDbConnector.class, config);
            assertMappedEvent(consumeSingleRecord(), "r", null, snapshotDocument, sourceMode);
            waitForSnapshotToBeCompleted("mongodb", SERVER);
            waitForStreamingRunning("mongodb", SERVER);

            final var inserted = snapshotDocument.clone();
            inserted.put("_id", new BsonInt32(2));
            collection.insertOne(inserted);
            assertMappedEvent(consumeSingleRecord(), "c", null, inserted, sourceMode);

            final var filter = new BsonDocument("_id", new BsonInt32(2));
            collection.updateOne(filter, BsonDocument.parse("""
                    {"$set":{"values.int32Value":43,"values.arrayValue":[1]},"$unset":{"values.stringValue":""}}
                    """));
            final var updated = inserted.clone();
            updated.getDocument("values").put("int32Value", new BsonInt32(43));
            updated.getDocument("values").put("arrayValue", new BsonArray(List.of(new BsonInt32(1))));
            updated.getDocument("values").remove("stringValue");
            assertThat(collection.find(filter).first()).isEqualTo(updated);
            assertMappedEvent(consumeSingleRecord(), "u", inserted, updated, sourceMode);

            collection.deleteOne(filter);
            final var deleted = consumeRecordsByTopic(2).allRecordsInOrder();
            assertThat(deleted).hasSize(2);
            assertMappedEvent(deleted.get(0), "d", updated, null, sourceMode);
            final var tombstone = deleted.get(1);
            assertThat(tombstone.value()).isNull();
            assertThat(tombstone.key()).isEqualTo(deleted.get(0).key());
            for (JsonOutputMode outputMode : JsonOutputMode.values()) {
                try (var mapper = mapper(outputMode); var changes = changes()) {
                    assertThat(changes.apply(mapper.apply(tombstone))).isSameAs(tombstone);
                }
            }
        }
    }

    private SourceRecord consumeSingleRecord() throws InterruptedException {
        final var records = consumeRecordsByTopic(1).allRecordsInOrder();
        assertThat(records).hasSize(1);
        return records.get(0);
    }

    private static void assertMappedEvent(SourceRecord original, String operation, BsonDocument before, BsonDocument after,
                                          JsonSerializationMode sourceMode)
            throws Exception {
        final var source = (Struct) original.value();
        final var settings = inputSettings(sourceMode);
        assertThat(source.getString("op")).isEqualTo(operation);
        assertThat(source.getString("before")).isEqualTo(before == null ? null : before.toJson(settings));
        assertThat(source.getString("after")).isEqualTo(after == null ? null : after.toJson(settings));
        original.headers().addString("test-header", "preserved");

        for (JsonOutputMode outputMode : JsonOutputMode.values()) {
            try (var mapper = mapper(outputMode); var changes = changes(); var converter = new JsonConverter()) {
                final var result = changes.apply(mapper.apply(original));
                final var envelope = (Struct) result.value();
                envelope.validate();
                assertImage(envelope.getStruct("before"), before, settings, outputMode);
                assertImage(envelope.getStruct("after"), after, settings, outputMode);
                assertThat(result.keySchema()).isSameAs(original.keySchema());
                assertThat(result.key()).isEqualTo(original.key());
                assertThat(result.sourcePartition()).isEqualTo(original.sourcePartition());
                assertThat(result.sourceOffset()).isEqualTo(original.sourceOffset());
                assertThat(result.topic()).isEqualTo(original.topic());
                assertThat(result.kafkaPartition()).isEqualTo(original.kafkaPartition());
                assertThat(result.timestamp()).isEqualTo(original.timestamp());
                assertThat(result.headers().lastWithName("test-header").value()).isEqualTo("preserved");
                for (var field : original.valueSchema().fields()) {
                    if (!List.of("before", "after").contains(field.name())) {
                        assertThat(envelope.get(field.name())).as(field.name()).isEqualTo(source.get(field.name()));
                    }
                }
                if (operation.equals("u")) {
                    assertThat(result.headers().lastWithName("Changed").value()).asList()
                            .containsAll(List.of("stringValue", "arrayValue", "int32Value", "documentJson", "valuesJson", "typedString", "typedInt32"));
                    assertThat(envelope.getStruct("before").schema()).isSameAs(envelope.getStruct("after").schema());
                }
                else {
                    assertThat((List<?>) result.headers().lastWithName("Changed").value()).isEmpty();
                }
                converter.configure(Map.of("schemas.enable", true), false);
                final var bytes = converter.fromConnectData(result.topic(), result.valueSchema(), result.value());
                final var restored = converter.toConnectData(result.topic(), bytes);
                assertThat(restored.schema()).isEqualTo(result.valueSchema());
                final var restoredEnvelope = (Struct) restored.value();
                restoredEnvelope.validate();
                assertImage(restoredEnvelope.getStruct("before"), before, settings, outputMode);
                assertImage(restoredEnvelope.getStruct("after"), after, settings, outputMode);
            }
        }
    }

    private static void assertImage(Struct image, BsonDocument original, JsonWriterSettings settings, JsonOutputMode outputMode) {
        if (original == null) {
            assertThat(image).isNull();
            return;
        }
        final var incoming = original.toJson(settings);
        // Canonical output cannot recover BSON type information already lost by the source's relaxed JSON mode.
        final var expected = outputMode == JsonOutputMode.INPUT ? original : BsonDocument.parse(incoming);
        final var outputSettings = outputMode == JsonOutputMode.INPUT ? settings : CANONICAL;
        assertThat(image.getString("documentJson")).isEqualTo(expected.toJson(outputSettings));
        assertThat(image.getString("valuesJson")).isEqualTo(expected.getDocument("values").toJson(outputSettings));
        final var values = expected.getDocument("values");
        for (var field : MongoBsonTypeTestData.values().keySet()) {
            assertThat(image.schema().field(field).schema().name()).isEqualTo(Json.LOGICAL_NAME);
            assertThat(image.getString(field)).as("%s / %s", outputMode, field).isEqualTo(fragment(values.get(field), outputSettings));
        }
        assertThat(image.getString("typedString")).isEqualTo(values.containsKey("stringValue") ? "Seoul 서울 🙂" : null);
        assertThat(image.getInt32("typedInt32")).isEqualTo(values.getInt32("int32Value").getValue());
        assertThat(image.getInt64("typedInt64")).isEqualTo(42L);
        assertThat(image.getBoolean("typedBoolean")).isTrue();
        assertThat(image.getFloat64("typedDouble")).isEqualTo(1.25);
        assertThat(image.getString("typedObjectId")).isEqualTo("507f1f77bcf86cd799439011");
        assertThat(image.getBytes("typedBinary")).containsExactly((byte) 0xff, (byte) 1);
        assertThat(image.getString("typedUuid")).isEqualTo("00112233-4455-6677-8899-aabbccddeeff");
        assertThat(image.get("typedDate")).isEqualTo(new Date(1_783_078_553_473L));
        assertThat(image.get("typedTimestamp")).isEqualTo(new Date(1_783_078_553_000L));
        assertThat(image.get("typedDecimal")).isEqualTo(new BigDecimal("12345678901234567890.12345678901234"));
    }

    private static String fragment(BsonValue value, JsonWriterSettings settings) {
        if (value == null || value.isNull()) {
            return null;
        }
        final var wrapper = new BsonDocument("v", value).toJson(settings);
        return wrapper.substring(wrapper.indexOf(':') + 1, wrapper.length() - 1).trim();
    }

    private static JsonWriterSettings inputSettings(JsonSerializationMode mode) {
        final var jsonMode = switch (mode) {
            case LEGACY, STRICT -> JsonMode.STRICT;
            case EXTENDED -> JsonMode.EXTENDED;
            case RELAXED -> JsonMode.RELAXED;
        };
        return JsonWriterSettings.builder().outputMode(jsonMode).indent(true).indentCharacters("").newLineCharacters("").build();
    }

    private static MongoToRelationalMapper<SourceRecord> mapper(JsonOutputMode mode) throws JsonProcessingException {
        Map<String, Object> projection = new LinkedHashMap<>();
        projection.put("documentJson", Map.of("path", "", "type", Json.LOGICAL_NAME));
        projection.put("valuesJson", Map.of("path", "/values", "type", Json.LOGICAL_NAME));
        MongoBsonTypeTestData.values().keySet().forEach(field -> projection.put(field, Map.of("path", "/values/" + field, "type", Json.LOGICAL_NAME)));
        final Map<String, String> typed = Map.ofEntries(
                Map.entry("stringValue", "string"), Map.entry("int32Value", "int32"), Map.entry("int64Value", "int64"),
                Map.entry("booleanValue", "boolean"), Map.entry("doubleValue", "float64"), Map.entry("objectIdValue", "string"),
                Map.entry("binaryValue", "bytes"), Map.entry("uuid", "io.debezium.data.Uuid"),
                Map.entry("dateValue", "org.apache.kafka.connect.data.Timestamp"), Map.entry("timestampValue", "org.apache.kafka.connect.data.Timestamp"));
        typed.forEach((field, type) -> {
            final var name = field.endsWith("Value") ? field.substring(0, field.length() - 5) : field;
            projection.put("typed" + Character.toUpperCase(name.charAt(0)) + name.substring(1), Map.of("path", "/values/" + field, "type", type));
        });
        projection.put("typedDecimal", Map.of("path", "/values/decimalValue", "type", "org.apache.kafka.connect.data.Decimal", "scale", 14, "precision", 34));
        final var mapper = new MongoToRelationalMapper<SourceRecord>();
        mapper.configure(Map.of("schema.mapping." + DATABASE + "." + COLLECTION, new ObjectMapper().writeValueAsString(projection), "json.output.mode", mode.getValue()));
        return mapper;
    }

    private static ExtractChangedRecordState<SourceRecord> changes() {
        final var changes = new ExtractChangedRecordState<SourceRecord>();
        changes.configure(Map.of("header.changed.name", "Changed", "header.unchanged.name", "Unchanged"));
        return changes;
    }
}
