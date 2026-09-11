/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.mongodb.MongoDbFieldName;
import io.debezium.connector.mongodb.MongoDbSchema;
import io.debezium.data.Json;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.transforms.ExtractChangedRecordState;

/**
 * Component integration tests using real SMTs and schema-enabled JSON converters.
 */
class MongoToRelationalMapperIntegrationTest {

    private static final String PROJECTION = """
            {"customer_city":{"path":"/customer/address/city","type":"string"},
             "second_sku":{"path":"/items/1/sku","type":"string"},
             "amount":{"path":"/amount","type":"org.apache.kafka.connect.data.Decimal","scale":2,"precision":10},
             "updated_at":{"path":"/updated_at","type":"org.apache.kafka.connect.data.Timestamp"},
             "document_json":{"path":"","type":"io.debezium.data.Json"}}
            """;
    private static final String BEFORE = """
             {"_id":1,"customer":{"address":{"city":"Seoul"}},"items":[{"sku":"A"},{"sku":"B"}],
              "amount":{"$numberDecimal":"12.34"},"updated_at":{"$date":"2026-01-01T00:00:00Z"},"unselected":[1,true,{"x":"y"}]}
            """;
    private static final String AFTER = """
             {"_id":1,"customer":{"address":{}},"items":[{"sku":"A"}],
              "amount":{"$numberDecimal":"12.34"},"updated_at":{"$date":"2026-01-01T00:00:00Z"},"unselected":[false,"text"]}
            """;

    private final MongoToRelationalMapper<SourceRecord> mapper = new MongoToRelationalMapper<>();
    private final ExtractChangedRecordState<SourceRecord> changes = new ExtractChangedRecordState<>();

    @BeforeEach
    void configure() {
        mapper.configure(Map.of());
        changes.configure(Map.of("header.changed.name", "Changed", "header.unchanged.name", "Unchanged"));
    }

    @AfterEach
    void close() {
        changes.close();
        mapper.close();
    }

    @Test
    void shouldIdentifyRemovedFieldsAndShrinkingArraysAfterInference() {
        final var original = record("u", """
                {"_id":1,"address":{"city":"Seoul","removed":42},"items":[1,2],"removed":true}
                """, """
                {"_id":1,"address":{"city":"Seoul"},"items":[]}
                """);
        final var result = changes.apply(mapper.apply(original));
        assertChangedFields(result, List.of("address", "items", "removed"), List.of("_id"));
        final var after = ((Struct) result.value()).getStruct("after");
        assertThat(after.getStruct("address").get("removed")).isNull();
        assertThat(after.get("removed")).isNull();
        assertThat(after.getArray("items")).isEmpty();
        assertPreservedMetadata(original, result);
    }

    @Test
    void shouldCompareProjectedFieldsWhilePreservingBothOriginalDocuments() {
        mapper.configure(Map.of("schema.mapping.shop.orders", PROJECTION));
        final var original = record("u", BEFORE, AFTER);
        final var result = changes.apply(mapper.apply(original));
        assertChangedFields(result, List.of("customer_city", "second_sku", "document_json"), List.of("amount", "updated_at"));
        assertProjection((Struct) result.value());
        assertPreservedMetadata(original, result);

        final var restored = changes.apply(mapper.apply(record("u", AFTER, BEFORE)));
        final var restoredValue = (Struct) restored.value();
        assertThat(restoredValue.getStruct("after").schema()).isSameAs(((Struct) result.value()).getStruct("after").schema());
        assertThat(restoredValue.getStruct("after").getString("second_sku")).isEqualTo("B");
        assertChangedFields(restored, List.of("customer_city", "second_sku", "document_json"), List.of("amount", "updated_at"));
    }

    @ParameterizedTest
    @CsvSource({ "c,false,true", "r,false,true", "u,false,true", "d,true,false", "d,false,false" })
    void shouldKeepLifecycleEventsWithEmptyChangeHeaders(String operation, boolean hasBefore, boolean hasAfter) {
        for (boolean fixedProjection : List.of(false, true)) {
            mapper.configure(fixedProjection ? Map.of("schema.mapping.shop.orders", PROJECTION) : Map.of());
            final var original = record(operation, hasBefore ? "{\"_id\":1}" : null, hasAfter ? "{\"_id\":1}" : null);
            final var result = changes.apply(mapper.apply(original));
            final var envelope = (Struct) result.value();
            assertChangedFields(result, List.of(), List.of());
            assertThat(envelope.getStruct("before") != null).isEqualTo(hasBefore);
            assertThat(envelope.getStruct("after") != null).isEqualTo(hasAfter);
            assertThat(envelope.schema().field("before").schema().isOptional()).isTrue();
            assertThat(envelope.schema().field("after").schema().isOptional()).isTrue();
            assertPreservedMetadata(original, result);
        }
    }

    @Test
    void shouldPassTombstonesThroughBothTransformations() {
        final var original = record("d", null, null);
        final var tombstone = original.newRecord(original.topic(), original.kafkaPartition(), original.keySchema(), original.key(), null, null,
                original.timestamp(), original.headers());
        assertThat(changes.apply(mapper.apply(tombstone))).isSameAs(tombstone);
        assertThat(tombstone.headers().lastWithName("Changed")).isNull();
        assertThat(tombstone.key()).isEqualTo(original.key());
        assertThat(tombstone.headers()).containsExactlyElementsOf(original.headers());
    }

    @ParameterizedTest
    @ValueSource(strings = { "c", "r", "u" })
    void shouldRejectMissingFullAfterDocuments(String operation) {
        for (boolean fixedProjection : List.of(false, true)) {
            mapper.configure(fixedProjection ? Map.of("schema.mapping.shop.orders", PROJECTION) : Map.of());
            assertThatThrownBy(() -> mapper.apply(record(operation, null, null)))
                    .isInstanceOf(DataException.class)
                    .hasMessageContaining("full after document")
                    .hasMessageContaining("change_streams_update_full");
        }
    }

    @Test
    void shouldFallBackToInferenceForAnUnmappedCollection() {
        mapper.configure(Map.of("schema.mapping.shop.other", PROJECTION));
        final var result = changes.apply(mapper.apply(record("u", "{\"_id\":1,\"removed\":42}", "{\"_id\":1}")));
        assertChangedFields(result, List.of("removed"), List.of("_id"));
        assertThat(((Struct) result.value()).getStruct("after").schema().field("document_json")).isNull();
    }

    @Test
    void shouldApplyTheSinkChainAfterSchemaEnabledJsonDeserialization() {
        final var original = record("u", BEFORE, AFTER);
        try (var keyConverter = new JsonConverter();
                var valueConverter = new JsonConverter();
                var sinkMapper = new MongoToRelationalMapper<SinkRecord>();
                var sinkChanges = new ExtractChangedRecordState<SinkRecord>()) {
            keyConverter.configure(Map.of("schemas.enable", true), true);
            valueConverter.configure(Map.of("schemas.enable", true), false);
            sinkMapper.configure(Map.of("schema.mapping.shop.orders", PROJECTION));
            sinkChanges.configure(Map.of("header.changed.name", "Changed", "header.unchanged.name", "Unchanged"));

            final var key = keyConverter.toConnectData(original.topic(), keyConverter.fromConnectData(original.topic(), original.keySchema(), original.key()));
            final var value = valueConverter.toConnectData(original.topic(),
                    valueConverter.fromConnectData(original.topic(), original.valueSchema(), original.value()));
            assertThat(((Struct) value.value()).getString("before")).isEqualTo(BEFORE);
            assertThat(((Struct) value.value()).getString("after")).isEqualTo(AFTER);
            final var input = new SinkRecord(original.topic(), original.kafkaPartition(), key.schema(), key.value(), value.schema(), value.value(), 123L);
            final var result = sinkChanges.apply(sinkMapper.apply(input));
            assertThat(result.kafkaOffset()).isEqualTo(123L);
            assertThat(result.keySchema()).isEqualTo(original.keySchema());
            assertThat(result.key()).isEqualTo(original.key());
            assertChangedFields(result, List.of("customer_city", "second_sku", "document_json"), List.of("amount", "updated_at"));

            final var restored = valueConverter.toConnectData(result.topic(),
                    valueConverter.fromConnectData(result.topic(), result.valueSchema(), result.value()));
            assertThat(restored.schema()).isEqualTo(result.valueSchema());
            assertThat(restored.value()).isEqualTo(result.value());
            assertProjection((Struct) restored.value());
        }
    }

    @Test
    void shouldRetainInferredMissingFieldsThroughSchemaEnabledJson() {
        final var result = mapper.apply(record("u", "{\"_id\":1,\"details\":{\"removed\":42}}", "{\"_id\":1,\"details\":{}}"));
        try (var converter = new JsonConverter()) {
            converter.configure(Map.of("schemas.enable", true), false);
            final var restored = converter.toConnectData(result.topic(), converter.fromConnectData(result.topic(), result.valueSchema(), result.value()));
            assertThat(restored.schema()).isEqualTo(result.valueSchema());
            assertThat(restored.value()).isEqualTo(result.value());
            final var envelope = (Struct) restored.value();
            assertThat(envelope.getStruct("before").getStruct("details").getInt32("removed")).isEqualTo(42);
            assertThat(envelope.getStruct("after").getStruct("details").get("removed")).isNull();
            final var deserializedRecord = result.newRecord(result.topic(), result.kafkaPartition(), result.keySchema(), result.key(), restored.schema(),
                    restored.value(), result.timestamp());
            assertChangedFields(changes.apply(deserializedRecord), List.of("details"), List.of("_id"));
        }
    }

    private static void assertProjection(Struct envelope) {
        final var before = envelope.getStruct("before");
        final var after = envelope.getStruct("after");
        assertThat(before.schema()).isEqualTo(after.schema());
        assertThat(after.schema().fields()).hasSize(5);
        assertThat(before.getString("customer_city")).isEqualTo("Seoul");
        assertThat(before.getString("second_sku")).isEqualTo("B");
        assertThat(after.get("customer_city")).isNull();
        assertThat(after.get("second_sku")).isNull();
        assertThat(after.get("amount")).isEqualTo(new BigDecimal("12.34"));
        assertThat(after.get("updated_at")).isEqualTo(new Date(1767225600000L));
        assertThat(before.getString("document_json")).isEqualTo(BEFORE);
        assertThat(after.getString("document_json")).isEqualTo(AFTER);
        assertThat(after.schema().field("document_json").schema().name()).isEqualTo(Json.LOGICAL_NAME);
    }

    private static void assertChangedFields(ConnectRecord<?> record, List<String> changed, List<String> unchanged) {
        assertThat(record.headers().lastWithName("Changed").value()).isEqualTo(changed);
        assertThat(record.headers().lastWithName("Unchanged").value()).isEqualTo(unchanged);
    }

    private static void assertPreservedMetadata(SourceRecord original, SourceRecord result) {
        assertThat(result.topic()).isEqualTo(original.topic());
        assertThat(result.kafkaPartition()).isEqualTo(original.kafkaPartition());
        assertThat(result.timestamp()).isEqualTo(original.timestamp());
        assertThat(result.sourcePartition()).isEqualTo(original.sourcePartition());
        assertThat(result.sourceOffset()).isEqualTo(original.sourceOffset());
        assertThat(result.keySchema()).isSameAs(original.keySchema());
        assertThat(result.key()).isSameAs(original.key());
        assertThat(result.headers()).containsAll(original.headers());
        assertThat(result.headers()).filteredOn(header -> header.key().equals("trace"))
                .extracting(Header::value).containsExactly("first", "second");
        assertThat(result.headers().lastWithName("attempt").value()).isEqualTo(1);
        assertThat(result.headers().lastWithName("attempt").schema()).isEqualTo(Schema.INT32_SCHEMA);
        assertThat(result.valueSchema().name()).isEqualTo(original.valueSchema().name());
        assertThat(result.valueSchema().version()).isEqualTo(original.valueSchema().version());
        assertThat(result.valueSchema().doc()).isEqualTo(original.valueSchema().doc());
        assertThat(result.valueSchema().parameters()).isEqualTo(original.valueSchema().parameters());
        final var envelope = (Struct) result.value();
        for (String field : List.of("source", "op", "ts_ms", "ts_us", "ts_ns", "transaction", "updateDescription", "custom_metadata")) {
            assertThat(envelope.get(field)).as(field).isEqualTo(((Struct) original.value()).get(field));
            assertThat(envelope.schema().field(field).schema()).as(field).isEqualTo(original.valueSchema().field(field).schema());
        }
        envelope.validate();
    }

    private static SourceRecord record(String operation, String before, String after) {
        final var sourceSchema = SchemaBuilder.struct().name("server.Source")
                .field("db", Schema.STRING_SCHEMA).field("collection", Schema.STRING_SCHEMA).build();
        final var envelopeSchema = SchemaBuilder.struct().name("server.shop.orders.Envelope").version(1)
                .doc("MongoDB event with additional envelope metadata").parameter("custom", "retained")
                .field("before", Json.builder().optional().build()).field("after", Json.builder().optional().build())
                .field("source", sourceSchema).field("op", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA).field("ts_us", Schema.INT64_SCHEMA).field("ts_ns", Schema.INT64_SCHEMA)
                .field("transaction", TransactionMonitor.TRANSACTION_BLOCK_SCHEMA)
                .field("updateDescription", MongoDbSchema.UPDATED_DESCRIPTION_SCHEMA)
                .field("custom_metadata", Schema.OPTIONAL_STRING_SCHEMA).build();
        final var transaction = new Struct(TransactionMonitor.TRANSACTION_BLOCK_SCHEMA)
                .put("id", "transaction-1").put("total_order", 1L).put("data_collection_order", 1L);
        final var updateDescription = new Struct(MongoDbSchema.UPDATED_DESCRIPTION_SCHEMA)
                .put("removedFields", List.of("customer.address.city"))
                .put("updatedFields", "{}")
                .put("truncatedArrays", List.of(new Struct(MongoDbSchema.TRUNCATED_ARRAY_SCHEMA)
                        .put(MongoDbFieldName.ARRAY_FIELD_NAME, "items").put(MongoDbFieldName.ARRAY_NEW_SIZE, 1)));
        final var envelope = new Struct(envelopeSchema).put("before", before).put("after", after)
                .put("source", new Struct(sourceSchema).put("db", "shop").put("collection", "orders"))
                .put("op", operation).put("ts_ms", 123L).put("ts_us", 123000L).put("ts_ns", 123000000L)
                .put("transaction", transaction).put("updateDescription", updateDescription).put("custom_metadata", "preserved");
        final var keySchema = SchemaBuilder.struct().name("server.shop.orders.Key").field("id", Schema.STRING_SCHEMA).build();
        final var key = new Struct(keySchema).put("id", "1");
        // Topic routing must not change which collection projection is selected.
        final var record = new SourceRecord(Map.of("server", "server"), Map.of("resume_token", "token-1"), "routed.orders", 2,
                keySchema, key, envelopeSchema, envelope, 456L);
        record.headers().addString("trace", "first").addString("trace", "second").addInt("attempt", 1);
        return record;
    }
}
