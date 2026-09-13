/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.data.Envelope;

class MongoToRelationalMapperInferenceTest {

    @ParameterizedTest
    @ValueSource(longs = { 0L, 1L, 2_147_483_647L, 2_147_483_648L, 4_294_967_295L })
    void shouldInferUnsignedTimestampsInBothImages(long seconds) {
        final var before = new BsonDocument("value", new BsonTimestamp((int) seconds, 0)).toJson();
        final var after = new BsonDocument("value", new BsonTimestamp((int) seconds, -1)).toJson();
        final var result = transform(before, after);
        assertThat(result.getStruct("before").get("value")).isEqualTo(new Date(seconds * 1_000));
        assertThat(result.getStruct("after").get("value")).isEqualTo(new Date(seconds * 1_000));
    }

    @ParameterizedTest
    @EnumSource(value = BsonType.class, names = { "UNDEFINED", "DB_POINTER" })
    void shouldRejectUnsupportedTypesWithTheirPathInEitherImage(BsonType type) {
        final var value = type == BsonType.UNDEFINED ? "{\"$undefined\":true}"
                : "{\"$dbPointer\":{\"$ref\":\"db.collection\",\"$id\":{\"$oid\":\"507f1f77bcf86cd799439011\"}}}";
        final Map<String, String> documents = Map.of(
                "/value", "{\"value\":%s}",
                "/nested/value", "{\"nested\":{\"value\":%s}}",
                "/items/1", "{\"items\":[null,%s]}",
                "/items/1/value", "{\"items\":[{}, {\"value\":%s}]}",
                "/a~1b/~0c", "{\"a/b\":{\"~c\":%s}}",
                "/", "{\"\":%s}",
                "/script/$scope/value", "{\"script\":{\"$code\":\"return value;\",\"$scope\":{\"value\":%s}}}");
        documents.forEach((path, template) -> {
            final var document = template.formatted(value);
            for (var images : List.of(List.of(document, "{}"), List.of("{}", document), List.of(document, document))) {
                assertThatThrownBy(() -> transform(images.get(0), images.get(1)))
                        .isInstanceOf(DataException.class)
                        .hasMessageContaining("unsupported BSON type " + type)
                        .hasMessageContaining("'" + path + "'")
                        .hasMessageContaining("schema.mapping.<database>.<collection>")
                        .hasMessageContaining("io.debezium.data.Json");
            }
        });
    }

    @ParameterizedTest
    @ValueSource(strings = { "server.db.collection", "server.Envelope.collection", "server.db.Envelope" })
    void shouldRemoveOnlyTrailingEnvelopeSuffixFromPayloadSchemaName(String schemaName) {
        final var result = transform("{\"value\":1}", "{\"value\":2}", schemaName);
        assertThat(result.schema().name()).isEqualTo(schemaName + Envelope.SCHEMA_NAME_SUFFIX);
        assertThat(result.getStruct("before").schema().name()).isEqualTo(schemaName);
        assertThat(result.getStruct("after").schema()).isSameAs(result.getStruct("before").schema());
    }

    @Test
    void shouldInferReferenceDocumentsWithoutTreatingThemAsDbPointers() {
        final var result = transform("{}", """
                {"reference":{"$ref":"db.collection","$id":{"$oid":"507f1f77bcf86cd799439011"}}}
                """);
        final var reference = result.getStruct("after").getStruct("reference");
        assertThat(reference.getString("$ref")).isEqualTo("db.collection");
        assertThat(reference.getString("$id")).isEqualTo("507f1f77bcf86cd799439011");
    }

    @Test
    void shouldRetainAddedAndRemovedNestedFieldsInBothImages() {
        final var result = transform("""
                {"address":{"city":"Seoul","details":{"removed":42}}}
                """, """
                {"address":{"city":"Busan","details":{"added":true}}}
                """);
        final var before = result.getStruct("before").getStruct("address");
        final var after = result.getStruct("after").getStruct("address");
        assertThat(before.schema()).isSameAs(after.schema());
        assertThat(before.getString("city")).isEqualTo("Seoul");
        assertThat(after.getString("city")).isEqualTo("Busan");
        assertThat(before.getStruct("details").getInt32("removed")).isEqualTo(42);
        assertThat(after.getStruct("details").get("removed")).isNull();
        assertThat(before.getStruct("details").get("added")).isNull();
        assertThat(after.getStruct("details").getBoolean("added")).isTrue();
        assertThat(after.getStruct("details").schema().field("removed").schema().isOptional()).isTrue();
    }

    @ParameterizedTest
    @CsvSource(value = {
            "{\"nested\":{}}|{\"nested\":{\"value\":42}}",
            "{\"nested\":null}|{\"nested\":{\"value\":42}}",
            "{}|{\"nested\":{\"value\":42}}",
            "{\"nested\":{\"value\":null}}|{\"nested\":{\"value\":42}}"
    }, delimiter = '|')
    void shouldInferNestedTypesFromEitherImage(String empty, String populated) {
        for (boolean reverse : List.of(false, true)) {
            final var result = transform(reverse ? populated : empty, reverse ? empty : populated);
            final var populatedImage = result.getStruct(reverse ? "before" : "after");
            final var emptyImage = result.getStruct(reverse ? "after" : "before");
            assertThat(populatedImage.schema()).isSameAs(emptyImage.schema());
            assertThat(populatedImage.getStruct("nested").getInt32("value")).isEqualTo(42);
            if (emptyImage.getStruct("nested") != null) {
                assertThat(emptyImage.getStruct("nested").get("value")).isNull();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "[]", "[1]", "null" })
    void shouldRetainArrayElementTypeWhenArrayShrinks(String afterArray) {
        final var result = transform("{\"items\":[1,2]}", "{\"items\":" + afterArray + "}");
        final var before = result.getStruct("before");
        final var after = result.getStruct("after");
        assertThat(before.getArray("items")).containsExactly(1, 2);
        assertThat(after.schema()).isSameAs(before.schema());
        assertThat(after.schema().field("items").schema().valueSchema()).isEqualTo(Schema.OPTIONAL_INT32_SCHEMA);
        assertThat(after.<Integer> getArray("items")).isEqualTo(switch (afterArray) {
            case "[]" -> List.of();
            case "[1]" -> List.of(1);
            default -> null;
        });
    }

    @Test
    void shouldUnionNestedFieldsAcrossArrayElementsAndImages() {
        final var result = transform("""
                {"items":[{"details":{"removed":42}},{"details":{"other":true}}]}
                """, """
                {"items":[{"details":{"added":"new"}}]}
                """);
        final List<Struct> before = result.getStruct("before").getArray("items");
        final List<Struct> after = result.getStruct("after").getArray("items");
        assertThat(before).hasSize(2);
        assertThat(after).hasSize(1);
        assertThat(before.get(0).getStruct("details").getInt32("removed")).isEqualTo(42);
        assertThat(before.get(1).getStruct("details").getBoolean("other")).isTrue();
        assertThat(after.get(0).getStruct("details").getString("added")).isEqualTo("new");
        assertThat(after.get(0).getStruct("details").get("removed")).isNull();
        assertThat(after.get(0).getStruct("details").schema()).isSameAs(before.get(0).getStruct("details").schema());
    }

    @ParameterizedTest
    @CsvSource(value = {
            "{\"address\":{\"city\":\"Seoul\"}}|{\"address\":\"Busan\"}|/address",
            "{\"address\":\"Seoul\"}|{\"address\":{\"city\":\"Busan\"}}|/address",
            "{\"address\":[]}|{\"address\":{}}|/address",
            "{\"address\":{\"city\":42}}|{\"address\":{\"city\":\"Busan\"}}|/address/city",
            "{\"items\":[1]}|{\"items\":[\"text\"]}|/items/0",
            "{\"items\":[{\"details\":{}}]}|{\"items\":[{\"details\":false}]}|/items/0/details",
            "{\"a/b\":{\"~c\":[]}}|{\"a/b\":{\"~c\":true}}|/a~1b/~0c"
    }, delimiter = '|')
    void shouldReportConflictingTypesWithDocumentPath(String before, String after, String path) {
        assertThatThrownBy(() -> transform(before, after))
                .isInstanceOf(DataException.class)
                .hasMessageContaining(path)
                .hasMessageContaining("schema.mapping.")
                .hasMessageContaining("io.debezium.data.Json");
    }

    @Test
    void shouldPreserveCompatibleBsonTypesWithTheSameConnectRepresentation() {
        final var result = transform("{\"id\":{\"$oid\":\"507f1f77bcf86cd799439011\"}}", "{\"id\":\"text\"}");
        assertThat(result.getStruct("before").getString("id")).isEqualTo("507f1f77bcf86cd799439011");
        assertThat(result.getStruct("after").getString("id")).isEqualTo("text");
    }

    private static Struct transform(String before, String after) {
        return transform(before, after, "server.db.collection");
    }

    private static Struct transform(String before, String after, String schemaName) {
        final var sourceSchema = SchemaBuilder.struct().name("server.Source")
                .field("db", Schema.STRING_SCHEMA).field("collection", Schema.STRING_SCHEMA).build();
        final var envelopeSchema = SchemaBuilder.struct().name(schemaName + Envelope.SCHEMA_NAME_SUFFIX)
                .field("before", Schema.OPTIONAL_STRING_SCHEMA)
                .field("after", Schema.OPTIONAL_STRING_SCHEMA)
                .field("source", sourceSchema)
                .field("op", Schema.STRING_SCHEMA).build();
        final var envelope = new Struct(envelopeSchema)
                .put("before", before).put("after", after)
                .put("source", new Struct(sourceSchema).put("db", "db").put("collection", "collection"))
                .put("op", "u");
        final var record = new SourceRecord(Map.of(), Map.of(), "server.db.collection", envelopeSchema, envelope);
        try (var mapper = new MongoToRelationalMapper<SourceRecord>()) {
            mapper.configure(Map.of());
            final var result = (Struct) mapper.apply(record).value();
            result.validate();
            return result;
        }
    }
}
