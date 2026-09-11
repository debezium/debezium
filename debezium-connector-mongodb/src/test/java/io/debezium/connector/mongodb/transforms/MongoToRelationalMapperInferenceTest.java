/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class MongoToRelationalMapperInferenceTest {

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
        final var sourceSchema = SchemaBuilder.struct().name("server.Source")
                .field("db", Schema.STRING_SCHEMA).field("collection", Schema.STRING_SCHEMA).build();
        final var envelopeSchema = SchemaBuilder.struct().name("server.db.collection.Envelope")
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
