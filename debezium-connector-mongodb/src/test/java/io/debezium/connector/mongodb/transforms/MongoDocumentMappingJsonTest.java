/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.mongodb.transforms.MongoToRelationalMapper.JsonOutputMode;

class MongoDocumentMappingJsonTest {

    private static final JsonWriterSettings CANONICAL = JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();
    private static final String MAPPING = """
            {"document_json":{"path":"","type":"io.debezium.data.Json"},
             "selected":{"path":"/v","type":"io.debezium.data.Json"},
             "alias":{"path":"/v","type":"io.debezium.data.Json"}}
            """;

    @ParameterizedTest
    @ValueSource(strings = {
            "{ \"age\" : 30, \"date\" : {\"$date\":0} }",
            "{\"age\":{\"$numberInt\":\"30\"},\"date\":{\"$date\":{\"$numberLong\":\"0\"}}}",
            "{\"age\":30,\"date\":{\"$date\":\"1970-01-01T00:00:00Z\"}}",
            "[ 1, true, {\"x\":\"y\"} ]", "[]", "{}", "true", "false", "null",
            "\"Seoul 서울 🙂 \\u0061 \\\" \\\\ /\"", "\"\"",
            "9223372036854775807", "-0.0", "1.234567890123456789", "1e+03",
            "NaN", "Infinity", "-Infinity", "{\"$numberDouble\":\"NaN\"}",
            "{\"$numberDecimal\":\"12345678901234567890.12345678901234\"}"
    })
    void shouldPreserveIncomingJsonWithoutReserializing(String selected) {
        final var json = " \n{\"v\":" + selected + ",\"tail\":false} \n";
        final var mapping = new MongoDocumentMapping("shop.orders", MAPPING);
        final var result = convert(mapping, json);
        final var expected = selected.equals("null") ? null : selected;
        assertThat(result.getString("document_json")).isEqualTo(json);
        assertThat(result.getString("selected")).isEqualTo(expected);
        assertThat(result.getString("alias")).isEqualTo(expected);
    }

    @ParameterizedTest
    @EnumSource(value = JsonMode.class, names = { "STRICT", "RELAXED", "EXTENDED" })
    void shouldApplyOneOutputPolicyToRootAndNestedBsonTypes(JsonMode inputMode) {
        final var document = MongoBsonTypeTestData.values();
        final var settings = JsonWriterSettings.builder().outputMode(inputMode).build();
        final var json = new BsonDocument("v", document).toJson(settings);
        final var input = convert(new MongoDocumentMapping("shop.orders", MAPPING, JsonOutputMode.INPUT), json);
        assertThat(input.getString("document_json")).isEqualTo(json);
        assertThat(input.getString("selected")).isEqualTo(document.toJson(settings));

        final var canonical = convert(new MongoDocumentMapping("shop.orders", MAPPING, JsonOutputMode.CANONICAL), json);
        final var parsed = BsonDocument.parse(json);
        assertThat(canonical.getString("document_json")).isEqualTo(parsed.toJson(CANONICAL));
        assertThat(canonical.getString("selected")).isEqualTo(parsed.getDocument("v").toJson(CANONICAL));
        assertThat(canonical.getString("alias")).isEqualTo(canonical.getString("selected"));
    }

    @Test
    void shouldCoverEveryStorableBsonType() {
        final var types = EnumSet.allOf(BsonType.class);
        types.remove(BsonType.END_OF_DOCUMENT);
        assertThat(MongoBsonTypeTestData.values().values()).extracting(BsonValue::getBsonType)
                .containsAll(types);
    }

    @ParameterizedTest
    @EnumSource(value = JsonMode.class, names = { "STRICT", "RELAXED", "EXTENDED" })
    void shouldProjectEveryBsonValueIndividually(JsonMode inputMode) {
        final var settings = JsonWriterSettings.builder().outputMode(inputMode).build();
        for (var entry : MongoBsonTypeTestData.values().entrySet()) {
            final var json = new BsonDocument("v", entry.getValue()).toJson(settings);
            final var parsed = BsonDocument.parse(json);
            for (JsonOutputMode outputMode : JsonOutputMode.values()) {
                final var result = convert(new MongoDocumentMapping("shop.orders", MAPPING, outputMode), json);
                final var expected = outputMode == JsonOutputMode.INPUT ? json : parsed.toJson(CANONICAL);
                assertThat(result.getString("document_json")).as("%s / %s / %s root", inputMode, outputMode, entry.getKey()).isEqualTo(expected);
                final var fragment = entry.getValue().isNull() ? null : expected.substring(6, expected.length() - 1);
                assertThat(result.getString("selected")).as("%s / %s / %s value", inputMode, outputMode, entry.getKey()).isEqualTo(fragment);
            }
        }
    }

    @ParameterizedTest
    @CsvSource(value = {
            "30|{\"$numberInt\": \"30\"}",
            "1e+03|{\"$numberDouble\": \"1000.0\"}",
            "NaN|{\"$numberDouble\": \"NaN\"}",
            "Infinity|{\"$numberDouble\": \"Infinity\"}",
            "-Infinity|{\"$numberDouble\": \"-Infinity\"}",
            "[1,true]|[{\"$numberInt\": \"1\"}, true]",
            "\"hello\"|\"hello\""
    }, delimiter = '|')
    void shouldCanonicalizeScalarAndArrayValues(String value, String expected) {
        final var json = "{\"v\":" + value + "}";
        final var result = convert(new MongoDocumentMapping("shop.orders", MAPPING, JsonOutputMode.CANONICAL), json);
        assertThat(result.getString("selected")).isEqualTo(expected);
        assertThat(result.getString("document_json")).isEqualTo("{\"v\": " + expected + "}");
    }

    @ParameterizedTest
    @CsvSource(value = {
            "/a~1b|a/b", "/a~0b|a~b", "/~01|~1", "/a.b|a.b", "/a,b|a,b", "/a:b|a:b", "/a=b|a=b",
            "/$price|$price", "/고객|고객", "/a\"b|a\"b", "/a\\b|a\\b", "/|''"
    }, delimiter = '|')
    void shouldPreserveJsonSelectedThroughEscapedAndLiteralPaths(String pointer, String field) throws JsonProcessingException {
        final var mapper = new ObjectMapper();
        final var mapping = new MongoDocumentMapping("shop.orders",
                mapper.writeValueAsString(Map.of("selected", Map.of("path", pointer, "type", "io.debezium.data.Json"))));
        final var json = "{\"prefix\":\"🙂\"," + mapper.writeValueAsString(field) + ":[ 1, 2 ]}";
        assertThat(convert(mapping, json).getString("selected")).isEqualTo("[ 1, 2 ]");
    }

    @ParameterizedTest
    @EnumSource(JsonOutputMode.class)
    void shouldPreserveBsonPathAndNullSemantics(JsonOutputMode mode) {
        final var mapping = new MongoDocumentMapping("shop.orders", """
                {"id":{"path":"/_id","type":"io.debezium.data.Json"},
                 "wrapper":{"path":"/_id/$oid","type":"io.debezium.data.Json"},
                 "date_wrapper":{"path":"/date/$date","type":"io.debezium.data.Json"},
                 "literal":{"path":"/$price","type":"io.debezium.data.Json"},
                 "nested":{"path":"/items/0/sku","type":"io.debezium.data.Json"},
                 "leading_zero":{"path":"/items/01","type":"io.debezium.data.Json"},
                 "overflow":{"path":"/items/999999999999999999999","type":"io.debezium.data.Json"},
                 "out_of_range":{"path":"/items/2","type":"io.debezium.data.Json"},
                 "absent":{"path":"/absent","type":"io.debezium.data.Json"},
                 "null_value":{"path":"/nil","type":"io.debezium.data.Json"}}
                """, mode);
        final var result = convert(mapping, """
                {"_id":{"$oid":"507f1f77bcf86cd799439011"},"date":{"$date":0},"$price":"literal",
                 "items":[{"sku":"first"},"second"],"nil":null}
                """);
        assertThat(BsonDocument.parse("{\"v\":" + result.getString("id") + "}").getObjectId("v").getValue().toHexString())
                .isEqualTo("507f1f77bcf86cd799439011");
        assertThat(result.getString("literal")).isEqualTo("\"literal\"");
        assertThat(result.getString("nested")).isEqualTo("\"first\"");
        for (String field : List.of("wrapper", "date_wrapper", "leading_zero", "overflow", "out_of_range", "absent", "null_value")) {
            assertThat(result.get(field)).as(field).isNull();
        }
        final var object = convert(mapping, "{\"items\":{\"0\":{\"sku\":\"object\"},\"01\":\"literal key\"}}");
        assertThat(object.getString("nested")).isEqualTo("\"object\"");
        assertThat(object.getString("leading_zero")).isEqualTo("\"literal key\"");
        assertThat(convert(mapping, "{\"items\":\"scalar\"}").get("nested")).isNull();
        assertThat(mapping.convert(null, null)).isNull();
    }

    @Test
    void shouldExtractOverlappingPathsAndUseTheLastDuplicateField() {
        final var mapping = new MongoDocumentMapping("shop.orders", """
                {"parent":{"path":"/items","type":"io.debezium.data.Json"},
                 "child":{"path":"/items/0","type":"io.debezium.data.Json"},
                 "leaf":{"path":"/items/0/n","type":"io.debezium.data.Json"}}
                """);
        final var result = convert(mapping, "{\"items\":[0],\"items\":[ {\"n\":1,\"n\":2}, [] ]}");
        assertThat(result.getString("parent")).isEqualTo("[ {\"n\":1,\"n\":2}, [] ]");
        assertThat(result.getString("child")).isEqualTo("{\"n\":1,\"n\":2}");
        assertThat(result.getString("leaf")).isEqualTo("2");
    }

    @Test
    void shouldPreserveFragmentsAcrossParserBuffers() {
        final var selected = "{\"text\":\"" + "서울 🙂 \\u0061 ".repeat(2_000) + "\",\"n\":1e+03}";
        final var json = "{\"prefix\":\"" + "🙂".repeat(5_000) + "\",\"v\":" + selected + "}";
        final var result = convert(new MongoDocumentMapping("shop.orders", MAPPING), json);
        assertThat(result.getString("document_json")).isEqualTo(json);
        assertThat(result.getString("selected")).isEqualTo(selected);
        assertThat(result.getString("alias")).isEqualTo(selected);
    }

    @Test
    void shouldRejectTrailingContentWhenExtractingJson() {
        final var mapping = new MongoDocumentMapping("shop.orders", MAPPING);
        assertThatThrownBy(() -> convert(mapping, "{\"v\":1} {\"v\":2}"))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("selected")
                .hasRootCauseMessage("Unexpected trailing content in the incoming MongoDB document JSON");
    }

    private static Struct convert(MongoDocumentMapping mapping, String json) {
        final var result = mapping.convert(BsonDocument.parse(json), json);
        result.validate();
        return result;
    }
}
