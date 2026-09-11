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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

class MongoDocumentMappingTest {

    @Test
    void shouldProjectBeforeConvertingUnselectedValues() {
        final var mapping = new MongoDocumentMapping("shop.orders", """
                {"city":{"path":"/customer/address/city","type":"string"},
                 "literal_city":{"path":"/customer.address.city","type":"string"},
                 "second_sku":{"path":"/items/1/sku","type":"string"}}
                """);
        final var first = convert(mapping, """
                {"customer":{"address":{"city":"Seoul"}},"customer.address.city":"Busan",
                 "items":[{"sku":"A"},{"sku":"B"}],"unselected":[1,"text",{"nested":[false,2]}]}
                """);
        assertThat(first.getString("city")).isEqualTo("Seoul");
        assertThat(first.getString("literal_city")).isEqualTo("Busan");
        assertThat(first.getString("second_sku")).isEqualTo("B");

        final var shrunk = convert(mapping, """
                {"customer":"changed to scalar","items":[{"sku":"A2"}],"unselected":[1,"text"]}
                """);
        assertThat(shrunk.schema()).isSameAs(first.schema());
        assertThat(shrunk.schema().fields()).hasSize(3);
        assertThat(shrunk.get("city")).isNull();
        assertThat(shrunk.get("literal_city")).isNull();
        assertThat(shrunk.get("second_sku")).isNull();

        final var restored = convert(mapping, """
                {"customer":{"address":{"city":"Incheon"}},"items":[{"sku":"A"},{"sku":"C"}]}
                """);
        assertThat(restored.schema()).isSameAs(first.schema());
        assertThat(restored.getString("city")).isEqualTo("Incheon");
        assertThat(restored.getString("second_sku")).isEqualTo("C");
    }

    @ParameterizedTest
    @CsvSource(value = {
            "/a~1b|a/b", "/a~0b|a~b", "/~01|~1", "/a.b|a.b", "/a,b|a,b", "/a:b|a:b", "/a=b|a=b",
            "/$price|$price", "/고객|고객", "/a\"b|a\"b", "/a\\b|a\\b"
    }, delimiter = '|')
    void shouldResolveLiteralFieldNames(String pointer, String field) throws JsonProcessingException {
        final var mapper = new ObjectMapper();
        final var mapping = new MongoDocumentMapping("shop.orders", mapper.writeValueAsString(Map.of("selected", Map.of("path", pointer, "type", "string"))));
        assertThat(convert(mapping, mapper.writeValueAsString(Map.of(field, "found"))).getString("selected")).isEqualTo("found");
    }

    @Test
    void shouldDistinguishEmptyFieldFromRootAndRetainOriginalJson() {
        final String json = " {\"\":\"empty field\", \"_id\":{\"$oid\":\"507f1f77bcf86cd799439011\"},\"mixed\":[1,true]} ";
        final var mapping = new MongoDocumentMapping("shop.orders", """
                {"empty":{"path":"/","type":"string"},
                 "document_json":{"path":"","type":"io.debezium.data.Json"},
                 "id":{"path":"/_id","type":"string"},
                 "wrapper":{"path":"/_id/$oid","type":"string"}}
                """);
        final var result = convert(mapping, json);
        assertThat(result.getString("empty")).isEqualTo("empty field");
        assertThat(result.getString("document_json")).isEqualTo(json);
        assertThat(result.getString("id")).isEqualTo("507f1f77bcf86cd799439011");
        assertThat(result.get("wrapper")).isNull();
    }

    @Test
    void shouldResolveNumericObjectKeysAndArrayIndicesByContainerType() {
        final var mapping = new MongoDocumentMapping("shop.orders", """
                {"sku":{"path":"/items/0/sku","type":"string"}}
                """);
        assertThat(convert(mapping, "{\"items\":[{\"sku\":\"array\"}]}").getString("sku")).isEqualTo("array");
        assertThat(convert(mapping, "{\"items\":{\"0\":{\"sku\":\"object\"}}}").getString("sku")).isEqualTo("object");
    }

    @ParameterizedTest
    @ValueSource(strings = { "/items/01", "/items/-1", "/items/-", "/items/*", "/items/999999999999999999999", "/items/2", "/absent/a" })
    void shouldReturnNullForUnresolvedPaths(String path) throws JsonProcessingException {
        final var mapping = new MongoDocumentMapping("shop.orders", new ObjectMapper().writeValueAsString(Map.of("selected", Map.of("path", path, "type", "string"))));
        assertThat(convert(mapping, "{\"items\":[\"first\"]}").get("selected")).isNull();
    }

    @Test
    void shouldHandleMissingAndNullWithoutDroppingOutputFields() {
        final var mapping = new MongoDocumentMapping("shop.orders", """
                {"selected":{"path":"/a/b","type":"int64"}}
                """);
        for (String json : List.of("{}", "{\"a\":null}", "{\"a\":{\"b\":null}}", "{\"a\":5}")) {
            final var result = convert(mapping, json);
            assertThat(result.schema().fields()).hasSize(1);
            assertThat(result.schema().field("selected").schema().isOptional()).isTrue();
            assertThat(result.get("selected")).isNull();
        }
        assertThat(mapping.convert(null, null)).isNull();
    }

    @Test
    void shouldRejectTypeMismatchAtSelectedLeaf() {
        final var mapping = new MongoDocumentMapping("shop.orders", """
                {"selected":{"path":"/a","type":"string"}}
                """);
        assertThatThrownBy(() -> convert(mapping, "{\"a\":{\"nested\":5}}"))
                .isInstanceOf(DataException.class).hasMessageContaining("selected");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "", "null", "[]", "{}", "{", "{} {}",
            "{\"a\":{\"path\":\"/x\",\"path\":\"/y\",\"type\":\"string\"}}",
            "{\"a\":{\"path\":\"/x\",\"type\":\"string\"},\"a\":{\"path\":\"/y\",\"type\":\"string\"}}",
            "{\"a\":\"string\"}", "{\"a\":{\"type\":\"string\"}}", "{\"a\":{\"path\":\"/x\"}}",
            "{\"a\":{\"path\":null,\"type\":\"string\"}}", "{\"a\":{\"path\":\"x\",\"type\":\"string\"}}",
            "{\"a\":{\"path\":\"/~\",\"type\":\"string\"}}", "{\"a\":{\"path\":\"/~2\",\"type\":\"string\"}}",
            "{\"a\":{\"path\":\"/x\",\"type\":\"typo\"}}", "{\"a\":{\"path\":\"/x\",\"type\":\"string\",\"typo\":1}}",
            "{\"a.b\":{\"path\":\"/x\",\"type\":\"string\"}}", "{\"1a\":{\"path\":\"/x\",\"type\":\"string\"}}",
            "{\"a\":{\"path\":\"/x\",\"type\":\"string\",\"scale\":2}}",
            "{\"a\":{\"path\":\"/x\",\"type\":\"string\",\"length\":2}}",
            "{\"a\":{\"path\":\"/x\",\"type\":\"org.apache.kafka.connect.data.Decimal\"}}",
            "{\"a\":{\"path\":\"/x\",\"type\":\"org.apache.kafka.connect.data.Decimal\",\"scale\":-1}}",
            "{\"a\":{\"path\":\"/x\",\"type\":\"org.apache.kafka.connect.data.Decimal\",\"scale\":\"2\"}}",
            "{\"a\":{\"path\":\"/x\",\"type\":\"org.apache.kafka.connect.data.Decimal\",\"scale\":2147483648}}",
            "{\"a\":{\"path\":\"/x\",\"type\":\"org.apache.kafka.connect.data.Decimal\",\"scale\":2,\"precision\":1}}",
            "{\"a\":{\"path\":\"/x\",\"type\":\"io.debezium.data.Bits\"}}",
            "{\"a\":{\"path\":\"/x\",\"type\":\"io.debezium.data.Bits\",\"length\":0}}"
    })
    void shouldRejectInvalidMappingsAtConfigurationTime(String mapping) {
        assertThatThrownBy(() -> new MongoDocumentMapping("shop.orders", mapping)).isInstanceOf(ConfigException.class);
    }

    private static Struct convert(MongoDocumentMapping mapping, String json) {
        final var result = mapping.convert(BsonDocument.parse(json), json);
        result.validate();
        return result;
    }
}
