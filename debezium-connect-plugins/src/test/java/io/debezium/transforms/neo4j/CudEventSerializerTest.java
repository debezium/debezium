/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.neo4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.data.VariableScaleDecimal;
import io.debezium.transforms.neo4j.CudEvent.Operation;

class CudEventSerializerTest {

    private CudEventSerializer serializer;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        serializer = new CudEventSerializer();
    }

    @Nested
    @DisplayName("Node event serialization")
    class NodeEvents {

        @Test
        @DisplayName("Serializes a merge node event with labels, ids and properties")
        void serializeMergeNode() throws Exception {
            final var event = new CudNodeEvent(Operation.MERGE,
                    List.of("Customer"),
                    Map.of("id", 1004L),
                    Map.of("first_name", "John", "email", "john@foo.org"),
                    null);

            final var json = serializer.serializeSingle(event);

            final var parsed = parseJson(json);
            assertThat(parsed.get("type")).isEqualTo("node");
            assertThat(parsed.get("op")).isEqualTo("merge");
            assertThat(parsed.get("labels")).isEqualTo(List.of("Customer"));
            assertThat(asMap(parsed, "ids")).containsEntry("id", 1004);
            assertThat(asMap(parsed, "properties")).containsEntry("first_name", "John");
            assertThat(parsed).doesNotContainKey("detach");
        }

        @Test
        @DisplayName("Serializes a delete node event with detach flag and no properties")
        void serializeDeleteNode() throws Exception {
            final var event = new CudNodeEvent(Operation.DELETE,
                    List.of("Customer"),
                    Map.of("id", 1004L),
                    null,
                    true);

            final var json = serializer.serializeSingle(event);

            final var parsed = parseJson(json);
            assertThat(parsed.get("op")).isEqualTo("delete");
            assertThat(parsed.get("detach")).isEqualTo(true);
            assertThat(parsed).doesNotContainKey("properties");
        }

        @Test
        @DisplayName("Serializes multiple labels")
        void serializeMultipleLabels() throws Exception {
            final var event = new CudNodeEvent(Operation.MERGE,
                    List.of("Person", "Employee"),
                    Map.of("id", 1L),
                    Collections.emptyMap(),
                    null);

            final var json = serializer.serializeSingle(event);

            final var parsed = parseJson(json);
            assertThat(parsed.get("labels")).isEqualTo(List.of("Person", "Employee"));
        }
    }

    @Nested
    @DisplayName("Relationship event serialization")
    class RelationshipEvents {

        @Test
        @DisplayName("Serializes a merge relationship with from/to endpoints")
        void serializeMergeRelationship() throws Exception {
            final var from = new CudRelationshipEvent.Endpoint(
                    List.of("Order"), Map.of("id", 5001L), Operation.MERGE);
            final var to = new CudRelationshipEvent.Endpoint(
                    List.of("Customer"), Map.of("id", 1004L), Operation.MATCH);
            final var event = new CudRelationshipEvent(Operation.MERGE, "PLACED_BY", from, to, Collections.emptyMap());

            final var json = serializer.serializeSingle(event);

            final var parsed = parseJson(json);
            assertThat(parsed.get("type")).isEqualTo("relationship");
            assertThat(parsed.get("rel_type")).isEqualTo("PLACED_BY");
            assertThat(asMap(parsed, "from")).containsEntry("op", "merge");
            assertThat(asMap(parsed, "to")).containsEntry("op", "match");
        }
    }

    @Nested
    @DisplayName("Array serialization")
    class ArrayEvents {

        @Test
        @DisplayName("Serializes a list of events as a JSON array")
        void serializeArray() throws Exception {
            final var nodeEvent = new CudNodeEvent(Operation.MERGE,
                    List.of("Order"), Map.of("id", 5001L),
                    Map.of("total", 99.95), null);
            final var from = new CudRelationshipEvent.Endpoint(
                    List.of("Order"), Map.of("id", 5001L), Operation.MERGE);
            final var to = new CudRelationshipEvent.Endpoint(
                    List.of("Customer"), Map.of("id", 1004L), Operation.MATCH);
            final var relEvent = new CudRelationshipEvent(Operation.MERGE, "PLACED_BY",
                    from, to, Collections.emptyMap());

            final var json = serializer.serializeArray(List.of(nodeEvent, relEvent));

            final var parsed = objectMapper.readValue(json, List.class);
            assertThat(parsed).hasSize(2);
            assertThat(((Map<?, ?>) parsed.get(0)).get("type")).isEqualTo("node");
            assertThat(((Map<?, ?>) parsed.get(1)).get("type")).isEqualTo("relationship");
        }
    }

    @Nested
    @DisplayName("Type conversion")
    class TypeConversion {

        @Test
        @DisplayName("Converts Byte to Long")
        void convertByte() {
            assertThat(serializer.toValue((byte) 42)).isEqualTo(42L);
        }

        @Test
        @DisplayName("Converts Short to Long")
        void convertShort() {
            assertThat(serializer.toValue((short) 256)).isEqualTo(256L);
        }

        @Test
        @DisplayName("Converts Integer to Long")
        void convertInteger() {
            assertThat(serializer.toValue(100)).isEqualTo(100L);
        }

        @Test
        @DisplayName("Converts Float to Double")
        void convertFloat() {
            assertThat(serializer.toValue(3.14f)).isEqualTo(3.140000104904175);
        }

        @Test
        @DisplayName("Converts BigDecimal to Double")
        void convertBigDecimal() {
            assertThat(serializer.toValue(new BigDecimal("99.95"))).isEqualTo(99.95);
        }

        @Test
        @DisplayName("Passes Long through unchanged")
        void passLong() {
            assertThat(serializer.toValue(42L)).isEqualTo(42L);
        }

        @Test
        @DisplayName("Passes String through unchanged")
        void passString() {
            assertThat(serializer.toValue("hello")).isEqualTo("hello");
        }

        @Test
        @DisplayName("Passes Boolean through unchanged")
        void passBoolean() {
            assertThat(serializer.toValue(true)).isEqualTo(true);
        }

        @Test
        @DisplayName("Returns null for null input")
        void passNull() {
            assertThat(serializer.toValue(null)).isNull();
        }

        @Test
        @DisplayName("Rejects VariableScaleDecimal with error recommending decimal.handling.mode config")
        void rejectsVariableScaleDecimal() {
            final var struct = VariableScaleDecimal.fromLogical(VariableScaleDecimal.schema(), new BigDecimal("123.456"));

            assertThatThrownBy(() -> serializer.toValue(struct))
                    .isInstanceOf(ConnectException.class)
                    .hasMessageContaining("VariableScaleDecimal")
                    .hasMessageContaining("decimal.handling.mode");
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseJson(String json) throws JsonProcessingException {
        return objectMapper.readValue(json, Map.class);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> asMap(Map<String, Object> parent, String key) {
        return (Map<String, Object>) parent.get(key);
    }
}
