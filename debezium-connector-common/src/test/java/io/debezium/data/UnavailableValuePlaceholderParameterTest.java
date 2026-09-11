/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link UnavailableValuePlaceholderParameter}.
 *
 * @author Sundong Kim
 */
public class UnavailableValuePlaceholderParameterTest {

    private static final String PLACEHOLDER = "__debezium_unavailable_value";
    private static final byte[] PLACEHOLDER_BYTES = PLACEHOLDER.getBytes(StandardCharsets.UTF_8);

    @Test
    public void shouldSerializeString() {
        assertThat(UnavailableValuePlaceholderParameter.serialize(Schema.OPTIONAL_STRING_SCHEMA, PLACEHOLDER)).isEqualTo(PLACEHOLDER);
    }

    @Test
    public void shouldSerializeBytesAndByteBufferIdentically() {
        final String expected = Base64.getEncoder().encodeToString(PLACEHOLDER_BYTES);
        assertThat(UnavailableValuePlaceholderParameter.serialize(Schema.OPTIONAL_BYTES_SCHEMA, PLACEHOLDER_BYTES)).isEqualTo(expected);
        final ByteBuffer buffer = ByteBuffer.wrap(PLACEHOLDER_BYTES);
        assertThat(UnavailableValuePlaceholderParameter.serialize(Schema.OPTIONAL_BYTES_SCHEMA, buffer)).isEqualTo(expected);
        // serializing must not consume the buffer
        assertThat(buffer.remaining()).isEqualTo(PLACEHOLDER_BYTES.length);
    }

    @Test
    public void shouldSerializeIntegers() {
        assertThat(UnavailableValuePlaceholderParameter.serialize(Schema.OPTIONAL_INT32_SCHEMA, 95)).isEqualTo("95");
        assertThat(UnavailableValuePlaceholderParameter.serialize(Schema.OPTIONAL_INT64_SCHEMA, 95L)).isEqualTo("95");
    }

    @Test
    public void shouldSerializeArrays() {
        final Schema strings = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build();
        assertThat(UnavailableValuePlaceholderParameter.serialize(strings, List.of(PLACEHOLDER)))
                .isEqualTo("[\"" + PLACEHOLDER + "\"]");
        final Schema ints = SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).build();
        assertThat(UnavailableValuePlaceholderParameter.serialize(ints, List.of(9, 5))).isEqualTo("[9,5]");
    }

    @Test
    public void shouldDistinguishElementBoundariesInStringArrays() {
        final Schema strings = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build();
        final String joined = UnavailableValuePlaceholderParameter.serialize(strings, List.of("a,b"));
        final String split = UnavailableValuePlaceholderParameter.serialize(strings, List.of("a", "b"));
        assertThat(joined).isNotEqualTo(split);
        final String quoted = UnavailableValuePlaceholderParameter.serialize(strings, List.of("a\"b\\c"));
        assertThat(quoted).isEqualTo("[\"a\\\"b\\\\c\"]");
    }

    @Test
    public void shouldSerializeMaps() {
        final Schema map = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).build();
        assertThat(UnavailableValuePlaceholderParameter.serialize(map, Map.of(PLACEHOLDER, PLACEHOLDER)))
                .isEqualTo("{\"" + PLACEHOLDER + "\":\"" + PLACEHOLDER + "\"}");
    }

    @Test
    public void shouldMatchOnlyTheDeclaredPlaceholder() {
        final String declared = UnavailableValuePlaceholderParameter.serialize(Schema.OPTIONAL_STRING_SCHEMA, PLACEHOLDER);
        assertThat(UnavailableValuePlaceholderParameter.matches(Schema.OPTIONAL_STRING_SCHEMA, PLACEHOLDER, declared)).isTrue();
        assertThat(UnavailableValuePlaceholderParameter.matches(Schema.OPTIONAL_STRING_SCHEMA, "other", declared)).isFalse();
    }

    @Test
    public void shouldMatchEveryTypeItCanSerialize() {
        final Schema strings = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build();
        final Schema map = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).build();
        final Map<Schema, Object> placeholders = new LinkedHashMap<>();
        placeholders.put(Schema.OPTIONAL_STRING_SCHEMA, PLACEHOLDER);
        placeholders.put(Schema.OPTIONAL_BYTES_SCHEMA, PLACEHOLDER_BYTES);
        placeholders.put(SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).build(), List.of(9, 5));
        placeholders.put(strings, List.of(PLACEHOLDER));
        placeholders.put(map, Map.of(PLACEHOLDER, PLACEHOLDER));

        placeholders.forEach((schema, placeholder) -> {
            final String declared = UnavailableValuePlaceholderParameter.serialize(schema, placeholder);
            assertThat(declared).isNotNull();
            assertThat(UnavailableValuePlaceholderParameter.matches(schema, placeholder, declared)).isTrue();
        });

        // A map that holds more than the declared single entry is not the placeholder
        final String declaredMap = UnavailableValuePlaceholderParameter.serialize(map, Map.of(PLACEHOLDER, PLACEHOLDER));
        assertThat(UnavailableValuePlaceholderParameter.matches(map, Map.of(PLACEHOLDER, PLACEHOLDER, "k", "v"), declaredMap)).isFalse();
    }

    @Test
    public void shouldRejectOversizedValuesWithoutSerializingThemInFull() {
        final Schema bytes = Schema.OPTIONAL_BYTES_SCHEMA;
        final String declared = UnavailableValuePlaceholderParameter.serialize(bytes, PLACEHOLDER_BYTES);
        final ByteBuffer oversized = ByteBuffer.wrap(new byte[10 * 1024 * 1024]);
        assertThat(UnavailableValuePlaceholderParameter.matches(bytes, oversized, declared)).isFalse();
        // the value was rejected on length alone, so it was never read
        assertThat(oversized.position()).isZero();

        final Schema strings = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build();
        final String declaredArray = UnavailableValuePlaceholderParameter.serialize(strings, List.of(PLACEHOLDER));
        assertThat(UnavailableValuePlaceholderParameter.matches(strings, List.of(PLACEHOLDER, PLACEHOLDER), declaredArray)).isFalse();
        assertThat(UnavailableValuePlaceholderParameter.matches(strings, List.of("x".repeat(10 * 1024 * 1024)), declaredArray)).isFalse();

        final Schema map = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).build();
        final String declaredMap = UnavailableValuePlaceholderParameter.serialize(map, Map.of(PLACEHOLDER, PLACEHOLDER));
        assertThat(UnavailableValuePlaceholderParameter.matches(map, Map.of("x".repeat(10 * 1024 * 1024), "v"), declaredMap)).isFalse();
    }

    @Test
    public void shouldNotSerializeUnsupportedOrMismatchedValues() {
        assertThat(UnavailableValuePlaceholderParameter.serialize(Schema.OPTIONAL_STRING_SCHEMA, null)).isNull();
        // Java type not matching the schema type
        assertThat(UnavailableValuePlaceholderParameter.serialize(SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).build(), PLACEHOLDER)).isNull();
        assertThat(UnavailableValuePlaceholderParameter.serialize(Schema.OPTIONAL_INT32_SCHEMA, PLACEHOLDER)).isNull();
        // structs are not supported
        final Schema struct = SchemaBuilder.struct().field("f", Schema.OPTIONAL_STRING_SCHEMA).build();
        assertThat(UnavailableValuePlaceholderParameter.serialize(struct, new org.apache.kafka.connect.data.Struct(struct))).isNull();
        // an array containing a null element has no canonical representation
        final Schema strings = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build();
        assertThat(UnavailableValuePlaceholderParameter.serialize(strings, Arrays.asList("a", null))).isNull();
    }
}
