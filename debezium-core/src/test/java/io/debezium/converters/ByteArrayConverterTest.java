/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.converters;

import static junit.framework.TestCase.fail;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

public class ByteArrayConverterTest {
    private static final String TOPIC = "topic";
    private static final byte[] SAMPLE_BYTES = "sample string".getBytes(StandardCharsets.UTF_8);

    private final ByteArrayConverter converter = new ByteArrayConverter();

    @Before
    public void setUp() {
        converter.configure(Collections.emptyMap(), false);
    }

    @Test
    public void shouldConvertFromData() {
        assertThat(converter.fromConnectData(TOPIC, Schema.BYTES_SCHEMA, SAMPLE_BYTES)).isEqualTo(SAMPLE_BYTES);
    }

    @Test
    public void shouldConvertFromDataWithNullSchema() {
        assertThat(converter.fromConnectData(TOPIC, null, SAMPLE_BYTES)).isEqualTo(SAMPLE_BYTES);
    }

    @Test
    public void shouldThrowExceptionWhenInvalidSchema() {
        assertThrows(DataException.class,
                () -> converter.fromConnectData(TOPIC, Schema.INT32_SCHEMA, SAMPLE_BYTES));
    }

    @Test
    public void shouldThrowExceptionWhenInvalidValue() {
        assertThrows(DataException.class,
                () -> converter.fromConnectData(TOPIC, Schema.BYTES_SCHEMA, 12));
    }

    @Test
    public void shouldReturnNullWhenConvertingNullValue() {
        assertThat(converter.fromConnectData(TOPIC, Schema.BYTES_SCHEMA, null)).isNull();
    }

    @Test
    public void shouldConvertFromDataIgnoringHeader() {
        assertThat(converter.fromConnectHeader(TOPIC, "ignored", Schema.BYTES_SCHEMA, SAMPLE_BYTES)).isEqualTo(SAMPLE_BYTES);
    }

    @Test
    public void shouldConvertToConnectData() {
        SchemaAndValue data = converter.toConnectData(TOPIC, SAMPLE_BYTES);
        assertThat(data.schema()).isEqualTo(Schema.OPTIONAL_BYTES_SCHEMA);
        assertThat((byte[]) data.value()).isEqualTo(SAMPLE_BYTES);
    }

    @Test
    public void shouldConvertToConnectDataForNullValue() {
        SchemaAndValue data = converter.toConnectData(TOPIC, null);
        assertThat(data.schema()).isEqualTo(Schema.OPTIONAL_BYTES_SCHEMA);
        assertThat(data.value()).isNull();
    }

    @Test
    public void shouldThrowWhenNoDelegateConverterConfigured() {
        try {
            converter.fromConnectData(TOPIC, Schema.OPTIONAL_STRING_SCHEMA, "Hello World");
            fail("now expected exception thrown");
        }
        catch (Exception e) {
            assertThat(e).isExactlyInstanceOf(DataException.class);
        }
    }

    @Test
    public void shouldThrowWhenNoSchemaOrDelegateConverterConfigured() {
        try {
            converter.fromConnectData(TOPIC, null, "Hello World");
            fail("now expected exception thrown");
        }
        catch (Exception e) {
            assertThat(e).isExactlyInstanceOf(DataException.class);
        }
    }

    @Test
    public void shouldConvertUsingDelegateConverter() {
        // Configure delegate converter
        converter.configure(Collections.singletonMap(ByteArrayConverter.DELEGATE_CONVERTER_TYPE, JsonConverter.class.getName()), false);

        byte[] data = converter.fromConnectData(TOPIC, Schema.OPTIONAL_STRING_SCHEMA, "{\"message\": \"Hello World\"}");

        JsonNode value = null;
        try (JsonDeserializer jsonDeserializer = new JsonDeserializer()) {
            value = jsonDeserializer.deserialize(TOPIC, data);
        }

        assertThat(value).isNotNull();
        assertThat(value.get("schema")).isNotNull();
        assertThat(value.get("schema").get("type").asText()).isEqualTo("string");
        assertThat(value.get("schema").get("optional").asBoolean()).isTrue();
        assertThat(value.get("payload")).isNotNull();
        assertThat(value.get("payload").asText()).isEqualTo("{\"message\": \"Hello World\"}");
    }

    @Test
    public void shouldConvertUsingDelegateConverterWithNoSchema() {
        // Configure delegate converter
        converter.configure(Collections.singletonMap(ByteArrayConverter.DELEGATE_CONVERTER_TYPE, JsonConverter.class.getName()), false);

        byte[] data = converter.fromConnectData(TOPIC, null, "{\"message\": \"Hello World\"}");

        JsonNode value = null;
        try (JsonDeserializer jsonDeserializer = new JsonDeserializer()) {
            value = jsonDeserializer.deserialize(TOPIC, data);
        }

        assertThat(value).isNotNull();
        assertThat(value.get("schema")).isNotNull();
        assertThat(value.get("schema").asText()).isEqualTo("null");
        assertThat(value.get("payload")).isNotNull();
        assertThat(value.get("payload").asText()).isEqualTo("{\"message\": \"Hello World\"}");
    }

    @Test
    public void shouldConvertUsingDelegateConverterWithOptionsAndNoSchema() {
        // Configure delegate converter
        final Map<String, String> config = new HashMap<>();
        config.put(ByteArrayConverter.DELEGATE_CONVERTER_TYPE, JsonConverter.class.getName());
        config.put(ByteArrayConverter.DELEGATE_CONVERTER_TYPE + ".schemas.enable", Boolean.FALSE.toString());
        converter.configure(config, false);

        byte[] data = converter.fromConnectData(TOPIC, null, "{\"message\": \"Hello World\"}");

        JsonNode value = null;
        try (JsonDeserializer jsonDeserializer = new JsonDeserializer()) {
            value = jsonDeserializer.deserialize(TOPIC, data);
        }

        assertThat(value.has("schema")).isFalse();
        assertThat(value.has("payload")).isFalse();
        assertThat(value.asText()).isEqualTo("{\"message\": \"Hello World\"}");
    }
}
