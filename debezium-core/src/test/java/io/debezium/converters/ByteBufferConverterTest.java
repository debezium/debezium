/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.converters;

import static junit.framework.TestCase.fail;
import static org.fest.assertions.Assertions.assertThat;

import java.nio.ByteBuffer;
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

public class ByteBufferConverterTest {

    private static final String TOPIC = "topic";
    private static final byte[] SAMPLE_BYTES = "sample string".getBytes(StandardCharsets.UTF_8);

    private ByteBufferConverter converter = new ByteBufferConverter();

    @Before
    public void setUp() {
        converter.configure(Collections.emptyMap(), false);
    }

    @Test
    public void shouldConvertFromConnectData() {
        byte[] bytes = converter.fromConnectData(TOPIC, Schema.BYTES_SCHEMA, ByteBuffer.wrap(SAMPLE_BYTES));

        assertThat(bytes).isEqualTo(SAMPLE_BYTES);
    }

    @Test
    public void shouldConvertFromConnectDataForOptionalBytesSchema() {
        byte[] bytes = converter.fromConnectData(TOPIC, Schema.OPTIONAL_BYTES_SCHEMA, ByteBuffer.wrap(SAMPLE_BYTES));

        assertThat(bytes).isEqualTo(SAMPLE_BYTES);
    }

    @Test
    public void shouldConvertFromConnectDataWithoutSchema() {
        byte[] bytes = converter.fromConnectData(TOPIC, null, ByteBuffer.wrap(SAMPLE_BYTES));

        assertThat(bytes).isEqualTo(SAMPLE_BYTES);
    }

    @Test
    public void shouldConvertNullFromConnectData() {
        byte[] bytes = converter.fromConnectData(TOPIC, null, null);

        assertThat(bytes).isNull();
    }

    @Test
    public void shouldThrowWhenConvertNonByteSchemaFromConnectData() {
        try {
            converter.fromConnectData(TOPIC, Schema.BOOLEAN_SCHEMA, null);
            fail("now expected exception thrown");
        }
        catch (Exception e) {
            assertThat(e).isExactlyInstanceOf(DataException.class);
        }
    }

    @Test
    public void shouldThrowWhenConvertRawByteArrayFromConnectData() {
        try {
            converter.fromConnectData(TOPIC, Schema.BOOLEAN_SCHEMA, null);
            fail("now expected exception thrown");
        }
        catch (Exception e) {
            assertThat(e).isExactlyInstanceOf(DataException.class);
        }

    }

    @Test
    public void shouldConvertToConnectData() {
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, SAMPLE_BYTES);

        assertThat(schemaAndValue.schema()).isEqualTo(Schema.OPTIONAL_BYTES_SCHEMA);
        assertThat(schemaAndValue.value()).isEqualTo(ByteBuffer.wrap(SAMPLE_BYTES));
    }

    @Test
    public void shouldConvertToConnectDataForNullValue() {
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, null);

        assertThat(schemaAndValue.schema()).isEqualTo(Schema.OPTIONAL_BYTES_SCHEMA);
        assertThat(schemaAndValue.value()).isNull();
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
    public void shouldConvertUsingDelegateConverter() {
        // Configure delegate converter
        converter.configure(Collections.singletonMap(ByteBufferConverter.DELEGATE_CONVERTER_TYPE, JsonConverter.class.getName()), false);

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
    public void shouldConvertUsingDelegateConverterWithOptions() {
        // Configure delegate converter
        final Map<String, String> config = new HashMap<>();
        config.put(ByteBufferConverter.DELEGATE_CONVERTER_TYPE, JsonConverter.class.getName());
        config.put(ByteBufferConverter.DELEGATE_CONVERTER_TYPE + ".schemas.enable", Boolean.FALSE.toString());
        converter.configure(config, false);

        byte[] data = converter.fromConnectData(TOPIC, Schema.OPTIONAL_STRING_SCHEMA, "{\"message\": \"Hello World\"}");

        JsonNode value = null;
        try (JsonDeserializer jsonDeserializer = new JsonDeserializer()) {
            value = jsonDeserializer.deserialize(TOPIC, data);
        }

        assertThat(value.has("schema")).isFalse();
        assertThat(value.has("payload")).isFalse();
        assertThat(value.asText()).isEqualTo("{\"message\": \"Hello World\"}");
    }
}
