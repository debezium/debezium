/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.serde.json;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.debezium.common.annotation.Incubating;
import io.debezium.data.Envelope;

/**
 * A {@link Serde} that (de-)serializes JSON. The {@link Deserializer} supports Debezium's CDC message format, i.e. for
 * such messages the values to be deserialized will be unwrapped from the {@code id} field (for keys) or from the
 * {@code after} field.
 *
 * @author Gunnar Morling, Jiri Pechanec
 *
 * @param <T> The object type
 */
@Incubating
public class JsonSerde<T> implements Serde<T> {
    private static final String PAYLOAD_FIELD = "payload";

    private final ObjectMapper mapper;
    private ObjectReader reader;
    private boolean isKey;
    private JsonSerdeConfig config;

    public JsonSerde(Class<T> objectType) {
        mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());

        this.reader = mapper.readerFor(objectType);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        this.config = new JsonSerdeConfig(configs);

        if (config.isUnknownPropertiesIgnored() &&
                mapper.getDeserializationConfig().isEnabled(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)) {
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            reader = reader.without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<T> serializer() {
        return new JsonSerializer();
    }

    @Override
    public Deserializer<T> deserializer() {
        return new JsonDeserializer();
    }

    private final class JsonDeserializer implements Deserializer<T> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public T deserialize(String topic, byte[] data) {
            if (data == null) {
                return null;
            }

            try {
                JsonNode node = mapper.readTree(data);

                return isKey ? readKey(node) : readValue(node);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private T readValue(JsonNode node) throws IOException {
            JsonNode payload = node.get(PAYLOAD_FIELD);

            // Schema + payload format
            if (payload != null) {
                node = payload;
            }
            // Debezium envelope
            if (config.asEnvelope()) {
                return reader.readValue(node);
            }
            else if (node.has(Envelope.FieldName.SOURCE) && node.has(config.sourceField())) {
                return reader.readValue(node.get(config.sourceField()));
            }
            // Extracted format
            else {
                return reader.readValue(node);
            }
        }

        private T readKey(JsonNode node) throws IOException {
            if (!node.isObject()) {
                return reader.readValue(node);
            }

            final JsonNode keys = node.has(PAYLOAD_FIELD) ? node.get(PAYLOAD_FIELD) : node;
            final Iterator<String> keyFields = keys.fieldNames();
            if (keyFields.hasNext()) {
                final String id = keyFields.next();
                if (!keyFields.hasNext()) {
                    // Simple key
                    return reader.readValue(keys.get(id));
                }
                // Composite key
                return reader.readValue(keys);
            }
            return reader.readValue(keys);
        }

        @Override
        public void close() {
        }
    }

    private final class JsonSerializer implements Serializer<T> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public byte[] serialize(String topic, T data) {
            try {
                return mapper.writeValueAsBytes(data);
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
        }
    }
}
