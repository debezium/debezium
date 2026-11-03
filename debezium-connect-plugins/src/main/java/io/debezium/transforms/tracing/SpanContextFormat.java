/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.tracing;

import java.util.Properties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;
import io.debezium.config.EnumeratedValue;

public enum SpanContextFormat implements EnumeratedValue {
    PROPERTIES("properties") {
        @Override
        public Properties deserialize(String spanContext) {
            return PropertiesGetter.extract(spanContext);
        }
    },
    JSON("json") {
        @Override
        public Properties deserialize(String spanContext) {
            final JsonNode node;
            try {
                node = OBJECT_MAPPER.readTree(spanContext);
            }
            catch (JsonProcessingException e) {
                throw new DebeziumException("Unable to parse the tracing span context as JSON", e);
            }
            if (node == null || !node.isObject()) {
                throw new DebeziumException("The tracing span context must be a JSON object");
            }
            final Properties props = new Properties();
            node.properties().forEach(entry -> {
                if (entry.getValue().isTextual()) {
                    props.setProperty(entry.getKey(), entry.getValue().textValue());
                }
            });
            return props;
        }
    };

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final String value;

    SpanContextFormat(String value) {
        this.value = value;
    }

    @Override
    public String getValue() {
        return value;
    }

    public abstract Properties deserialize(String spanContext);

    public static SpanContextFormat parse(String value) {
        if (value == null) {
            return null;
        }
        value = value.trim();
        for (SpanContextFormat option : SpanContextFormat.values()) {
            if (option.getValue().equalsIgnoreCase(value)) {
                return option;
            }
        }
        return null;
    }
}
