/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.configmap;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;

/**
 * This class manage the format of key used for Kubernetes config map
 * <p>
 * The keys are converted to a valid config:
 * alphanumeric characters, '-', '_' or '.' (e.g. 'key.name',  or 'KEY_NAME',  or 'key-name',
 * regex used for validation is '[-._a-zA-Z0-9]+').
 * <p>
 * The values are just base64 encoded
 *
 * @author Mario Fiore Vitale
 */
public class ConfigMapFormatter {

    public static final String CONNECTOR_NAME_SEPARATOR = ".";
    public static final String ENTRIES_SEPARATOR = "_";
    public static final String KEY_VALUE_SEPARATOR = "-";
    private final ObjectMapper objectMapper;

    public ConfigMapFormatter(ObjectMapper mapper) {
        this.objectMapper = mapper;
    }

    public ConfigMapFormatter() {
        this.objectMapper = new ObjectMapper();
    }

    public Map<ByteBuffer, ByteBuffer> convertFromStorableFormat(Map<String, String> binaryData) {

        return binaryData.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> fromStringKey(e.getKey()),
                        e -> ByteBuffer.wrap(Base64.getDecoder().decode(e.getValue().getBytes()))));
    }

    public Map<String, String> convertToStorableFormat(Map<ByteBuffer, ByteBuffer> data) {

        return data.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> toStringKey(e.getKey()),
                        e -> new String(Base64.getEncoder().encode(e.getValue().array()))));
    }

    private ByteBuffer fromStringKey(String key) {

        String[] split = key.split("\\" + CONNECTOR_NAME_SEPARATOR);
        String[] entries = split[1].split(ENTRIES_SEPARATOR);
        Map<String, String> properties = Arrays.stream(entries)
                .map(this::toEntry)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        try {
            return ByteBuffer.wrap(objectMapper.writeValueAsBytes(List.of(split[0], properties)));
        }
        catch (Exception e) {
            throw new DebeziumException(String.format("Unable to parse key %s", key), e);
        }
    }

    private Map.Entry<String, String> toEntry(String storedEntry) {
        String[] keyAndValue = storedEntry.split(KEY_VALUE_SEPARATOR);
        return Map.entry(keyAndValue[0], keyAndValue[1]);
    }

    private String toStringKey(ByteBuffer key) {

        try {
            JsonNode jsonNode = objectMapper.readTree(key.array());

            // a valid config key must consist of alphanumeric characters, '-', '_' or '.' (e.g. 'key.name', or 'KEY_NAME', or 'key-name', regex used for validation is '[-._a-zA-Z0-9]+').
            // Note: offset key is always in a form [namespace, partition].
            // On Kafka Connect namespace is the connector name, for engine it is the configured engine name.
            StringBuilder keyBuilder = new StringBuilder();
            for (JsonNode node : jsonNode) {
                if (node.isTextual()) {
                    keyBuilder.append(String.format("%s%s", node.textValue(), CONNECTOR_NAME_SEPARATOR));
                }
                else if (node.isObject()) {
                    Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
                    while (fields.hasNext()) {
                        Map.Entry<String, JsonNode> field = fields.next();
                        keyBuilder.append(String.format("%s%s%s", field.getKey(), KEY_VALUE_SEPARATOR, field.getValue().asText()));
                        if (fields.hasNext()) {
                            keyBuilder.append(ENTRIES_SEPARATOR);
                        }
                    }
                }
            }
            return keyBuilder.toString();
        }
        catch (Exception e) {
            throw new DebeziumException(String.format("Unable to format key %s to a format compatible with config map key", key));
        }
    }

}
