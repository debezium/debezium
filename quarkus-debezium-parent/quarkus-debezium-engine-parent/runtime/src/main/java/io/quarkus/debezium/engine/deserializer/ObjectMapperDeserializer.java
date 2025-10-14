/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.deserializer;

import java.io.IOException;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 * Jackson deserializer for a change data capture event
 * @param <T>
 */
public class ObjectMapperDeserializer<T> implements Deserializer<T> {

    public static final String PAYLOAD = "payload";
    private final JavaType type;
    private final ObjectMapper objectMapper;

    public ObjectMapperDeserializer(Class<T> type) {
        this(type, ObjectMapperProducer.get());
    }

    public ObjectMapperDeserializer(Class<T> type, ObjectMapper objectMapper) {
        this.type = TypeFactory.defaultInstance().constructType(type);
        this.objectMapper = objectMapper;
    }

    @Override
    public T deserialize(byte[] data, String path) {
        if (data == null) {
            return null;
        }

        if (path == null) {
            return deserializeFlattenEvent(data);
        }

        return deserializeDebeziumEvent(data, path);
    }

    private T deserializeFlattenEvent(byte[] data) {
        try {
            JsonNode root = objectMapper.readTree(data);

            return objectMapper.treeToValue(root.path(PAYLOAD), type);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private T deserializeDebeziumEvent(byte[] data, String path) {
        try {
            JsonNode root = objectMapper.readTree(data);
            JsonNode afterNode = root.path(PAYLOAD).path(path);

            if (containsJson(afterNode)) {
                return objectMapper.treeToValue(objectMapper.readTree(afterNode.textValue()), type);
            }

            return objectMapper.treeToValue(afterNode, type);

        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * @param afterNode Json tree model to check
     * @return if the json node is a string that contains JSON
     */
    private boolean containsJson(JsonNode afterNode) {
        return afterNode.isTextual();
    }

    @Override
    public void close() {

    }
}
