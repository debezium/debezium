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

        try {
            JsonNode root = objectMapper.readTree(data);
            JsonNode afterNode = root.path(PAYLOAD).path(path);
            return objectMapper.treeToValue(afterNode, type);

        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {

    }
}
