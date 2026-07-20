/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi.storage;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;

/**
 * Default implementation of {@link OffsetStorageReader} that works with any {@link OffsetStore}.
 * <p>
 * Uses Jackson for JSON serialization, producing the same byte format as Kafka Connect's
 * {@code JsonConverter} with {@code schemas.enable=false}:
 * <ul>
 *   <li>Key format: {@code ["namespace", {"field": "value", ...}]} (JSON array)</li>
 *   <li>Value format: {@code {"field": value, ...}} (JSON object)</li>
 * </ul>
 * This ensures backward compatibility with data written by Kafka Connect-based offset stores.
 *
 * @author Debezium Authors
 */
public class DefaultOffsetStorageReader implements OffsetStorageReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultOffsetStorageReader.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REF = new TypeReference<>() {
    };

    private final OffsetStore backingStore;
    private final String namespace;

    public DefaultOffsetStorageReader(OffsetStore backingStore, String namespace) {
        this.backingStore = backingStore;
        this.namespace = namespace;
    }

    @Override
    public <T> Map<String, Object> offset(Map<String, T> partition) {
        return offsets(java.util.List.of(partition)).get(partition);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
        Map<ByteBuffer, Map<String, T>> serializedToOriginal = new HashMap<>(partitions.size());
        for (Map<String, T> partition : partitions) {
            try {
                byte[] keySerialized = MAPPER.writeValueAsBytes(Arrays.asList(namespace, partition));
                ByteBuffer keyBuffer = ByteBuffer.wrap(keySerialized);
                serializedToOriginal.put(keyBuffer, partition);
            }
            catch (JsonProcessingException e) {
                LOGGER.error("Failed to serialize partition key for namespace {}. "
                        + "No value for this partition will be returned.", namespace, e);
            }
        }

        Map<ByteBuffer, ByteBuffer> raw;
        try {
            Future<Map<ByteBuffer, ByteBuffer>> future = backingStore.get(serializedToOriginal.keySet());
            raw = future.get();
        }
        catch (Exception e) {
            LOGGER.error("Failed to fetch offsets from namespace {}", namespace, e);
            throw new DebeziumException("Failed to fetch offsets.", e);
        }

        Map<Map<String, T>, Map<String, Object>> result = new HashMap<>(partitions.size());
        for (Map.Entry<ByteBuffer, ByteBuffer> rawEntry : raw.entrySet()) {
            try {
                if (!serializedToOriginal.containsKey(rawEntry.getKey())) {
                    LOGGER.error("Unexpected key {} in response from backing store", rawEntry.getKey());
                    continue;
                }
                Map<String, T> origKey = serializedToOriginal.get(rawEntry.getKey());
                if (rawEntry.getValue() == null) {
                    continue;
                }

                byte[] valueBytes;
                if (rawEntry.getValue().hasArray()) {
                    valueBytes = rawEntry.getValue().array();
                }
                else {
                    valueBytes = new byte[rawEntry.getValue().remaining()];
                    rawEntry.getValue().get(valueBytes);
                }
                Map<String, Object> deserializedValue = MAPPER.readValue(valueBytes, MAP_TYPE_REF);
                result.put(origKey, deserializedValue);
            }
            catch (Exception e) {
                LOGGER.error("Failed to deserialize offset data for namespace {}. "
                        + "No value for this partition will be returned.", namespace, e);
            }
        }

        return result;
    }
}
