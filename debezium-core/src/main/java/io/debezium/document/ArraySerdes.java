/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.document;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import io.debezium.annotation.Immutable;

/**
 * A Kafka {@link Serializer} and {@link Serializer} that operates upon Debezium {@link Array}s.
 *
 * @author Randall Hauch
 */
@Immutable
public class ArraySerdes implements Serializer<Array>, Deserializer<Array> {

    private static final ArrayWriter ARRAY_WRITER = ArrayWriter.defaultWriter();
    private static final ArrayReader ARRAY_READER = ArrayReader.defaultReader();

    @Override
    public void configure(Map<String, ?> arg0, boolean arg1) {
    }

    @Override
    public byte[] serialize(String topic, Array data) {
        return ARRAY_WRITER.writeAsBytes(data);
    }

    @Override
    public Array deserialize(String topic, byte[] data) {
        try {
            return ARRAY_READER.readArray(bytesToString(data));
        }
        catch (IOException e) {
            // Should never see this, but shit if we do ...
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
    }

    private String bytesToString(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
