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
 * A Kafka {@link Deserializer} and {@link Serializer} that operates upon Debezium {@link Document}s.
 *
 * @author Randall Hauch
 */
@Immutable
public class DocumentSerdes implements Serializer<Document>, Deserializer<Document> {

    public static DocumentSerdes INSTANCE = new DocumentSerdes();

    private static final DocumentReader DOCUMENT_READER = DocumentReader.defaultReader();
    private static final DocumentWriter DOCUMENT_WRITER = DocumentWriter.defaultWriter();

    @Override
    public void configure(Map<String, ?> arg0, boolean arg1) {
    }

    @Override
    public byte[] serialize(String topic, Document data) {
        return DOCUMENT_WRITER.writeAsBytes(data);
    }

    @Override
    public Document deserialize(String topic, byte[] data) {
        try {
            return DOCUMENT_READER.read(bytesToString(data));
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
