/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.ehcache.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.SerializerException;

/**
 * An abstract implementation of the Ehcache {@link Serializer} interface.
 *
 * @author Chris Cranford
 */
public abstract class AbstractEhcacheSerializer<T> implements Serializer<T> {
    @Override
    public ByteBuffer serialize(T object) throws SerializerException {
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            try (SerializerOutputStream stream = new SerializerOutputStream(output)) {
                serialize(object, stream);
            }
            return ByteBuffer.wrap(output.toByteArray());
        }
        catch (Exception e) {
            throw new SerializerException("Failed to serialize " + object.getClass().getSimpleName(), e);
        }
    }

    @Override
    public T read(ByteBuffer buffer) throws ClassNotFoundException, SerializerException {
        try (ByteArrayInputStream input = new ByteArrayInputStream(buffer.array())) {
            // Depending on how the data is sourced by Ehcache, it may adjust the buffer offset
            // to deal with off-heap preamble bytes. This makes sure skip the right number of
            // bytes in the stream based on the current array offset in the buffer.
            if (input.skip(buffer.arrayOffset()) != buffer.arrayOffset()) {
                throw new SerializerException("Failed to adjust buffer offset position before read");
            }
            try (SerializerInputStream stream = new SerializerInputStream(input)) {
                return deserialize(stream);
            }
        }
        catch (Exception e) {
            throw new SerializerException("Failed to deserialize buffer", e);
        }
    }

    @Override
    public boolean equals(T object, ByteBuffer buffer) throws ClassNotFoundException, SerializerException {
        return Objects.equals(object, read(buffer));
    }

    /**
     * Serialize the specified object to the output stream.
     *
     * @param object the object to be serialized, should not be {@code null}
     * @param stream the output stream to write to, should not be {@code null}
     * @throws IOException when a write operation fails on the output stream
     */
    protected abstract void serialize(T object, SerializerOutputStream stream) throws IOException;

    /**
     * Deserializes the data within the input stream.
     *
     * @param stream the input stream to read, should not be {@code null}
     * @return the object deserialized from the input stream, should not be {@code null}
     * @throws IOException when a read operation fails on the input stream
     */
    protected abstract T deserialize(SerializerInputStream stream) throws IOException;
}
