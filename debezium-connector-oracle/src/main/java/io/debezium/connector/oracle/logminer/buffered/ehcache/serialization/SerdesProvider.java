/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.ehcache.serialization;

import java.io.IOException;

/**
 * Defines the contract for supplying a Serdes (Serializer/Deserializer tuple) for the Ehcache
 * transaction event cache that stores different types of events.
 *
 * @author Chris Cranford
 */
public interface SerdesProvider<T> {
    /**
     * Get the java type for the serialization/deserialization provider.
     *
     * @return the java class type
     */
    Class<?> getJavaType();

    /**
     * Serializes the object into the output stream.
     *
     * @param object the object to be serialized, should not be {@code null}
     * @param stream the output stream, should not be {@code null}
     * @throws IOException thrown if there is a problem serializing the data to the output stream
     */
    void serialize(T object, SerializerOutputStream stream) throws IOException;

    /**
     * Deserializes the input stream.
     *
     * @param context the deserialization context, should not be {@code null}
     * @param stream the input stream to be read, should not be {@code null}
     * @throws IOException thrown if there is a problem deserializing the data from the input stream
     */
    void deserialize(DeserializationContext context, SerializerInputStream stream) throws IOException;
}
