/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.deserializer;

/**
 * Deserializer for a change data capture event
 * @param <T>
 */
public interface Deserializer<T> extends AutoCloseable {
    T deserialize(byte[] data, String path);
}
