/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.deserializer;

public interface MutableCapturingEventDeserializerRegistry<V> extends CapturingEventDeserializerRegistry<V> {
    void register(String identifier, Deserializer<?> deserializer);

    void unregister(String identifier);
}
