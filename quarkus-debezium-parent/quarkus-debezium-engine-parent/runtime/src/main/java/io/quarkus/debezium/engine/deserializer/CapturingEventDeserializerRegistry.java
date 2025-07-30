/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.deserializer;

import io.quarkus.debezium.engine.CapturingEventDeserializer;

/**
 * Registry that contains deserializers that maps V to type parameter for {@link io.debezium.runtime.CapturingEvent<>}
 * @param <V>
 */
public interface CapturingEventDeserializerRegistry<V> {

    CapturingEventDeserializer<?, V> get(String identifier);
}
