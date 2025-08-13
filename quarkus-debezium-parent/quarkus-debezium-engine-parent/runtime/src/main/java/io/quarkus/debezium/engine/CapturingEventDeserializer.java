/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import io.debezium.runtime.CapturingEvent;

public interface CapturingEventDeserializer<T, V> {
    CapturingEvent<T> deserialize(CapturingEvent<V> event);
}
