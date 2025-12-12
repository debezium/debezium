/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.quarkus.arc.Arc;
import io.quarkus.arc.ArcContainer;

public class ObjectMapperProducer {
    private ObjectMapperProducer() {
    }

    static ObjectMapper get() {
        ObjectMapper objectMapper = null;
        ArcContainer container = Arc.container();
        if (container != null) {
            objectMapper = container.instance(ObjectMapper.class).get();
        }
        return objectMapper != null ? objectMapper : new ObjectMapper();
    }
}
