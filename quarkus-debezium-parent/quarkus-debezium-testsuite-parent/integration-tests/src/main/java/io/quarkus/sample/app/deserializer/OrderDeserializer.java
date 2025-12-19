/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.sample.app.deserializer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.quarkus.debezium.engine.deserializer.ObjectMapperDeserializer;
import io.quarkus.sample.app.dto.Order;

public class OrderDeserializer extends ObjectMapperDeserializer<Order> {
    private static final ObjectMapper configuredMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public OrderDeserializer() {
        super(Order.class, configuredMapper);
    }
}
