/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.it;

import java.time.Instant;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.debezium.outbox.quarkus.ExportedEvent;

public class MyOutboxEvent implements ExportedEvent<Long, JsonNode> {
    private static ObjectMapper mapper = new ObjectMapper();
    private final Map<String, Object> additionalValues;

    public MyOutboxEvent(Map<String, Object> additionalValues) {
        this.additionalValues = additionalValues;
    }

    @Override
    public Long getAggregateId() {
        return 1L;
    }

    @Override
    public String getAggregateType() {
        return "MyOutboxEvent";
    }

    @Override
    public String getType() {
        return "SomeType";
    }

    @Override
    public Instant getTimestamp() {
        return Instant.now();
    }

    // using this test to illustrate case of JsonNode payload
    @Override
    public JsonNode getPayload() {
        ObjectNode asJson = mapper.createObjectNode()
                .put("something", "Some amazing payload");

        return asJson;
        // return "Some amazing payload";
    }

    @Override
    public Map<String, Object> getAdditionalFieldValues() {
        return additionalValues;
    }
}
