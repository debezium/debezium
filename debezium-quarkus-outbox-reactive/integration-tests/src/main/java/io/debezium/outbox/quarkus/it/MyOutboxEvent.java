/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.it;

import java.time.Instant;
import java.util.Map;

import io.debezium.outbox.quarkus.ExportedEvent;

public class MyOutboxEvent implements ExportedEvent<Long, String> {

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

    @Override
    public String getPayload() {
        return "Some amazing payload";
    }

    @Override
    public Map<String, Object> getAdditionalFieldValues() {
        return additionalValues;
    }
}
