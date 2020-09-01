/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.it;

import java.time.Instant;

import io.debezium.outbox.quarkus.ExportedEvent;

public class MyOutboxEvent implements ExportedEvent<Long, String> {

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
}
