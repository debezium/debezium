/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr.client.payloads;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a mutation (DML) payload event.
 *
 * @author Chris Cranford
 */
public abstract class AbstractMutationEvent extends AbstractPayloadEvent {
    @JsonProperty("schema")
    private PayloadSchema schema;
    @JsonProperty("before")
    private Values before;
    @JsonProperty("after")
    private Values after;

    public AbstractMutationEvent(Type type) {
        super(type);
    }

    public PayloadSchema getSchema() {
        return schema;
    }

    public Values getBefore() {
        return before;
    }

    public Values getAfter() {
        return after;
    }

    @Override
    public String toString() {
        return "AbstractMutationEvent{" +
                "schema=" + schema +
                ", before=" + before +
                ", after=" + after +
                "} " + super.toString();
    }
}
