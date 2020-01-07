/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.internal;

import java.time.Instant;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.databind.JsonNode;

import io.debezium.outbox.quarkus.ExportedEvent;

/**
 * The outbox event entity.
 *
 * The contents of the {@link ExportedEvent} will be replicated to this entity definition and persisted to
 * the database in order for Debezium to capture the event.
 *
 * @author Chris Cranford
 */
@Entity
public class OutboxEvent {
    @Id
    @GeneratedValue
    private UUID id;

    @NotNull
    private String aggregateType;

    @NotNull
    private String aggregateId;

    @NotNull
    private String type;

    @NotNull
    private Instant timestamp;

    // todo: for now mapping this as varchar(8000).
    // Need to investigate if we can write our own hibernate type implementation that does not
    // cause the DeletedElementException when building a native image.
    @NotNull
    @Column(columnDefinition = "varchar(8000)")
    @Convert(converter = JsonNodeAttributeConverter.class)
    private JsonNode payload;

    OutboxEvent() {
    }

    public OutboxEvent(String aggregateType, String aggregateId, String type, JsonNode payload, Instant timestamp) {
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.type = type;
        this.payload = payload;
        this.timestamp = timestamp;
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public void setAggregateType(String aggregateType) {
        this.aggregateType = aggregateType;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public void setAggregateId(String aggregateId) {
        this.aggregateId = aggregateId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public JsonNode getPayload() {
        return payload;
    }

    public void setPayload(JsonNode payload) {
        this.payload = payload;
    }
}
