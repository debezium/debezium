/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr.client.payloads;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a checkpoint event.
 *
 * @author Chris Cranford
 */
public class CheckpointEvent extends AbstractPayloadEvent {

    @JsonProperty("seq")
    private Long sequence;
    private Long offset;
    private boolean redo;

    public CheckpointEvent() {
        super(Type.CHECKPOINT);
    }

    public Long getSequence() {
        return sequence;
    }

    public Long getOffset() {
        return offset;
    }

    public boolean isRedo() {
        return redo;
    }

    @Override
    public String toString() {
        return "CheckpointPayloadEvent{" +
                "sequence=" + sequence +
                ", offset=" + offset +
                ", redo=" + redo +
                "} " + super.toString();
    }

}
