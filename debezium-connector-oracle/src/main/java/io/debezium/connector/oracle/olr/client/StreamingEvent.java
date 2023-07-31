/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr.client;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A logical streaming event.
 *
 * OpenLogReplicator emits an event that contains zero or many payload events. A payload event
 * describes a particular operation in the transaction logs. A streaming event emits zero or
 * many payloads because OpenLogReplicator supports emitting a transaction as one or multiple
 * events. By default, Debezium consumes a transaction across multiple events.
 *
 * @author Chris Cranford
 */
public class StreamingEvent {

    private String scn;
    @JsonProperty("tm")
    private String timestamp;
    private String xid;
    private List<PayloadEvent> payload;

    public String getScn() {
        return scn;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getXid() {
        return xid;
    }

    public List<PayloadEvent> getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "StreamingEvent{" +
                "scn='" + scn + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", xid='" + xid + '\'' +
                ", payload=" + payload +
                '}';
    }

}
