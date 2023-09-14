/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr.client;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.debezium.connector.oracle.Scn;

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
    @JsonProperty("db")
    private String databaseName;
    @JsonProperty("c_scn")
    private String checkpointScn;
    @JsonProperty("c_idx")
    private Long checkpointIndex;
    private List<PayloadEvent> payload;
    @JsonIgnore
    private Scn eventScn;
    @JsonIgnore
    private Scn eventCheckpointScn;

    public Scn getEventScn() {
        if (eventScn == null) {
            eventScn = scn == null ? Scn.NULL : Scn.valueOf(scn);
        }
        return eventScn;
    }

    public Scn getEventCheckpointScn() {
        if (eventCheckpointScn == null) {
            eventCheckpointScn = checkpointScn == null ? Scn.NULL : Scn.valueOf(checkpointScn);
        }
        return eventCheckpointScn;
    }

    public String getScn() {
        return scn;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getXid() {
        return xid;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getCheckpointScn() {
        return checkpointScn;
    }

    public Long getCheckpointIndex() {
        return checkpointIndex;
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
                ", databaseName='" + databaseName + '\'' +
                ", checkpointScn='" + checkpointScn + '\'' +
                ", checkpointIndex=" + checkpointIndex +
                ", payload=" + payload +
                ", eventScn=" + eventScn +
                '}';
    }

}
