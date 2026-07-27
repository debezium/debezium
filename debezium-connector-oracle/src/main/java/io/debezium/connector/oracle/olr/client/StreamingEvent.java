/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr.client;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

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

    @JsonDeserialize(using = ScnDeserializer.class)
    private Scn scn = Scn.NULL;
    @JsonProperty("tm")
    @JsonDeserialize(using = TimestampAsInstantDeserializer.class)
    private Instant timestamp;
    @JsonProperty("b_tm")
    @JsonDeserialize(using = TimestampAsInstantDeserializer.class)
    private Instant beginTimestamp;
    @JsonProperty("e_tm")
    @JsonDeserialize(using = TimestampAsInstantDeserializer.class)
    private Instant commitTimestamp;
    private String xid;
    @JsonProperty("db")
    private String databaseName;
    @JsonProperty("c_scn")
    @JsonDeserialize(using = ScnDeserializer.class)
    private Scn checkpointScn = Scn.NULL;
    @JsonProperty("b_scn")
    @JsonDeserialize(using = ScnDeserializer.class)
    private Scn beginScn = Scn.NULL;
    @JsonProperty("e_scn")
    @JsonDeserialize(using = ScnDeserializer.class)
    private Scn commitScn = Scn.NULL;
    @JsonProperty("c_idx")
    private Long checkpointIndex;
    @JsonProperty("usr")
    private String userName;
    @JsonProperty("rth")
    private Integer redoThreadId;
    private List<PayloadEvent> payload;

    public Scn getScn() {
        return scn;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public Instant getBeginTimestamp() {
        return beginTimestamp;
    }

    public Instant getCommitTimestamp() {
        return commitTimestamp;
    }

    public String getXid() {
        return xid;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public Scn getCheckpointScn() {
        return checkpointScn;
    }

    public Scn getBeginScn() {
        return beginScn;
    }

    public Scn getCommitScn() {
        return commitScn;
    }

    public Long getCheckpointIndex() {
        return checkpointIndex;
    }

    public String getUserName() {
        return userName;
    }

    public int getRedoThreadId() {
        return redoThreadId == null ? 0 : redoThreadId;
    }

    public List<PayloadEvent> getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "StreamingEvent{" +
                "scn='" + scn + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", beginTimestamp='" + beginTimestamp + '\'' +
                ", commitTimestamp='" + commitTimestamp + '\'' +
                ", xid='" + xid + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", checkpointScn='" + checkpointScn + '\'' +
                ", beginScn='" + beginScn + '\'' +
                ", commitScn='" + commitScn + '\'' +
                ", checkpointIndex=" + checkpointIndex +
                ", userName='" + userName + '\'' +
                ", redoThreadId=" + redoThreadId +
                ", payload=" + payload +
                '}';
    }

    static class ScnDeserializer extends StdDeserializer<Scn> {
        ScnDeserializer() {
            super(Scn.class);
        }

        @Override
        public Scn getNullValue(DeserializationContext ctxt) throws JsonMappingException {
            return Scn.NULL;
        }

        @Override
        public Scn deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException {
            final String scn = jsonParser.getText();
            try {
                return scn == null ? Scn.NULL : Scn.valueOf(scn);
            }
            catch (Exception e) {
                throw new IOException("Failed to deserialize SCN: " + scn);
            }
        }
    }

    static class TimestampAsInstantDeserializer extends StdDeserializer<Instant> {
        TimestampAsInstantDeserializer() {
            super(Instant.class);
        }

        @Override
        public Instant deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException {
            final String timestamp = jsonParser.getText();
            try {
                return Instant.ofEpochMilli(Long.parseLong(timestamp));
            }
            catch (NumberFormatException e) {
                throw new IOException("Failed to deserialize timestamp as instant: " + timestamp);
            }
        }
    }
}
