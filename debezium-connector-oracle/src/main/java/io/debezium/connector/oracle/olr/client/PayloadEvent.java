/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr.client;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import io.debezium.connector.oracle.olr.client.payloads.BeginEvent;
import io.debezium.connector.oracle.olr.client.payloads.CheckpointEvent;
import io.debezium.connector.oracle.olr.client.payloads.CommitEvent;
import io.debezium.connector.oracle.olr.client.payloads.DeleteEvent;
import io.debezium.connector.oracle.olr.client.payloads.InsertEvent;
import io.debezium.connector.oracle.olr.client.payloads.SchemaChangeEvent;
import io.debezium.connector.oracle.olr.client.payloads.UpdateEvent;

/**
 * @author Chris Cranford
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "op")
@JsonSubTypes({
        @JsonSubTypes.Type(value = BeginEvent.class, name = "begin"),
        @JsonSubTypes.Type(value = CommitEvent.class, name = "commit"),
        @JsonSubTypes.Type(value = InsertEvent.class, name = "c"),
        @JsonSubTypes.Type(value = UpdateEvent.class, name = "u"),
        @JsonSubTypes.Type(value = DeleteEvent.class, name = "d"),
        @JsonSubTypes.Type(value = SchemaChangeEvent.class, name = "ddl"),
        @JsonSubTypes.Type(value = CheckpointEvent.class, name = "chkpt")
})
public interface PayloadEvent {
    /**
     * Various payload event types
     */
    enum Type {
        BEGIN,
        COMMIT,
        INSERT,
        UPDATE,
        DELETE,
        DDL,
        CHECKPOINT
    };

    Type getType();

    String getRid();

    Integer getNum();

}
