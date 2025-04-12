/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.processor.infinispan.marshalling;

import java.time.Instant;

import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.relational.TableId;

/**
 * An Infinispan ProtoStream adapter to marshall {@link LogMinerEvent} instances.
 *
 * This class defines a factory for creating {@link LogMinerEvent} instances when hydrating
 * records from the persisted event datastore as well as field handlers to extract values
 * to be marshalled to the protocol buffer stream.
 *
 * The underlying protocol buffer record consists of the following structure:
 * <pre>
 *     message LogMinerEvent {
 *         required int32 eventType = 1;
 *         required string scn = 2;
 *         required string tableId = 3;
 *         required string rowId = 4;
 *         required object rsId = 5;
 *         required string changeTime = 6;
 *     }
 * </pre>
 *
 * @author Chris Cranford
 */
@ProtoAdapter(LogMinerEvent.class)
public class LogMinerEventAdapter {

    /**
     * A ProtoStream factory that creates {@link LogMinerEvent} instances.
     *
     * @param eventType the event type
     * @param scn the system change number, must not be {@code null}
     * @param tableId the fully-qualified table name
     * @param rowId the Oracle row-id the change is associated with
     * @param rsId the Oracle rollback segment identifier
     * @param changeTime the time the change occurred
     * @return the constructed DmlEvent
     */
    @ProtoFactory
    public LogMinerEvent factory(int eventType, String scn, String tableId, String rowId, String rsId, String changeTime) {
        return new LogMinerEvent(EventType.from(eventType), Scn.valueOf(scn), TableId.parse(tableId), rowId, rsId, Instant.parse(changeTime));
    }

    /**
     * A ProtoStream handler to extract the {@code eventType} field from the {@link LogMinerEvent}.
     *
     * @param event the event instance, must not be {@code null}
     * @return the event type's numerical representation
     */
    @ProtoField(number = 1, required = true)
    public int getEventType(LogMinerEvent event) {
        // Serialized to its numerical value since int32 data types are native to protocol buffers
        return event.getEventType().getValue();
    }

    /**
     * A ProtoStream handler to extract the {@code scn} field from the {@link LogMinerEvent}.
     *
     * @param event the event instance, must not be {@code null}
     * @return the event's system change number represented as a string
     */
    @ProtoField(number = 2, required = true)
    public String getScn(LogMinerEvent event) {
        // We intentionally serialize the Scn as a string since string values are natively supported by
        // protocol buffers and we don't need to write a special marshaller for the Scn class.
        return event.getScn().toString();
    }

    /**
     * A ProtoStream handler to extract the {@code tableId} field from the {@link LogMinerEvent}.
     *
     * @param event the event instance, must not be {@code null}
     * @return the event's table identifier represented as a string
     */
    @ProtoField(number = 3, required = true)
    public String getTableId(LogMinerEvent event) {
        return event.getTableId().toDoubleQuotedString();
    }

    /**
     * A ProtoStream handler to extract the {@code rowId} field from the {@link LogMinerEvent}.
     *
     * @param event the event instance, must not be {@code null}
     * @return the event's row identifier, never {@code null}
     */
    @ProtoField(number = 4, required = true)
    public String getRowId(LogMinerEvent event) {
        return event.getRowId();
    }

    /**
     * A ProtoStream handler to extract the {@code rsId} field from the {@link LogMinerEvent}.
     *
     * @param event the event instance, must not be {@code null}
     * @return the rollback segment identifier, never {@code null}
     */
    @ProtoField(number = 5, required = true)
    public String getRsId(LogMinerEvent event) {
        return event.getRsId();
    }

    /**
     * A ProtoStream handler to extract the {@code changeTime} field from the {@link LogMinerEvent}.
     *
     * @param event the event instance, must not be {@code null}
     * @return the time when the event occurred as a string, never {@code null}
     */
    @ProtoField(number = 6, required = true)
    public String getChangeTime(LogMinerEvent event) {
        return event.getChangeTime().toString();
    }
}
