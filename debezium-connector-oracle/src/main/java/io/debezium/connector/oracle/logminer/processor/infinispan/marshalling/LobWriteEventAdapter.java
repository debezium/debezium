/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.infinispan.marshalling;

import java.time.Instant;

import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LobWriteEvent;
import io.debezium.relational.TableId;

/**
 * An Infinispan ProtoStream adapter to marshall {@link LobWriteEvent} instances.
 *
 * This class defines a factory for creating {@link LobWriteEvent} instances when hydrating
 * records from the persisted datastore asa well as field handlers to extract values
 * to be marshalled to the protocol buffer stream.
 *
 * The underlying protocol buffer record consists of the following structure:
 * <pre>
 *     message LobWriteEvent {
 *         // structure of the super type, LogMinerEventAdapter
 *         string data = 7;
 *     }
 * </pre>
 *
 * @author Chris Cranford
 */
@ProtoAdapter(LobWriteEvent.class)
public class LobWriteEventAdapter extends LogMinerEventAdapter {

    /**
     * A ProtoStream factory that creates {@link LobWriteEvent} instances.
     *
     * @param eventType the event type
     * @param scn the system change number, must not be {@code null}
     * @param tableId the fully-qualified table name
     * @param rowId the Oracle row-id the change is associated with
     * @param rsId the Oracle rollback segment identifier
     * @param changeTime the time the change occurred
     * @param data the LOB data
     * @param offset the LOB data offset (0-based)
     * @param length the LOB data length
     * @return the constructed DmlEvent
     */
    @ProtoFactory
    public LobWriteEvent factory(int eventType, String scn, String tableId, String rowId, String rsId, String changeTime, String data, int offset, int length) {
        return new LobWriteEvent(EventType.from(eventType), Scn.valueOf(scn), TableId.parse(tableId), rowId, rsId, Instant.parse(changeTime), data, offset, length);
    }

    /**
     * A ProtoStream handler to extract the {@code data} field from a {@link LobWriteEvent} type.
     *
     * @param event the event instance, must not be {@code null}
     * @return the data to be written for a LOB field, may be {@code null}
     */
    @ProtoField(number = 7)
    public String getData(LobWriteEvent event) {
        return event.getData();
    }

    /**
     * A ProtoStream handler to extract the {@code offset} field from a {@link LobWriteEvent} type.
     *
     * @param event the event instance, must not be {@code null}
     * @return the offset of the data to be written for a LOB field
     */
    @ProtoField(number = 8, required = true)
    public int getOffset(LobWriteEvent event) {
        return event.getOffset();
    }

    /**
     * A ProtoStream handler to extract the {@code length} field from a {@link LobWriteEvent} type.
     *
     * @param event the event instance, must not be {@code null}
     * @return the length of the data to be written for a LOB field
     */
    @ProtoField(number = 9, required = true)
    public int getLength(LobWriteEvent event) {
        return event.getLength();
    }
}
