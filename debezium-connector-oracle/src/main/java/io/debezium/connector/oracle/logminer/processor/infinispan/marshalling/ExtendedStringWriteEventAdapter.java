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
import io.debezium.connector.oracle.logminer.events.ExtendedStringWriteEvent;
import io.debezium.relational.TableId;

/**
 * An Infinispan ProtoStream adapter to marshall {@link ExtendedStringWriteEvent} instances.
 *
 * This class defines a factory for creating {@link ExtendedStringWriteEvent} instances when hydrating
 * records from the persisted datastore as well as field handlers to extract values to be marshalled
 * to the protocol buffer stream.
 *
 * @author Chris Cranford
 */
@ProtoAdapter(ExtendedStringWriteEvent.class)
public class ExtendedStringWriteEventAdapter extends LogMinerEventAdapter {
    /**
     * A ProtoStream factory that creates {@link ExtendedStringWriteEvent} instances.
     *
     * @param eventType the event type
     * @param scn the system change number, must not be {@code null}
     * @param tableId the fully-qualified table name
     * @param rowId the Oracle row-id the change is associated with
     * @param rsId the Oracle rollback segment identifier
     * @param changeTime the time the change occurred
     * @param data the LOB data
     * @return the constructed ExtendedStringWriteEvent
     */
    @ProtoFactory
    public ExtendedStringWriteEvent factory(int eventType, String scn, String tableId, String rowId, String rsId, String changeTime, String data) {
        return new ExtendedStringWriteEvent(EventType.from(eventType), Scn.valueOf(scn), TableId.parse(tableId), rowId, rsId, Instant.parse(changeTime), data);
    }

    /**
     * A ProtoStream handler to extract the {@code data} field from a {@link ExtendedStringWriteEvent} type.
     *
     * @param event the event instance, must not be {@code null}
     * @return the data to be written for a LOB field, may be {@code null}
     */
    @ProtoField(number = 7)
    public String getData(ExtendedStringWriteEvent event) {
        return event.getData();
    }
}
