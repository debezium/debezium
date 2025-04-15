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
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntryImpl;
import io.debezium.relational.TableId;

/**
 * An Infinispan ProtoStream adapter to marshall {@link DmlEvent} instances.
 *
 * This class defines a factory for creating {@link DmlEvent} instances when hydrating
 * records from the persisted datastore as well as field handlers to extract values
 * to be marshalled to the protocol buffer stream.
 *
 * The underlying protocol buffer record consists of the following structure:
 * <pre>
 *     message DmlEvent {
 *         // structure of the super type, LogMinerEventAdapter
 *         required LogMinerDmlEntryImpl entry = 7;
 *     }
 * </pre>
 *
 * @author Chris Cranford
 */
@ProtoAdapter(DmlEvent.class)
public class DmlEventAdapter extends LogMinerEventAdapter {

    /**
     * A ProtoStream factory that creates {@link DmlEvent} instances.
     *
     * @param eventType the event type
     * @param scn the system change number, must not be {@code null}
     * @param tableId the fully-qualified table name
     * @param rowId the Oracle row-id the change is associated with
     * @param rsId the Oracle rollback segment identifier
     * @param changeTime the time the change occurred
     * @param entry the parsed SQL statement entry
     * @return the constructed DmlEvent
     */
    @ProtoFactory
    public DmlEvent factory(int eventType, String scn, String tableId, String rowId, String rsId, String changeTime, LogMinerDmlEntryImpl entry) {
        return new DmlEvent(EventType.from(eventType), Scn.valueOf(scn), TableId.parse(tableId), rowId, rsId, Instant.parse(changeTime), entry);
    }

    /**
     * A ProtoStream handler to extract the {@code entry} field from the {@link DmlEvent}.
     *
     * @param event the event instance, must not be {@code null}
     * @return the LogMinerDmlEntryImpl instance
     */
    @ProtoField(number = 7, required = true)
    public LogMinerDmlEntryImpl getEntry(DmlEvent event) {
        return (LogMinerDmlEntryImpl) event.getDmlEntry();
    }
}
