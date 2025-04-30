/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.infinispan.marshalling;

import java.time.Instant;

import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.XmlBeginEvent;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntryImpl;
import io.debezium.relational.TableId;

/**
 * An Infinispan ProtoStream adapter to marashall {@link XmlBeginEvent} instances.
 *
 * This class defines a factory for creating {@link XmlBeginEvent} instances when hydrating
 * records from the persisted datastore as well as field handlers to extract values
 * to be marshalled to the protocol buffer stream.
 *
 * The underlying protocol buffer record consists of the following structure:
 * <pre>
 *      message XmlBeginEvent {
 *          // structure of the super type, DmlEventAdapter
 *          required string columnName = 8;
 *      }
 * </pre>
 *
 * @author Chris Cranford
 */
@ProtoAdapter(XmlBeginEvent.class)
public class XmlBeginEventAdapter extends DmlEventAdapter {

    /**
     * A ProtoStream factory that creates {@link XmlBeginEvent} instances.
     *
     * @param eventType the event type
     * @param scn the system change number, must not be {@code null}
     * @param tableId the fully-qualified table name
     * @param rowId the Oracle row-id the change is associated with
     * @param rsId the Oracle rollback segment identifier
     * @param changeTime the time the change occurred
     * @param entry the parsed SQL statement entry
     * @param columnName the column name references by the SelectLobLocatorEvent
     * @return the constructed SelectLobLocatorEvent
     */
    @ProtoFactory
    public XmlBeginEvent factory(int eventType, String scn, String tableId, String rowId, String rsId, String changeTime,
                                 LogMinerDmlEntryImpl entry, String columnName) {
        return new XmlBeginEvent(EventType.from(eventType), Scn.valueOf(scn), TableId.parse(tableId), rowId, rsId,
                Instant.parse(changeTime), entry, columnName);
    }

    /**
     * A ProtoStream handler to extract the {@code columnName} field from a {@link XmlBeginEvent} type.
     *
     * @param event the event instance, must not be {@code null}
     * @return the column name
     */
    @ProtoField(number = 8, required = true)
    public String getColumnName(XmlBeginEvent event) {
        return event.getColumnName();
    }

}
