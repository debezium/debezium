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

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LobEraseEvent;
import io.debezium.connector.oracle.logminer.events.LobWriteEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.SelectLobLocatorEvent;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntryImpl;
import io.debezium.relational.TableId;
import io.debezium.util.Strings;

/**
 * An Infinispan ProtoStream adapter to marshall {@link LogMinerEvent} instances and its subclasses.
 *
 * This class defines a factory for creating {@link LogMinerEvent} instances and its subclasses for when
 * hydrating records from the persisted datastore as well as field handlers to extract instance values
 * to be marshalled to the protocol buffer stream.
 *
 * The underlying protocol buffer record consists of the following structure:
 * <pre>
 *     message LogMinerEvent {
 *          required int32 eventType = 1;
 *          required string scn = 2;
 *          required string tableId = 3;
 *          required string rowId = 4;
 *          required object rsId = 5;
 *          required string changeTime = 6;
 *          LogMinerDmlEntryImpl entry = 7;
 *          string columnName = 8;
 *          boolean binary = 9;
 *          string data = 10;
 *     }
 * </pre>
 * @author Chris Cranford
 */
@ProtoAdapter(LogMinerEvent.class)
public class LogMinerEventAdapter {

    /**
     * A ProtoStream factory that creates {@link LogMinerEvent} instances or one of its subclasses from field values.
     *
     * @param eventType the event type
     * @param scn the system change number, must not be {@code null}
     * @param tableId the fully-qualified table name
     * @param rowId the Oracle row-id the change is associated with
     * @param rsId the Oracle rollback segment identifier
     * @param changeTime the time the change occurred
     * @param entry the parsed SQL statement entry
     * @param columnName the column name for a {@code SEL_LOB_LOCATOR} event type
     * @param binary whether the data is binary for a {@code SEL_LOB_LOCATOR} event type
     * @param data the data to be written by a {@code LOB_WRITE} event type
     * @return the constructed LogMinerEvent or one of its subclasses
     */
    @ProtoFactory
    public LogMinerEvent factory(int eventType, String scn, String tableId, String rowId, String rsId, String changeTime,
                                 LogMinerDmlEntryImpl entry, String columnName, Boolean binary, String data) {
        final EventType type = EventType.from(eventType);
        final Scn eventScn = Scn.valueOf(scn);

        final TableId id;
        if (Strings.isNullOrEmpty(tableId)) {
            id = null;
        }
        else {
            id = TableId.parse(tableId);
        }

        final Instant time;
        if (Strings.isNullOrEmpty(changeTime)) {
            time = null;
        }
        else {
            time = Instant.parse(changeTime);
        }

        switch (type) {
            case INSERT:
            case UPDATE:
            case DELETE:
                return new DmlEvent(type, eventScn, id, rowId, rsId, time, entry);
            case SELECT_LOB_LOCATOR:
                return new SelectLobLocatorEvent(type, eventScn, id, rowId, rsId, time, entry, columnName, binary);
            case LOB_WRITE:
                return new LobWriteEvent(type, eventScn, id, rowId, rsId, time, data);
            case LOB_ERASE:
                return new LobEraseEvent(type, eventScn, id, rowId, rsId, time);
            case LOB_TRIM:
                return new LogMinerEvent(type, eventScn, id, rowId, rsId, time);
            default:
                throw new DebeziumException("Unknown event type: " + eventType);
        }
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
        // We intentionally serialize the TableId as a string since string values are natively supported
        // by protocol buffers and we don't need to write a special marshaller for the TableId class.
        return event.getTableId().identifier();
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
        // Serialized using Instant's normal toString() format since strings as handled natively by
        // protocol buffers and will be parsed using Instant#parse in the factory method.
        return event.getChangeTime().toString();
    }

    /**
     * A ProtoStream handler to extract the {@code entry} field from the {@link DmlEvent} and subclass types.
     *
     * @param event the event instance, must not be {@code null}
     * @return the LogMinerDmlEntryImpl instance or {@code null} if the event type isn't related to DML events
     */
    @ProtoField(number = 7)
    public LogMinerDmlEntryImpl getEntry(LogMinerEvent event) {
        switch (event.getEventType()) {
            case INSERT:
            case UPDATE:
            case DELETE:
            case SELECT_LOB_LOCATOR:
                return (LogMinerDmlEntryImpl) ((DmlEvent) event).getDmlEntry();
            default:
                return null;
        }
    }

    /**
     * A ProtoStream handler to extract the {@code columnName} field from a {@link SelectLobLocatorEvent} type.
     *
     * @param event the event instance, must not be {@code null}
     * @return the column name or {@code null} if the event is not a SelectLobLocatorEvent type
     */
    @ProtoField(number = 8)
    public String getColumnName(LogMinerEvent event) {
        if (EventType.SELECT_LOB_LOCATOR.equals(event.getEventType())) {
            return ((SelectLobLocatorEvent) event).getColumnName();
        }
        return null;
    }

    /**
     * A ProtoStream handler to extract the {@code binary} field from a {@link SelectLobLocatorEvent} type.
     *
     * @param event the event instance, must not be {@code null}
     * @return the binary data flag or {@code null} if the event is not a SelectLobLocatorEvent type
     */
    @ProtoField(number = 9)
    public Boolean getBinary(LogMinerEvent event) {
        if (EventType.SELECT_LOB_LOCATOR.equals(event.getEventType())) {
            return ((SelectLobLocatorEvent) event).isBinary();
        }
        return null;
    }

    /**
     * A ProtoStream handler to extract the {@code data} field from a {@link LobWriteEvent} type.
     *
     * @param event the event instance, must not be {@code null}
     * @return the data to be written for a LOB field or {@code null} if the event is not a LobWriteEvent type
     */
    @ProtoField(number = 10)
    public String getData(LogMinerEvent event) {
        if (EventType.LOB_WRITE.equals(event.getEventType())) {
            return ((LobWriteEvent) event).getData();
        }
        return null;
    }
}
