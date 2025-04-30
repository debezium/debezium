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
import io.debezium.connector.oracle.logminer.events.XmlWriteEvent;
import io.debezium.relational.TableId;

/**
 * A LogMiner event that represents a {@code XML_WRITE} event type.
 *
 * @author Chris Cranford
 */
@ProtoAdapter(XmlWriteEvent.class)
public class XmlWriteEventAdapter extends LogMinerEventAdapter {

    /**
     * A ProtoStream factory that creates {@link XmlWriteEvent} instances.
     *
     * @param eventType the event type
     * @param scn the system change number, must not be {@code null}
     * @param tableId the fully-qualified table name
     * @param rowId the Oracle row-id the change is associated with
     * @param rsId the Oracle rollback segment identifier
     * @param changeTime the time the change occurred
     * @param xml the xml document
     * @param length the length of the document
     * @return the constructed DmlEvent
     */
    @ProtoFactory
    public XmlWriteEvent factory(int eventType, String scn, String tableId, String rowId, String rsId, String changeTime, String xml, Integer length) {
        return new XmlWriteEvent(EventType.from(eventType), Scn.valueOf(scn), TableId.parse(tableId), rowId, rsId, Instant.parse(changeTime), xml, length);
    }

    /**
     * A ProtoStream handler to extract the {@code xml} field from the {@link XmlWriteEvent}.
     *
     * @param event the event instance, must not be {@code null}
     * @return the xml
     */
    @ProtoField(number = 7)
    public String getXml(XmlWriteEvent event) {
        return event.getXml();
    }

    /**
     * A ProtoStream handler to extract the {@code length} of the XML field from the {@link XmlWriteEvent}.
     *
     * @param event the event instance, must not be {@code null}
     * @return the length, never {@code null}
     */
    @ProtoField(number = 8, required = true)
    public Integer getLength(XmlWriteEvent event) {
        return event.getLength();
    }

}
