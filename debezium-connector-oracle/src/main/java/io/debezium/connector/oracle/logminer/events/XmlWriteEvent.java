/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.events;

import java.time.Instant;

import io.debezium.connector.oracle.Scn;
import io.debezium.relational.TableId;

/**
 * An event that represents a write operation of an XML document in the event stream.
 *
 * @author Chris Cranford
 */
public class XmlWriteEvent extends LogMinerEvent {

    private final String xml;
    private final int length;

    public XmlWriteEvent(LogMinerEventRow row, String xml, Integer length) {
        super(row);
        this.xml = xml;
        this.length = length;
    }

    public XmlWriteEvent(EventType eventType, Scn scn, TableId tableId, String rowId, String rsId, Instant changeTime, String xml, Integer length) {
        super(eventType, scn, tableId, rowId, rsId, changeTime);
        this.xml = xml;
        this.length = length;
    }

    public String getXml() {
        return xml;
    }

    public Integer getLength() {
        return length;
    }

    @Override
    public String toString() {
        return "XmlWriteEvent{" +
                "xml='" + xml + '\'' +
                "} " + super.toString();
    }
}
