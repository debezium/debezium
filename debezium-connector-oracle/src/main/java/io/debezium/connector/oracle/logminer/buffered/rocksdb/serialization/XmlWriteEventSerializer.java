/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.rocksdb.serialization;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.logminer.buffered.rocksdb.RocksDbEventSerializer;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.XmlWriteEvent;

/**
 * Serializer for XmlWriteEvent.
 */
public class XmlWriteEventSerializer extends RocksDbEventSerializer.BaseEventSerializer {

    @Override
    public byte getTypeId() {
        return RocksDbEventSerializer.TYPE_XML_WRITE;
    }

    @Override
    public void writeEvent(DataOutputStream dataOutput, LogMinerEvent event) throws IOException {
        if (!(event instanceof XmlWriteEvent)) {
            throw new DebeziumException("Expected XmlWriteEvent but got " + event.getClass().getSimpleName());
        }

        XmlWriteEvent xmlWriteEvent = (XmlWriteEvent) event;

        writeCommonFields(dataOutput, event);

        writeString(dataOutput, xmlWriteEvent.getXml());
        dataOutput.writeInt(xmlWriteEvent.getLength());
    }

    @Override
    public LogMinerEvent readEvent(DataInputStream dataInput) throws IOException {
        byte eventTypeOrdinal = dataInput.readByte();
        EventType eventType = EventType.values()[eventTypeOrdinal];

        LogMinerEvent baseEvent = readCommonFields(dataInput, eventType);

        String xml = readString(dataInput);
        int length = dataInput.readInt();

        return new XmlWriteEvent(eventType, baseEvent.getScn(), baseEvent.getTableId(),
                baseEvent.getRowId(), baseEvent.getRsId(), baseEvent.getChangeTime(),
                xml, length);
    }
}
