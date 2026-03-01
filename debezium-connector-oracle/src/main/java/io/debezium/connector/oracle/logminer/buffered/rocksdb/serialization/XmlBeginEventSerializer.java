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
import io.debezium.connector.oracle.logminer.events.XmlBeginEvent;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntryImpl;

/**
 * Serializer for XmlBeginEvent.
 */
public class XmlBeginEventSerializer extends RocksDbEventSerializer.BaseEventSerializer {

    @Override
    public byte getTypeId() {
        return RocksDbEventSerializer.TYPE_XML_BEGIN;
    }

    @Override
    public void writeEvent(DataOutputStream dataOutput, LogMinerEvent event) throws IOException {
        if (!(event instanceof XmlBeginEvent)) {
            throw new DebeziumException("Expected XmlBeginEvent but got " + event.getClass().getSimpleName());
        }

        XmlBeginEvent xmlBeginEvent = (XmlBeginEvent) event;

        writeCommonFields(dataOutput, event);

        writeString(dataOutput, xmlBeginEvent.getColumnName());

        writeDmlEntry(dataOutput, xmlBeginEvent.getDmlEntry());
    }

    @Override
    public LogMinerEvent readEvent(DataInputStream dataInput) throws IOException {
        byte eventTypeOrdinal = dataInput.readByte();
        EventType eventType = EventType.values()[eventTypeOrdinal];

        LogMinerEvent baseEvent = readCommonFields(dataInput, eventType);

        String columnName = readString(dataInput);

        LogMinerDmlEntryImpl dmlEntry = readDmlEntry(dataInput, eventType);

        return new XmlBeginEvent(eventType, baseEvent.getScn(), baseEvent.getTableId(),
                baseEvent.getRowId(), baseEvent.getRsId(), baseEvent.getChangeTime(),
                dmlEntry, columnName);
    }

    private void writeDmlEntry(DataOutputStream dataOutput, LogMinerDmlEntry dmlEntry) throws IOException {
        writeValuesArray(dataOutput, dmlEntry.getOldValues());
        writeValuesArray(dataOutput, dmlEntry.getNewValues());
        writeString(dataOutput, dmlEntry.getObjectOwner());
        writeString(dataOutput, dmlEntry.getObjectName());
    }

    private LogMinerDmlEntryImpl readDmlEntry(DataInputStream dataInput, EventType eventType) throws IOException {
        Object[] oldValues = readValuesArray(dataInput);
        Object[] newValues = readValuesArray(dataInput);
        String objectOwner = readString(dataInput);
        String objectName = readString(dataInput);

        return new LogMinerDmlEntryImpl(eventType.getValue(), newValues, oldValues, objectOwner, objectName);
    }
}
