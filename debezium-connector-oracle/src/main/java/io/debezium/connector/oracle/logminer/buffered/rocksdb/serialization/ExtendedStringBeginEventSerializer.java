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
import io.debezium.connector.oracle.logminer.events.ExtendedStringBeginEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntryImpl;

/**
 * Serializer for {@link ExtendedStringBeginEvent}.
 */
public class ExtendedStringBeginEventSerializer extends RocksDbEventSerializer.BaseEventSerializer {

    @Override
    public byte getTypeId() {
        return RocksDbEventSerializer.TYPE_EXTENDED_STRING_BEGIN;
    }

    @Override
    public void writeEvent(DataOutputStream dos, LogMinerEvent event) throws IOException {
        if (!(event instanceof ExtendedStringBeginEvent)) {
            throw new DebeziumException("Expected ExtendedStringBeginEvent but got " + event.getClass().getSimpleName());
        }

        ExtendedStringBeginEvent extendedStringEvent = (ExtendedStringBeginEvent) event;

        writeCommonFields(dos, event);

        writeString(dos, extendedStringEvent.getColumnName());

        // Write DML entry
        writeDmlEntry(dos, extendedStringEvent.getDmlEntry());
    }

    @Override
    public LogMinerEvent readEvent(DataInputStream dis) throws IOException {
        // Read event type
        byte eventTypeOrdinal = dis.readByte();
        EventType eventType = EventType.values()[eventTypeOrdinal];

        LogMinerEvent baseEvent = readCommonFields(dis, eventType);

        String columnName = readString(dis);

        LogMinerDmlEntryImpl dmlEntry = readDmlEntry(dis, eventType);

        return new ExtendedStringBeginEvent(eventType, baseEvent.getScn(), baseEvent.getTableId(),
                baseEvent.getRowId(), baseEvent.getRsId(), baseEvent.getChangeTime(),
                dmlEntry, columnName);
    }

    private void writeDmlEntry(DataOutputStream dos, LogMinerDmlEntry dmlEntry) throws IOException {
        writeValuesArray(dos, dmlEntry.getOldValues());
        writeValuesArray(dos, dmlEntry.getNewValues());
        writeString(dos, dmlEntry.getObjectOwner());
        writeString(dos, dmlEntry.getObjectName());
    }

    private LogMinerDmlEntryImpl readDmlEntry(DataInputStream dis, EventType eventType) throws IOException {
        Object[] oldValues = readValuesArray(dis);
        Object[] newValues = readValuesArray(dis);
        String objectOwner = readString(dis);
        String objectName = readString(dis);

        return new LogMinerDmlEntryImpl(eventType.getValue(), newValues, oldValues, objectOwner, objectName);
    }
}
