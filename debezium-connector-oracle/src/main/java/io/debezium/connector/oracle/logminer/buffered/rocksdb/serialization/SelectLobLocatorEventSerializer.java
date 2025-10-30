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
import io.debezium.connector.oracle.logminer.events.SelectLobLocatorEvent;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntryImpl;

/**
 * Serializer for SelectLobLocatorEvent.
 */
public class SelectLobLocatorEventSerializer extends RocksDbEventSerializer.BaseEventSerializer {

    // reuse helpers from BaseEventSerializer

    @Override
    public byte getTypeId() {
        return RocksDbEventSerializer.TYPE_SELECT_LOB_LOCATOR;
    }

    @Override
    public void writeEvent(DataOutputStream dos, LogMinerEvent event) throws IOException {
        if (!(event instanceof SelectLobLocatorEvent)) {
            throw new DebeziumException("Expected SelectLobLocatorEvent but got " + event.getClass().getSimpleName());
        }

        SelectLobLocatorEvent selectLobEvent = (SelectLobLocatorEvent) event;

        writeCommonFields(dos, event);

        writeString(dos, selectLobEvent.getColumnName());
        dos.writeBoolean(selectLobEvent.isBinary());

        writeDmlEntry(dos, selectLobEvent.getDmlEntry());
    }

    @Override
    public LogMinerEvent readEvent(DataInputStream dis) throws IOException {
        byte eventTypeOrdinal = dis.readByte();
        EventType eventType = EventType.values()[eventTypeOrdinal];

        LogMinerEvent baseEvent = readCommonFields(dis, eventType);

        String columnName = readString(dis);
        boolean binary = dis.readBoolean();

        LogMinerDmlEntryImpl dmlEntry = readDmlEntry(dis, eventType);

        return new SelectLobLocatorEvent(eventType, baseEvent.getScn(), baseEvent.getTableId(),
                baseEvent.getRowId(), baseEvent.getRsId(), baseEvent.getChangeTime(),
                dmlEntry, columnName, binary);
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
