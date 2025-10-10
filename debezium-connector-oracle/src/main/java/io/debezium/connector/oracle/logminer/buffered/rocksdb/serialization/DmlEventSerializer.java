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
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntryImpl;

/**
 * RocksDB serializer for DML events.
 *
 * @author Debezium Authors
 */
public class DmlEventSerializer extends io.debezium.connector.oracle.logminer.buffered.rocksdb.RocksDbEventSerializer.BaseEventSerializer {

    @Override
    public byte getTypeId() {
        return RocksDbEventSerializer.TYPE_DML_EVENT;
    }

    @Override
    public void writeEvent(DataOutputStream out, LogMinerEvent event) throws IOException {
        if (!(event instanceof DmlEvent)) {
            throw new DebeziumException("Expected DmlEvent but got " + event.getClass().getName());
        }
        DmlEvent dmlEvent = (DmlEvent) event;
        LogMinerDmlEntry dmlEntry = dmlEvent.getDmlEntry();

        // use inherited helper
        writeCommonFields(out, event);

        // write dml entry values and object details
        writeValuesArray(out, dmlEntry.getOldValues());
        writeValuesArray(out, dmlEntry.getNewValues());
        writeString(out, dmlEntry.getObjectOwner());
        writeString(out, dmlEntry.getObjectName());
    }

    @Override
    public LogMinerEvent readEvent(DataInputStream in) throws IOException {
        byte eventTypeOrdinal = in.readByte();
        EventType eventType = EventType.values()[eventTypeOrdinal];

        LogMinerEvent baseEvent = readCommonFields(in, eventType);

        Object[] oldValues = readValuesArray(in);
        Object[] newValues = readValuesArray(in);
        String objectOwner = readString(in);
        String objectName = readString(in);

        LogMinerDmlEntry dmlEntry = new LogMinerDmlEntryImpl(eventType.getValue(), newValues, oldValues, objectOwner, objectName);

        return new DmlEvent(eventType, baseEvent.getScn(), baseEvent.getTableId(), baseEvent.getRowId(), baseEvent.getRsId(), baseEvent.getChangeTime(), dmlEntry);
    }
}
