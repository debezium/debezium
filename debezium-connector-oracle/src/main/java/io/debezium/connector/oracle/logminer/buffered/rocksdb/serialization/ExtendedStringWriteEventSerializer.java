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
import io.debezium.connector.oracle.logminer.events.ExtendedStringWriteEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;

/**
 * Serializer for {@link ExtendedStringWriteEvent}.
 */
public class ExtendedStringWriteEventSerializer extends RocksDbEventSerializer.BaseEventSerializer {

    @Override
    public byte getTypeId() {
        return RocksDbEventSerializer.TYPE_EXTENDED_STRING_WRITE;
    }

    @Override
    public void writeEvent(DataOutputStream dos, LogMinerEvent event) throws IOException {
        if (!(event instanceof ExtendedStringWriteEvent)) {
            throw new DebeziumException("Expected ExtendedStringWriteEvent but got " + event.getClass().getSimpleName());
        }

        ExtendedStringWriteEvent writeEvent = (ExtendedStringWriteEvent) event;

        writeCommonFields(dos, writeEvent);

        writeString(dos, writeEvent.getData());
    }

    @Override
    public LogMinerEvent readEvent(DataInputStream dis) throws IOException {
        byte eventTypeOrdinal = dis.readByte();
        EventType eventType = EventType.values()[eventTypeOrdinal];

        LogMinerEvent baseEvent = readCommonFields(dis, eventType);

        String data = readString(dis);

        return new ExtendedStringWriteEvent(eventType, baseEvent.getScn(), baseEvent.getTableId(),
                baseEvent.getRowId(), baseEvent.getRsId(), baseEvent.getChangeTime(), data);
    }

}
