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
import io.debezium.connector.oracle.logminer.events.LobWriteEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;

/**
 * Serializer for {@link LobWriteEvent}.
 */
public class LobWriteEventSerializer extends RocksDbEventSerializer.BaseEventSerializer {

    @Override
    public byte getTypeId() {
        return RocksDbEventSerializer.TYPE_LOB_WRITE;
    }

    @Override
    public void writeEvent(DataOutputStream dos, LogMinerEvent event) throws IOException {
        if (!(event instanceof LobWriteEvent)) {
            throw new DebeziumException("Expected LobWriteEvent but got " + event.getClass().getSimpleName());
        }

        LobWriteEvent lobWriteEvent = (LobWriteEvent) event;

        writeCommonFields(dos, lobWriteEvent);

        writeString(dos, lobWriteEvent.getData());
        dos.writeInt(lobWriteEvent.getOffset());
        dos.writeInt(lobWriteEvent.getLength());
    }

    @Override
    public LogMinerEvent readEvent(DataInputStream dis) throws IOException {
        byte eventTypeOrdinal = dis.readByte();
        EventType eventType = EventType.values()[eventTypeOrdinal];

        LogMinerEvent baseEvent = readCommonFields(dis, eventType);

        String data = readString(dis);
        int offset = dis.readInt();
        int length = dis.readInt();

        return new LobWriteEvent(eventType, baseEvent.getScn(), baseEvent.getTableId(),
                baseEvent.getRowId(), baseEvent.getRsId(), baseEvent.getChangeTime(),
                data, offset, length);
    }

}
