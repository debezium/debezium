/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.rocksdb.serialization;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import io.debezium.connector.oracle.logminer.buffered.rocksdb.RocksDbEventSerializer;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LobEraseEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;

/**
 * Serializer for {@link LobEraseEvent} objects.
 *
 * @author Debezium Authors
 */
public class LobEraseEventSerializer extends RocksDbEventSerializer.BaseEventSerializer {

    @Override
    public byte getTypeId() {
        return RocksDbEventSerializer.TYPE_LOB_ERASE;
    }

    @Override
    public void writeEvent(DataOutputStream dataOutput, LogMinerEvent event) throws IOException {
        if (!(event instanceof LobEraseEvent)) {
            throw new IllegalArgumentException("Expected LobEraseEvent but got " + event.getClass().getSimpleName());
        }

        writeCommonFields(dataOutput, event);
    }

    @Override
    public LogMinerEvent readEvent(DataInputStream dataInput) throws IOException {
        byte eventTypeOrdinal = dataInput.readByte();
        EventType eventType = EventType.values()[eventTypeOrdinal];

        LogMinerEvent baseEvent = readCommonFields(dataInput, eventType);

        return new LobEraseEvent(eventType, baseEvent.getScn(), baseEvent.getTableId(),
                baseEvent.getRowId(), baseEvent.getRsId(), baseEvent.getChangeTime());
    }
}
