/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.chronicle.serialization;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.logminer.buffered.chronicle.ChronicleEventSerializer;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LobWriteEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;

import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;

/**
 * Serializer for LobWriteEvent.
 */
public class LobWriteEventSerializer extends ChronicleEventSerializer.BaseEventSerializer {

    @Override
    public byte getTypeId() {
        return ChronicleEventSerializer.TYPE_LOB_WRITE;
    }

    @Override
    public void writeEvent(ValueOut valueOut, LogMinerEvent event) {
        if (!(event instanceof LobWriteEvent)) {
            throw new DebeziumException("Expected LobWriteEvent but got " + event.getClass().getSimpleName());
        }

        LobWriteEvent lobWriteEvent = (LobWriteEvent) event;

        writeCommonFields(valueOut, event);

        writeString(valueOut, lobWriteEvent.getData());
        valueOut.int32(lobWriteEvent.getOffset());
        valueOut.int32(lobWriteEvent.getLength());
    }

    @Override
    public LogMinerEvent readEvent(ValueIn valueIn) {
        byte eventTypeOrdinal = valueIn.int8();
        EventType eventType = EventType.values()[eventTypeOrdinal];

        LogMinerEvent baseEvent = readCommonFields(valueIn, eventType);

        String data = readString(valueIn);
        int offset = valueIn.int32();
        int length = valueIn.int32();

        return new LobWriteEvent(eventType, baseEvent.getScn(), baseEvent.getTableId(),
                baseEvent.getRowId(), baseEvent.getRsId(), baseEvent.getChangeTime(),
                data, offset, length);
    }
}
