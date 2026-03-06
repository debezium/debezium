/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.chronicle.serialization;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.logminer.buffered.chronicle.ChronicleEventSerializer;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LobEraseEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;

import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;

/**
 * Serializer for {@link LobEraseEvent}.
 *
 * @author Debezium Authors
 */
public class LobEraseEventSerializer extends ChronicleEventSerializer.BaseEventSerializer {

    @Override
    public byte getTypeId() {
        return ChronicleEventSerializer.TYPE_LOB_ERASE;
    }

    @Override
    public void writeEvent(ValueOut valueOut, LogMinerEvent event) {
        if (!(event instanceof LobEraseEvent)) {
            throw new DebeziumException("Expected LobEraseEvent but got " + event.getClass().getSimpleName());
        }

        // LobEraseEvent has no additional fields beyond the base LogMinerEvent
        writeCommonFields(valueOut, event);
    }

    @Override
    public LogMinerEvent readEvent(ValueIn valueIn) {
        byte eventTypeOrdinal = valueIn.int8();
        EventType eventType = EventType.values()[eventTypeOrdinal];

        LogMinerEvent baseEvent = readCommonFields(valueIn, eventType);

        return new LobEraseEvent(eventType, baseEvent.getScn(), baseEvent.getTableId(),
                baseEvent.getRowId(), baseEvent.getRsId(), baseEvent.getChangeTime());
    }
}
