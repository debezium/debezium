/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.chronicle.serialization;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.logminer.buffered.chronicle.ChronicleEventSerializer;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.ExtendedStringWriteEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;

import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;

/**
 * Serializer for {@link ExtendedStringWriteEvent}.
 *
 * @author Debezium Authors
 */
public class ExtendedStringWriteEventSerializer extends ChronicleEventSerializer.BaseEventSerializer {

    @Override
    public byte getTypeId() {
        return ChronicleEventSerializer.TYPE_EXTENDED_STRING_WRITE;
    }

    @Override
    public void writeEvent(ValueOut valueOut, LogMinerEvent event) {
        if (!(event instanceof ExtendedStringWriteEvent)) {
            throw new DebeziumException("Expected ExtendedStringWriteEvent but got " + event.getClass().getSimpleName());
        }

        ExtendedStringWriteEvent writeEvent = (ExtendedStringWriteEvent) event;

        writeCommonFields(valueOut, event);
        writeString(valueOut, writeEvent.getData());
    }

    @Override
    public LogMinerEvent readEvent(ValueIn valueIn) {
        byte eventTypeOrdinal = valueIn.int8();
        EventType eventType = EventType.values()[eventTypeOrdinal];

        LogMinerEvent baseEvent = readCommonFields(valueIn, eventType);
        String data = readString(valueIn);

        return new ExtendedStringWriteEvent(eventType, baseEvent.getScn(), baseEvent.getTableId(),
                baseEvent.getRowId(), baseEvent.getRsId(), baseEvent.getChangeTime(), data);
    }
}
