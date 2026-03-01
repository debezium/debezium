/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.chronicle.serialization;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.logminer.buffered.chronicle.ChronicleEventSerializer;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.XmlEndEvent;

import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;

/**
 * Serializer for XmlEndEvent.
 */
public class XmlEndEventSerializer extends ChronicleEventSerializer.BaseEventSerializer {

    @Override
    public byte getTypeId() {
        return ChronicleEventSerializer.TYPE_XML_END;
    }

    @Override
    public void writeEvent(ValueOut valueOut, LogMinerEvent event) {
        if (!(event instanceof XmlEndEvent)) {
            throw new DebeziumException("Expected XmlEndEvent but got " + event.getClass().getSimpleName());
        }

        writeCommonFields(valueOut, event);
    }

    @Override
    public LogMinerEvent readEvent(ValueIn valueIn) {
        byte eventTypeOrdinal = valueIn.int8();
        EventType eventType = EventType.values()[eventTypeOrdinal];

        LogMinerEvent baseEvent = readCommonFields(valueIn, eventType);

        return new XmlEndEvent(eventType, baseEvent.getScn(), baseEvent.getTableId(),
                baseEvent.getRowId(), baseEvent.getRsId(), baseEvent.getChangeTime());
    }
}
