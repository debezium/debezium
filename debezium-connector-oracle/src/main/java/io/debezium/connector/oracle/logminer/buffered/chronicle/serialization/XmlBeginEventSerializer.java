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
import io.debezium.connector.oracle.logminer.events.XmlBeginEvent;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;

import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;

/**
 * Serializer for XmlBeginEvent.
 */
public class XmlBeginEventSerializer extends ChronicleEventSerializer.BaseEventSerializer {

    @Override
    public byte getTypeId() {
        return ChronicleEventSerializer.TYPE_XML_BEGIN;
    }

    @Override
    public void writeEvent(ValueOut valueOut, LogMinerEvent event) {
        if (!(event instanceof XmlBeginEvent)) {
            throw new DebeziumException("Expected XmlBeginEvent but got " + event.getClass().getSimpleName());
        }

        XmlBeginEvent xmlBeginEvent = (XmlBeginEvent) event;

        writeCommonFields(valueOut, event);

        writeString(valueOut, xmlBeginEvent.getColumnName());

        writeDmlEntry(valueOut, xmlBeginEvent.getDmlEntry());
    }

    @Override
    public LogMinerEvent readEvent(ValueIn valueIn) {
        byte eventTypeOrdinal = valueIn.int8();
        EventType eventType = EventType.values()[eventTypeOrdinal];

        LogMinerEvent baseEvent = readCommonFields(valueIn, eventType);

        String columnName = readString(valueIn);

        LogMinerDmlEntry dmlEntry = readDmlEntry(valueIn, eventType);

        return new XmlBeginEvent(eventType, baseEvent.getScn(), baseEvent.getTableId(),
                baseEvent.getRowId(), baseEvent.getRsId(), baseEvent.getChangeTime(),
                dmlEntry, columnName);
    }
}
