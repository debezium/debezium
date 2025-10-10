/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.chronicle.serialization;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.logminer.buffered.chronicle.ChronicleEventSerializer;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.ExtendedStringBeginEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntryImpl;

import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;

/**
 * Serializer for ExtendedStringBeginEvent.
 */
public class ExtendedStringBeginEventSerializer extends ChronicleEventSerializer.BaseEventSerializer {

    @Override
    public byte getTypeId() {
        return ChronicleEventSerializer.TYPE_EXTENDED_STRING_BEGIN;
    }

    @Override
    public void writeEvent(ValueOut valueOut, LogMinerEvent event) {
        if (!(event instanceof ExtendedStringBeginEvent)) {
            throw new DebeziumException("Expected ExtendedStringBeginEvent but got " + event.getClass().getSimpleName());
        }

        ExtendedStringBeginEvent extendedStringEvent = (ExtendedStringBeginEvent) event;

        writeCommonFields(valueOut, event);

        writeString(valueOut, extendedStringEvent.getColumnName());

        writeDmlEntry(valueOut, extendedStringEvent.getDmlEntry());
    }

    @Override
    public LogMinerEvent readEvent(ValueIn valueIn) {
        byte eventTypeOrdinal = valueIn.int8();
        EventType eventType = EventType.values()[eventTypeOrdinal];

        LogMinerEvent baseEvent = readCommonFields(valueIn, eventType);

        String columnName = readString(valueIn);

        LogMinerDmlEntryImpl dmlEntry = readDmlEntry(valueIn, eventType);

        return new ExtendedStringBeginEvent(eventType, baseEvent.getScn(), baseEvent.getTableId(),
                baseEvent.getRowId(), baseEvent.getRsId(), baseEvent.getChangeTime(),
                dmlEntry, columnName);
    }
}
