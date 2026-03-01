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
import io.debezium.connector.oracle.logminer.events.TruncateEvent;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntryImpl;

import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;

/**
 * Serializer for TruncateEvent.
 */
public class TruncateEventSerializer extends ChronicleEventSerializer.BaseEventSerializer {

    @Override
    public byte getTypeId() {
        return ChronicleEventSerializer.TYPE_TRUNCATE;
    }

    @Override
    public void writeEvent(ValueOut valueOut, LogMinerEvent event) {
        if (!(event instanceof TruncateEvent)) {
            throw new DebeziumException("Expected TruncateEvent but got " + event.getClass().getSimpleName());
        }

        TruncateEvent truncateEvent = (TruncateEvent) event;

        writeCommonFields(valueOut, event);

        writeDmlEntry(valueOut, truncateEvent.getDmlEntry());
    }

    @Override
    public LogMinerEvent readEvent(ValueIn valueIn) {
        byte eventTypeOrdinal = valueIn.int8();
        EventType eventType = EventType.values()[eventTypeOrdinal];

        LogMinerEvent baseEvent = readCommonFields(valueIn, eventType);

        LogMinerDmlEntryImpl dmlEntry = readDmlEntry(valueIn, eventType);

        return new TruncateEvent(eventType, baseEvent.getScn(), baseEvent.getTableId(),
                baseEvent.getRowId(), baseEvent.getRsId(), baseEvent.getChangeTime(),
                dmlEntry);
    }
}
