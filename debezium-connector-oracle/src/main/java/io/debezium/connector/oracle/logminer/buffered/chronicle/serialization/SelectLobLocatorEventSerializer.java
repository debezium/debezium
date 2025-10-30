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
import io.debezium.connector.oracle.logminer.events.SelectLobLocatorEvent;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntryImpl;

import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;

/**
 * Serializer for SelectLobLocatorEvent.
 */
public class SelectLobLocatorEventSerializer extends ChronicleEventSerializer.BaseEventSerializer {

    @Override
    public byte getTypeId() {
        return ChronicleEventSerializer.TYPE_SELECT_LOB_LOCATOR;
    }

    @Override
    public void writeEvent(ValueOut valueOut, LogMinerEvent event) {
        if (!(event instanceof SelectLobLocatorEvent)) {
            throw new DebeziumException("Expected SelectLobLocatorEvent but got " + event.getClass().getSimpleName());
        }

        SelectLobLocatorEvent selectLobEvent = (SelectLobLocatorEvent) event;

        writeCommonFields(valueOut, event);

        writeString(valueOut, selectLobEvent.getColumnName());
        valueOut.bool(selectLobEvent.isBinary());

        writeDmlEntry(valueOut, selectLobEvent.getDmlEntry());
    }

    @Override
    public LogMinerEvent readEvent(ValueIn valueIn) {
        byte eventTypeOrdinal = valueIn.int8();
        EventType eventType = EventType.values()[eventTypeOrdinal];

        LogMinerEvent baseEvent = readCommonFields(valueIn, eventType);

        String columnName = readString(valueIn);
        boolean binary = valueIn.bool();

        LogMinerDmlEntryImpl dmlEntry = readDmlEntry(valueIn, eventType);

        return new SelectLobLocatorEvent(eventType, baseEvent.getScn(), baseEvent.getTableId(),
                baseEvent.getRowId(), baseEvent.getRsId(), baseEvent.getChangeTime(),
                dmlEntry, columnName, binary);
    }
}
