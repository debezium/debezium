/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.chronicle.serialization;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.logminer.buffered.chronicle.ChronicleEventSerializer;
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntryImpl;

import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;

/**
 * Serializer for DML events
 */
public class DmlEventSerializer extends ChronicleEventSerializer.BaseEventSerializer {

    @Override
    public byte getTypeId() {
        return ChronicleEventSerializer.TYPE_DML_EVENT;
    }

    @Override
    public void writeEvent(ValueOut valueOut, LogMinerEvent event) {
        if (!(event instanceof DmlEvent)) {
            throw new DebeziumException("Expected DmlEvent but got " + event.getClass().getSimpleName());
        }

        DmlEvent dmlEvent = (DmlEvent) event;
        LogMinerDmlEntry dmlEntry = dmlEvent.getDmlEntry();

        writeCommonFields(valueOut, event);

        writeValuesArray(valueOut, dmlEntry.getOldValues());

        writeValuesArray(valueOut, dmlEntry.getNewValues());

        writeString(valueOut, dmlEntry.getObjectOwner());
        writeString(valueOut, dmlEntry.getObjectName());
    }

    @Override
    public LogMinerEvent readEvent(ValueIn valueIn) {
        byte eventTypeOrdinal = valueIn.int8();
        EventType eventType = EventType.values()[eventTypeOrdinal];

        LogMinerEvent baseEvent = readCommonFields(valueIn, eventType);

        Object[] oldValues = readValuesArray(valueIn);

        Object[] newValues = readValuesArray(valueIn);

        String objectOwner = readString(valueIn);
        String objectName = readString(valueIn);

        LogMinerDmlEntry dmlEntry = new LogMinerDmlEntryImpl(
                eventType.getValue(), newValues, oldValues, objectOwner, objectName);

        return new DmlEvent(eventType, baseEvent.getScn(), baseEvent.getTableId(),
                baseEvent.getRowId(), baseEvent.getRsId(), baseEvent.getChangeTime(),
                dmlEntry);
    }
}
