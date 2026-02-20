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
import io.debezium.connector.oracle.logminer.events.RedoSqlDmlEvent;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntryImpl;

import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;

/**
 * Serializer for RedoSqlDmlEvent.
 */
public class RedoSqlDmlEventSerializer extends ChronicleEventSerializer.BaseEventSerializer {

    @Override
    public byte getTypeId() {
        return ChronicleEventSerializer.TYPE_REDO_SQL_DML;
    }

    @Override
    public void writeEvent(ValueOut valueOut, LogMinerEvent event) {
        if (!(event instanceof RedoSqlDmlEvent)) {
            throw new DebeziumException("Expected RedoSqlDmlEvent but got " + event.getClass().getSimpleName());
        }

        RedoSqlDmlEvent redoSqlEvent = (RedoSqlDmlEvent) event;

        writeCommonFields(valueOut, event);

        writeString(valueOut, redoSqlEvent.getRedoSql());

        writeDmlEntry(valueOut, redoSqlEvent.getDmlEntry());
    }

    @Override
    public LogMinerEvent readEvent(ValueIn valueIn) {
        byte eventTypeOrdinal = valueIn.int8();
        EventType eventType = EventType.values()[eventTypeOrdinal];

        LogMinerEvent baseEvent = readCommonFields(valueIn, eventType);

        String redoSql = readString(valueIn);

        LogMinerDmlEntryImpl dmlEntry = readDmlEntry(valueIn, eventType);

        return new RedoSqlDmlEvent(eventType, baseEvent.getScn(), baseEvent.getTableId(),
                baseEvent.getRowId(), baseEvent.getRsId(), baseEvent.getChangeTime(),
                dmlEntry, redoSql);
    }
}
