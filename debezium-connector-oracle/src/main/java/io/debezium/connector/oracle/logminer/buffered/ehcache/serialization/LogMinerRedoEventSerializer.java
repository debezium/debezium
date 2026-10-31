/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.ehcache.serialization;

import java.io.IOException;

import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerRedoEvent;

/**
 * The Ehcache serializer for {@link LogMinerRedoEvent}, the one type the events cache stores. One fixed layout,
 * so no class name per entry and no constructor lookup.
 *
 * @author Chris Cranford
 */
public class LogMinerRedoEventSerializer extends AbstractEhcacheSerializer<LogMinerRedoEvent> {

    public LogMinerRedoEventSerializer(ClassLoader classLoader) {
    }

    @Override
    protected void serialize(LogMinerRedoEvent event, SerializerOutputStream stream) throws IOException {
        stream.writeInt(event.getEventType().getValue());
        stream.writeBoolean(event.isRollback());
        stream.writeScn(event.getScn());
        stream.writeTableId(event.getTableId());
        stream.writeString(event.getRowIdAsString());
        stream.writeString(event.getRsId());
        stream.writeInstant(event.getChangeTime());
        // Redo SQL with its continuation rows joined can exceed the 65,535 bytes writeString can hold
        stream.writeLongString(event.getRedoSql());
        stream.writeInt(event.getStatus());
        stream.writeString(event.getInfo());
        stream.writeLong(event.getTransactionSequence());
        stream.writeLong(event.getObjectId());
        stream.writeLong(event.getDataObjectId());
    }

    @Override
    protected LogMinerRedoEvent deserialize(SerializerInputStream stream) throws IOException {
        return new LogMinerRedoEvent(
                EventType.from(stream.readInt()),
                stream.readBoolean(),
                stream.readScn(),
                stream.readTableId(),
                stream.readString(),
                stream.readString(),
                stream.readInstant(),
                stream.readLongString(),
                stream.readInt(),
                stream.readString(),
                stream.readLong(),
                stream.readLong(),
                stream.readLong());
    }
}
