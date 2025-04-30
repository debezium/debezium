/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.ehcache.serialization;

import java.io.IOException;

import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;

/**
 * A specialized implementation of {@link SerdesProvider} for {@link LogMinerEvent} types.
 *
 * @author Chris Cranford
 */
public class LogMinerEventSerdesProvider<T extends LogMinerEvent> implements SerdesProvider<T> {
    @Override
    public Class<?> getJavaType() {
        return LogMinerEvent.class;
    }

    @Override
    public void serialize(LogMinerEvent event, SerializerOutputStream stream) throws IOException {
        stream.writeInt(event.getEventType().getValue());
        stream.writeScn(event.getScn());
        stream.writeTableId(event.getTableId());
        stream.writeString(event.getRowId());
        stream.writeString(event.getRsId());
        stream.writeInstant(event.getChangeTime());
    }

    @Override
    public void deserialize(DeserializationContext context, SerializerInputStream stream) throws IOException {
        context.addValue(EventType.from(stream.readInt()));
        context.addValue(stream.readScn());
        context.addValue(stream.readTableId());
        context.addValue(stream.readString());
        context.addValue(stream.readString());
        context.addValue(stream.readInstant());
    }
}
