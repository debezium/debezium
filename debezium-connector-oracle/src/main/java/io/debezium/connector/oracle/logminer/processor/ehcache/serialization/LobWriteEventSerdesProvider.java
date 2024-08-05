/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.ehcache.serialization;

import java.io.IOException;

import io.debezium.connector.oracle.logminer.events.LobWriteEvent;

/**
 * A specialized implementation of {@link SerdesProvider} for {@link LobWriteEvent} types.
 *
 * @author Chris Cranford
 */
public class LobWriteEventSerdesProvider<T extends LobWriteEvent> extends LogMinerEventSerdesProvider<T> {
    @Override
    public Class<?> getJavaType() {
        return LobWriteEvent.class;
    }

    @Override
    public void serialize(LobWriteEvent event, SerializerOutputStream stream) throws IOException {
        super.serialize(event, stream);

        stream.writeString(event.getData());
        stream.writeInt(event.getOffset());
        stream.writeInt(event.getLength());
    }

    @Override
    public void deserialize(DeserializationContext context, SerializerInputStream stream) throws IOException {
        super.deserialize(context, stream);

        context.addValue(stream.readString());
        context.addValue(stream.readInt());
        context.addValue(stream.readInt());
    }
}
