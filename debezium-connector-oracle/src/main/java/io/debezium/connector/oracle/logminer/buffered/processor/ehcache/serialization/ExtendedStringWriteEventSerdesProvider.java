/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.processor.ehcache.serialization;

import java.io.IOException;

import io.debezium.connector.oracle.logminer.events.ExtendedStringWriteEvent;

/**
 * A specialized implementation of {@link SerdesProvider} for {@link ExtendedStringWriteEvent} types.
 *
 * @author Chris Cranford
 */
public class ExtendedStringWriteEventSerdesProvider<T extends ExtendedStringWriteEvent> extends LogMinerEventSerdesProvider<T> {
    @Override
    public Class<?> getJavaType() {
        return ExtendedStringWriteEvent.class;
    }

    @Override
    public void serialize(T event, SerializerOutputStream stream) throws IOException {
        super.serialize(event, stream);

        stream.writeString(event.getData());
    }

    @Override
    public void deserialize(DeserializationContext context, SerializerInputStream stream) throws IOException {
        super.deserialize(context, stream);

        context.addValue(stream.readString());
    }
}
