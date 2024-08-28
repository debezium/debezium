/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.ehcache.serialization;

import java.io.IOException;

import io.debezium.connector.oracle.logminer.events.ExtendedStringBeginEvent;

/**
 * A specialized implementation of {@link SerdesProvider} for {@link ExtendedStringBeginEvent} types.
 *
 * @author Chris Cranford
 */
public class ExtendedStringBeginEventSerdesProvider<T extends ExtendedStringBeginEvent> extends DmlEventSerdesProvider<T> {
    @Override
    public Class<?> getJavaType() {
        return ExtendedStringBeginEvent.class;
    }

    @Override
    public void serialize(T event, SerializerOutputStream stream) throws IOException {
        super.serialize(event, stream);

        stream.writeString(event.getColumnName());
    }

    @Override
    public void deserialize(DeserializationContext context, SerializerInputStream stream) throws IOException {
        super.deserialize(context, stream);

        context.addValue(stream.readString());
    }
}
