/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.ehcache.serialization;

import java.io.IOException;

import io.debezium.connector.oracle.logminer.events.DmlEvent;

/**
 * A specialized implementation of {@link SerdesProvider} for {@link DmlEvent} types.
 *
 * @author Chris Cranford
 */
public class DmlEventSerdesProvider<T extends DmlEvent> extends LogMinerEventSerdesProvider<T> {
    @Override
    public Class<?> getJavaType() {
        return DmlEvent.class;
    }

    @Override
    public void serialize(DmlEvent event, SerializerOutputStream stream) throws IOException {
        super.serialize(event, stream);

        stream.writeObjectArray(event.getNewValues());
        stream.writeObjectArray(event.getOldValues());
    }

    @Override
    public void deserialize(DeserializationContext context, SerializerInputStream stream) throws IOException {
        super.deserialize(context, stream);

        final Object[] newValues = stream.readObjectArray();
        final Object[] oldValues = stream.readObjectArray();

        context.addValue(oldValues);
        context.addValue(newValues);
    }
}
