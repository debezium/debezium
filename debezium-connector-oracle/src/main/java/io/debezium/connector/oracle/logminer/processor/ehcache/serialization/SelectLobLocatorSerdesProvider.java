/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.ehcache.serialization;

import java.io.IOException;

import io.debezium.connector.oracle.logminer.events.SelectLobLocatorEvent;

/**
 * A specialized implementation of {@link SerdesProvider} for {@link SelectLobLocatorEvent} types.
 *
 * @author Chris Cranford
 */
public class SelectLobLocatorSerdesProvider<T extends SelectLobLocatorEvent> extends DmlEventSerdesProvider<T> {
    @Override
    public Class<?> getJavaType() {
        return SelectLobLocatorEvent.class;
    }

    @Override
    public void serialize(SelectLobLocatorEvent event, SerializerOutputStream stream) throws IOException {
        super.serialize(event, stream);

        stream.writeString(event.getColumnName());
        stream.writeBoolean(event.isBinary());
    }

    @Override
    public void deserialize(DeserializationContext context, SerializerInputStream stream) throws IOException {
        super.deserialize(context, stream);

        context.addValue(stream.readString());
        context.addValue(stream.readBoolean());
    }
}
