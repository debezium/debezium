/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.ehcache.serialization;

import java.io.IOException;

import io.debezium.connector.oracle.logminer.events.XmlWriteEvent;

/**
 * A specialized implementation of {@link SerdesProvider} for {@link XmlWriteEvent} types.
 *
 * @author Chris Cranford
 */
public class XmlWriteEventSerdesProvider<T extends XmlWriteEvent> extends LogMinerEventSerdesProvider<T> {
    @Override
    public Class<?> getJavaType() {
        return XmlWriteEvent.class;
    }

    @Override
    public void serialize(XmlWriteEvent event, SerializerOutputStream stream) throws IOException {
        super.serialize(event, stream);

        stream.writeString(event.getXml());
        stream.writeInt(event.getLength());
    }

    @Override
    public void deserialize(DeserializationContext context, SerializerInputStream stream) throws IOException {
        super.deserialize(context, stream);

        context.addValue(stream.readString());
        context.addValue(stream.readInt());
    }
}
