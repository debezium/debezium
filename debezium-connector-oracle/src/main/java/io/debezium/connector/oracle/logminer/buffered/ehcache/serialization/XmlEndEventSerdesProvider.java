/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.ehcache.serialization;

import java.io.IOException;

import io.debezium.connector.oracle.logminer.events.XmlEndEvent;

/**
 * A specialized implementation of {@link SerdesProvider} for {@link XmlEndEvent} types.
 *
 * @author Chris Cranford
 */
public class XmlEndEventSerdesProvider<T extends XmlEndEvent> extends LogMinerEventSerdesProvider<T> {
    @Override
    public Class<?> getJavaType() {
        return XmlEndEvent.class;
    }

    @Override
    public void serialize(XmlEndEvent event, SerializerOutputStream stream) throws IOException {
        super.serialize(event, stream);

        stream.writeLong(event.getTransactionSequence());
    }

    @Override
    public void deserialize(DeserializationContext context, SerializerInputStream stream) throws IOException {
        super.deserialize(context, stream);

        context.addValue(stream.hasNext() ? stream.readLong() : 1L);
    }
}
