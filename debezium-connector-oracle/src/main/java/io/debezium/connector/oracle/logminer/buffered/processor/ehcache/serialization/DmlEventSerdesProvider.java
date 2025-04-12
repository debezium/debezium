/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.processor.ehcache.serialization;

import java.io.IOException;

import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntryImpl;

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

        final LogMinerDmlEntry dmlEntry = event.getDmlEntry();
        stream.writeInt(dmlEntry.getEventType().getValue());
        stream.writeString(dmlEntry.getObjectName());
        stream.writeString(dmlEntry.getObjectOwner());
        stream.writeObjectArray(dmlEntry.getNewValues());
        stream.writeObjectArray(dmlEntry.getOldValues());
    }

    @Override
    public void deserialize(DeserializationContext context, SerializerInputStream stream) throws IOException {
        super.deserialize(context, stream);

        final int entryType = stream.readInt();
        final String objectName = stream.readString();
        final String objectOwner = stream.readString();
        final Object[] newValues = stream.readObjectArray();
        final Object[] oldValues = stream.readObjectArray();

        context.addValue(new LogMinerDmlEntryImpl(entryType, newValues, oldValues, objectOwner, objectName));
    }
}
