/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.ehcache.serialization;

import java.io.IOException;
import java.time.Instant;

import org.ehcache.spi.serialization.Serializer;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.processor.ehcache.EhcacheTransaction;

/**
 * An Ehcache {@link Serializer} implementation for storing an {@link EhcacheTransaction} in the cache.
 *
 * @author Chris Cranford
 */
public class EhcacheTransactionSerializer extends AbstractEhcacheSerializer<EhcacheTransaction> {

    public EhcacheTransactionSerializer(ClassLoader classLoader) {
    }

    @Override
    protected void serialize(EhcacheTransaction object, SerializerOutputStream stream) throws IOException {
        stream.writeString(object.getTransactionId());
        stream.writeScn(object.getStartScn());
        stream.writeInstant(object.getChangeTime());
        stream.writeString(object.getUserName());
        stream.writeInt(object.getRedoThreadId());
        stream.writeInt(object.getNumberOfEvents());
    }

    @Override
    protected EhcacheTransaction deserialize(SerializerInputStream stream) throws IOException {
        final String transactionId = stream.readString();
        final Scn startScn = readScn(stream.readString());
        final Instant changeTime = stream.readInstant();
        final String userName = stream.readString();
        final int redoThread = stream.readInt();
        final int numberOfEvents = stream.readInt();
        return new EhcacheTransaction(transactionId, startScn, changeTime, userName, redoThread, numberOfEvents);
    }

    private Scn readScn(String value) {
        return value.equals("null") ? Scn.NULL : Scn.valueOf(value);
    }
}
