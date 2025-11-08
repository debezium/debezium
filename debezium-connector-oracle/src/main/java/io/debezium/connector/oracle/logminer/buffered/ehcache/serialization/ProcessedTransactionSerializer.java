/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.ehcache.serialization;

import java.io.IOException;

import org.ehcache.spi.serialization.Serializer;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.buffered.ProcessedTransaction;
import io.debezium.connector.oracle.logminer.buffered.ProcessedTransaction.ProcessType;

/**
 * An Ehcache {@link Serializer} implementation for storing an {@link ProcessedTransaction} in the cache.
 *
 * @author Chris Cranford
 */
public class ProcessedTransactionSerializer extends AbstractEhcacheSerializer<ProcessedTransaction> {

    public ProcessedTransactionSerializer(ClassLoader classLoader) {
    }

    @Override
    protected void serialize(ProcessedTransaction object, SerializerOutputStream stream) throws IOException {
        stream.writeString(object.getTransactionId());
        stream.writeString(object.getStartScn().toString());
        stream.writeString(object.getProcessType().toString());
    }

    @Override
    protected ProcessedTransaction deserialize(SerializerInputStream stream) throws IOException {
        final String transactionId = stream.readString();
        final Scn startScn = readScn(stream.readString());
        final ProcessType processType = readProcessType(stream.readString());
        return new ProcessedTransaction(transactionId, startScn, processType);
    }

    private Scn readScn(String value) {
        return value.equals("null") ? Scn.NULL : Scn.valueOf(value);
    }

    private ProcessType readProcessType(String value) {
        try {
            return ProcessType.valueOf(value);
        }
        catch (IllegalArgumentException e) {
            return null;
        }
    }
}
