/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.infinispan.marshalling;

import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.buffered.ProcessedTransaction;
import io.debezium.connector.oracle.logminer.buffered.ProcessedTransaction.ProcessType;

/**
 * An Infinispan ProtoStream adapter for marshalling a {@link ProcessedTransaction} instance.
 *
 * This class defines a factory for creating {@link ProcessedTransaction} instances when hydrating from the
 * protocol buffer data store as well as field handlers to extract values from a given instance for
 * serialization to a protocol buffer stream.
 *
 * @author Chris Cranford
 */
@ProtoAdapter(ProcessedTransaction.class)
public class ProcessedTransactionAdapter {
    /**
     * A ProtoStream factory that creates a {@link ProcessedTransaction} instance from field values.
     *
     * @param transactionId the transaction identifier
     * @param scn the starting system change number of the transaction
     * @param processType type or how the transaction is "processed"
     * @return the constructed instance
     */
    @ProtoFactory
    public ProcessedTransaction factory(String transactionId, String scn, String processType) {
        return new ProcessedTransaction(transactionId, Scn.valueOf(scn), ProcessType.valueOf(processType));
    }

    /**
     * A ProtoStream handler to extract the {@code transactionId} field from the {@link ProcessedTransaction}.
     *
     * @param processedTransaction the processed transaction object
     * @return the transaction identifier
     */
    @ProtoField(number = 1)
    public String getTransactionId(ProcessedTransaction processedTransaction) {
        return processedTransaction.getTransactionId();
    }

    /**
     * A ProtoStream handler to extract the {@code startScn} field from the {@link ProcessedTransaction}.
     *
     * @param processedTransaction the processed transaction object
     * @return the transaction's starting system change number
     */
    @ProtoField(number = 2)
    public String getScn(ProcessedTransaction processedTransaction) {
        return processedTransaction.getStartScn().toString();
    }

    /**
     * A ProtoStream handler to extract the {@code processType} field from the {@link ProcessedTransaction}.
     *
     * @param processedTransaction the processed transaction object
     * @return the process type
     */
    @ProtoField(number = 3)
    public String getProcessType(ProcessedTransaction processedTransaction) {
        return processedTransaction.getProcessType().toString();
    }
}
