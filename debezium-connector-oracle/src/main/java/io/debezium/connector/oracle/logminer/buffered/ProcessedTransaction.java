/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import java.util.Objects;

import io.debezium.connector.oracle.Scn;

/**
 * Represents a transaction that was processed or skipped.
 *
 * @author Chris Cranford
 */
public class ProcessedTransaction {

    public enum ProcessType {
        PROCESSED,
        SKIPPED
    }

    private final String transactionId;
    private final Scn startScn;
    private final ProcessType processType;

    public ProcessedTransaction(String transactionId, Scn startScn, ProcessType processType) {
        this.transactionId = transactionId;
        this.startScn = startScn;
        this.processType = processType;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public Scn getStartScn() {
        return startScn;
    }

    public ProcessType getProcessType() {
        return processType;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ProcessedTransaction that = (ProcessedTransaction) o;
        return Objects.equals(transactionId, that.transactionId);
    }

    @Override
    public int hashCode() {
        return transactionId.hashCode();
    }
}
