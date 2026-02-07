/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.unbuffered.events;

import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;

/**
 * Custom unbuffered event that represents a data modification (DML).
 *
 * @author Chris Cranford
 */
public class UnbufferedDmlEvent extends DmlEvent implements UnbufferedEvent {

    private final String transactionId;
    private final Long transactionSequence;

    public UnbufferedDmlEvent(LogMinerEventRow row, LogMinerDmlEntry dmlEntry) {
        super(row, dmlEntry);
        this.transactionId = row.getTransactionId();
        this.transactionSequence = row.getTransactionSequence();
    }

    @Override
    public String getTransactionId() {
        return transactionId;
    }

    @Override
    public Long getTransactionSequence() {
        return transactionSequence;
    }

    @Override
    public String toString() {
        return "UnbufferedDmlEvent{" +
                "transactionId='" + transactionId + '\'' +
                ", transactionSequence=" + transactionSequence +
                "} " + super.toString();
    }
}
