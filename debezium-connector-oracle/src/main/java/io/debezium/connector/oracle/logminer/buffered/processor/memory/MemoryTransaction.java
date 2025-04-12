/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.processor.memory;

import java.time.Instant;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.buffered.processor.AbstractTransaction;

/**
 * A concrete implementation of a {@link AbstractTransaction} for the JVM heap memory processor.
 *
 * @author Chris Cranford
 */
public class MemoryTransaction extends AbstractTransaction {

    private int numberOfEvents;

    public MemoryTransaction(String transactionId, Scn startScn, Instant changeTime, String userName, Integer redoThreadId, String clientId) {
        super(transactionId, startScn, changeTime, userName, redoThreadId, clientId);
        start();
    }

    @Override
    public int getNumberOfEvents() {
        return numberOfEvents;
    }

    @Override
    public int getNextEventId() {
        return numberOfEvents++;
    }

    @Override
    public void start() {
        numberOfEvents = 0;
    }

    @Override
    public String toString() {
        return "MemoryTransaction{" +
                "numberOfEvents=" + numberOfEvents +
                "} " + super.toString();
    }
}
