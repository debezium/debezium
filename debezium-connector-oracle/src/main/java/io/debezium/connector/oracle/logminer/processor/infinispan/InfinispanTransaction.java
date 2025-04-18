/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.infinispan;

import java.time.Instant;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.processor.AbstractTransaction;
import io.debezium.connector.oracle.logminer.processor.infinispan.marshalling.VisibleForMarshalling;

/**
 * A concrete implementation of {@link AbstractTransaction} for the Infinispan processor.
 *
 * @author Chris Cranford
 */
public class InfinispanTransaction extends AbstractTransaction {

    private int numberOfEvents;

    public InfinispanTransaction(String transactionId, Scn startScn, Instant changeTime, String userName, Integer redoThreadId, String clientId) {
        super(transactionId, startScn, changeTime, userName, redoThreadId, clientId);
        start();
    }

    @VisibleForMarshalling
    public InfinispanTransaction(String transactionId, Scn startScn, Instant changeTime, String userName, int numberOfEvents, Integer redoThreadId, String clientId) {
        this(transactionId, startScn, changeTime, userName, redoThreadId, clientId);
        this.numberOfEvents = numberOfEvents;
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

    public String getEventId(int index) {
        if (index < 0 || index >= numberOfEvents) {
            throw new IndexOutOfBoundsException("Index " + index + "outside the transaction " + getTransactionId() + " event list bounds");
        }
        return getTransactionId() + "-" + index;
    }

    @Override
    public String toString() {
        return "InfinispanTransaction{" +
                "numberOfEvents=" + numberOfEvents +
                "} " + super.toString();
    }
}
