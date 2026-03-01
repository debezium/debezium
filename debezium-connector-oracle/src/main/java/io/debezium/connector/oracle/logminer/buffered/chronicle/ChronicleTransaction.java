/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.chronicle;

import java.time.Instant;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.buffered.AbstractTransaction;

/**
 * A {@link AbstractTransaction} implementation for the Chronicle Queue spill-to-disk buffer.
 *
 * @author Debezium Authors
 */
public class ChronicleTransaction extends AbstractTransaction {

    private int numberOfEvents;
    private static final long UNASSIGNED_INDEX = -1L;
    private long startQueueIndex = UNASSIGNED_INDEX;

    public ChronicleTransaction(String transactionId, Scn startScn, Instant changeTime, String userName, Integer redoThread, String clientId) {
        super(transactionId, startScn, changeTime, userName, redoThread, clientId);
        start();
    }

    public ChronicleTransaction(String transactionId, Scn startScn, Instant changeTime, String userName, Integer redoThread, int numberOfEvents, String clientId) {
        super(transactionId, startScn, changeTime, userName, redoThread, clientId);
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

    public void setStartQueueIndex(long index) {
        this.startQueueIndex = index;
    }

    public long getStartQueueIndex() {
        return startQueueIndex;
    }

    @Override
    public void start() {
        numberOfEvents = 0;
    }
}
