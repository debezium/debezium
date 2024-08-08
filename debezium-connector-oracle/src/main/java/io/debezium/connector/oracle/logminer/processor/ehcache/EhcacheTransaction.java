/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.ehcache;

import java.time.Instant;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.processor.AbstractTransaction;

/**
 * A {@link AbstractTransaction} implementation for Ehcache off-heap caches.
 *
 * @author Chris Cranford
 */
public class EhcacheTransaction extends AbstractTransaction {

    private int numberOfEvents;

    public EhcacheTransaction(String transactionId, Scn startScn, Instant changeTime, String userName, Integer redoThread) {
        super(transactionId, startScn, changeTime, userName, redoThread);
        start();
    }

    public EhcacheTransaction(String transactionId, Scn startScn, Instant changeTime, String userName, Integer redoThread, int numberOfEvents) {
        super(transactionId, startScn, changeTime, userName, redoThread);
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
}
