/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.ehcache;

import java.time.Instant;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.buffered.AbstractTransaction;

/**
 * A {@link AbstractTransaction} implementation for Ehcache off-heap caches.
 *
 * @author Chris Cranford
 */
public class EhcacheTransaction extends AbstractTransaction {

    private int numberOfEvents;

    public EhcacheTransaction(long xid, Scn startScn, Instant changeTime, String userName, Integer redoThread, String clientId) {
        super(xid, startScn, changeTime, userName, redoThread, clientId);
        start();
    }

    public EhcacheTransaction(long xid, Scn startScn, Instant changeTime, String userName, Integer redoThread, int numberOfEvents, String clientId) {
        super(xid, startScn, changeTime, userName, redoThread, clientId);
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
