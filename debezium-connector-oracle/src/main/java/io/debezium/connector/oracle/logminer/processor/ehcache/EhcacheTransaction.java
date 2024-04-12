/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.ehcache;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.processor.AbstractTransaction;

/**
 * A concrete implementation of a {@link AbstractTransaction} for the Ehcache processor.
 */
public class EhcacheTransaction extends AbstractTransaction implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheTransaction.class);

    private int numberOfEvents;
    private List<LogMinerEvent> events;

    public EhcacheTransaction(String transactionId, Scn startScn, Instant changeTime, String userName, Integer redoThreadId) {
        super(transactionId, startScn, changeTime, userName, redoThreadId);
        this.events = new ArrayList<>();
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

    public List<LogMinerEvent> getEvents() {
        return events;
    }

    public boolean removeEventWithRowId(String rowId) {
        // Should always iterate from the back of the event queue and remove the last that matches row-id.
        for (int i = events.size() - 1; i >= 0; i--) {
            final LogMinerEvent event = events.get(i);
            if (event.getRowId().equals(rowId)) {
                events.remove(i);
                LOGGER.trace("Undo applied for event {}.", event);
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "EhcacheTransaction{" +
                "numberOfEvents=" + numberOfEvents +
                "} " + super.toString();
    }
}
