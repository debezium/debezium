/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.memory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.processor.AbstractTransaction;

/**
 * A concrete implementation of a {@link AbstractTransaction} for the JVM heap memory processor.
 *
 * @author Chris Cranford
 */
public class MemoryTransaction extends AbstractTransaction {

    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryTransaction.class);

    private int numberOfEvents;
    private List<LogMinerEvent> events;

    public MemoryTransaction(String transactionId, Scn startScn, Instant changeTime, String userName) {
        super(transactionId, startScn, changeTime, userName);
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
        return "MemoryTransaction{" +
                "numberOfEvents=" + numberOfEvents +
                "} " + super.toString();
    }
}
