/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.events;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.processor.infinispan.marshalling.VisibleForMarshalling;

/**
 * A logical database transaction
 *
 * @author Chris Cranford
 */
public class Transaction {

    private static final Logger LOGGER = LoggerFactory.getLogger(Transaction.class);
    private static final String UNKNOWN = "UNKNOWN";

    private final String transactionId;
    private final Scn startScn;
    private final Instant changeTime;
    private final List<LogMinerEvent> events;
    private final String userName;
    private int numberOfEvents;

    @VisibleForMarshalling
    public Transaction(String transactionId, Scn startScn, Instant changeTime, List<LogMinerEvent> events, String userName, int numberOfEvents) {
        this.transactionId = transactionId;
        this.startScn = startScn;
        this.changeTime = changeTime;
        this.events = events;
        this.userName = !UNKNOWN.equalsIgnoreCase(userName) ? userName : null;
        this.numberOfEvents = numberOfEvents;
    }

    public Transaction(String transactionId, Scn startScn, Instant changeTime, String userName) {
        this(transactionId, startScn, changeTime, new ArrayList<>(), userName, 0);
    }

    public String getTransactionId() {
        return transactionId;
    }

    public Scn getStartScn() {
        return startScn;
    }

    public Instant getChangeTime() {
        return changeTime;
    }

    public List<LogMinerEvent> getEvents() {
        return events;
    }

    public int getNumberOfEvents() {
        return numberOfEvents;
    }

    public int getNextEventId() {
        return numberOfEvents++;
    }

    /**
     * Should be called when a transaction start is detected.
     */
    public void started() {
        numberOfEvents = 0;
    }

    /**
     * Removes any all events within the transaction with the specified {code rowId}.
     *
     * @param rowId the row id for the SQL event that should be removed
     */
    public void removeEventWithRowId(String rowId) {
        events.removeIf(event -> {
            if (event.getRowId().equals(rowId)) {
                LOGGER.trace("Undo applied for event {}.", event);
                return true;
            }
            return false;
        });
    }

    public String getUserName() {
        return userName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Transaction that = (Transaction) o;
        return Objects.equals(transactionId, that.transactionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId);
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "transactionId='" + transactionId + '\'' +
                ", startScn=" + startScn +
                ", userName='" + userName +
                ", numberOfEvents=" + numberOfEvents +
                "'}";
    }
}
