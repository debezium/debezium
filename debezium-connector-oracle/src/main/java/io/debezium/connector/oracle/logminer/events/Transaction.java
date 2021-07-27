/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.events;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.processor.infinispan.VisibleForMarshalling;

/**
 * A logical database transaction
 *
 * @author Chris Cranford
 */
public class Transaction {

    private String transactionId;
    private Scn startScn;
    private Instant changeTime;
    private List<LogMinerEvent> events;
    private Set<Long> hashes;

    @VisibleForMarshalling
    public Transaction(String transactionId, Scn startScn, Instant changeTime, List<LogMinerEvent> events, Set<Long> hashes) {
        this.transactionId = transactionId;
        this.startScn = startScn;
        this.changeTime = changeTime;
        this.events = events;
        this.hashes = hashes;
    }

    public Transaction(String transactionId, Scn startScn, Instant changeTime) {
        this(transactionId, startScn, changeTime, new ArrayList<>(), new HashSet<>());
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

    public Set<Long> getHashes() {
        return hashes;
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
                '}';
    }
}
