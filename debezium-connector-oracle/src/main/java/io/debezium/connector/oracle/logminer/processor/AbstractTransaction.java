/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor;

import java.time.Instant;
import java.util.Objects;

import io.debezium.connector.oracle.Scn;

/**
 * An abstract implementation of an Oracle {@link Transaction}.
 *
 * @author Chris Cranford
 */
public abstract class AbstractTransaction implements Transaction {

    private static final String UNKNOWN = "UNKNOWN";

    private final String transactionId;
    private final Scn startScn;
    private final Instant changeTime;
    private final String userName;
    private final Integer redoThreadId;
    private boolean hasLobEvent;

    public AbstractTransaction(String transactionId, Scn startScn, Instant changeTime, String userName, Integer redoThreadId) {
        this.transactionId = transactionId;
        this.startScn = startScn;
        this.changeTime = changeTime;
        this.userName = !UNKNOWN.equalsIgnoreCase(userName) ? userName : null;
        this.redoThreadId = redoThreadId;
    }

    @Override
    public String getTransactionId() {
        return transactionId;
    }

    @Override
    public Scn getStartScn() {
        return startScn;
    }

    @Override
    public Instant getChangeTime() {
        return changeTime;
    }

    @Override
    public String getUserName() {
        return userName;
    }

    @Override
    public int getRedoThreadId() {
        return redoThreadId == null ? -1 : redoThreadId;
    }

    @Override
    public boolean hasLobEvent() {
        return hasLobEvent;
    }

    @Override
    public void setHasLobEvent() {
        hasLobEvent = true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractTransaction that = (AbstractTransaction) o;
        return Objects.equals(transactionId, that.transactionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId);
    }

    @Override
    public String toString() {
        return "AbstractTransaction{" +
                "transactionId='" + transactionId + '\'' +
                ", startScn=" + startScn +
                ", changeTime=" + changeTime +
                ", userName='" + userName + '\'' +
                '}';
    }
}
