/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;

/**
 * Replication message instance representing an ORIGIN message from PostgreSQL.
 * The ORIGIN message indicates that the transaction originated from another server
 * in a logical replication setup.
 *
 * @author Shishir Sharma
 */
public class OriginMessage implements ReplicationMessage {

    private final String originName;
    private final Lsn originLsn;
    private final Long transactionId;
    private final Instant commitTime;

    public OriginMessage(String originName, Lsn originLsn, Long transactionId, Instant commitTime) {
        this.originName = originName;
        this.originLsn = originLsn;
        this.transactionId = transactionId;
        this.commitTime = commitTime;
    }

    @Override
    public Operation getOperation() {
        return Operation.ORIGIN;
    }

    @Override
    public boolean isLastEventForLsn() {
        return false;
    }

    @Override
    public OptionalLong getTransactionId() {
        return transactionId == null ? OptionalLong.empty() : OptionalLong.of(transactionId);
    }

    @Override
    public String getTable() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Column> getOldTupleList() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Column> getNewTupleList() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Instant getCommitTime() {
        return commitTime;
    }

    /**
     * @return the name of the origin server from which the transaction originated
     */
    public String getOriginName() {
        return originName;
    }

    /**
     * @return the LSN on the origin server
     */
    public Lsn getOriginLsn() {
        return originLsn;
    }

    @Override
    public String toString() {
        return "OriginMessage [originName=" + originName + ", originLsn=" + (originLsn != null ? originLsn.asString() : null) +
                ", transactionId=" + transactionId + ", commitTime=" + commitTime + "]";
    }
}
